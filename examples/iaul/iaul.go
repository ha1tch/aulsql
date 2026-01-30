package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	_ "github.com/microsoft/go-mssqldb"
)

type Config struct {
	Host               string `json:"host"`
	Port               int    `json:"port"`
	User               string `json:"user"`
	Password           string `json:"password"`
	Database           string `json:"database"`
	Encrypt            string `json:"encrypt"`
	TrustServerCert    *bool  `json:"trust_server_cert"`
	AppName            string `json:"app_name"`
	ConnectionTimeoutS int    `json:"connection_timeout_s"`
}

// Pre-configured SQL statements
var presetQueries = []struct {
	Name string
	SQL  string
}{
	{"List tables", "SELECT * FROM sys.tables WHERE is_ms_shipped = 0"},
	{"List procedures", "SELECT * FROM sys.procedures WHERE is_ms_shipped = 0"},
	{"List schemas", "SELECT * FROM sys.schemas"},
	{"List types", "SELECT * FROM sys.types"},
	{"List databases", "SELECT * FROM sys.databases"},
	{"Server version", "SELECT @@VERSION"},
	{"Current database", "SELECT DB_NAME()"},
	{"Row count", "SELECT @@ROWCOUNT"},
}

const (
	defaultPort     = 1433
	defaultTimeoutS = 10
	defaultEncrypt  = "disable"
	maxHistory      = 100
)

func main() {
	var (
		cfgPath  = flag.String("config", "config.json", "Path to JSON config file")
		host     = flag.String("host", "", "SQL Server host")
		port     = flag.Int("port", 0, "SQL Server port")
		user     = flag.String("user", "", "SQL Server user")
		password = flag.String("password", "", "SQL Server password")
		database = flag.String("database", "", "Database name")
		encrypt  = flag.String("encrypt", "", "Encryption: disable, false, true, strict")
	)
	flag.Parse()

	// Load config
	cfg := loadConfig(*cfgPath)
	applyEnv(&cfg)
	applyCLI(&cfg, *host, *port, *user, *password, *database, *encrypt)
	applyDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		log.Fatalf("Config error: %v", err)
	}

	connStr := buildConnString(cfg)

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectionTimeoutS)*time.Second)
	defer cancel()

	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Connection failed: %v", err)
	}

	fmt.Printf("Connected to %s:%d/%s\n", cfg.Host, cfg.Port, cfg.Database)
	printServerInfo(context.Background(), db)
	fmt.Println()

	// Start interactive CLI
	runCLI(db)
}

func runCLI(db *sql.DB) {
	reader := bufio.NewReader(os.Stdin)
	history := make([]string, 0, maxHistory)

	printHelp()

	for {
		fmt.Print("\nsql> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("\nGoodbye!")
			return
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		switch strings.ToLower(input) {
		case "exit", "quit", "q":
			fmt.Println("Goodbye!")
			return

		case "help", "h", "?":
			printHelp()

		case "presets", "p":
			showPresets()

		case "history", "hist":
			showHistory(history)

		case "clear":
			history = history[:0]
			fmt.Println("History cleared.")

		default:
			// Check for preset selection: p1, p2, etc.
			if strings.HasPrefix(strings.ToLower(input), "p") && len(input) > 1 {
				if idx, err := strconv.Atoi(input[1:]); err == nil && idx >= 1 && idx <= len(presetQueries) {
					sql := presetQueries[idx-1].SQL
					fmt.Printf(">> %s\n", sql)
					executeAndPrint(db, sql)
					history = addToHistory(history, sql)
					continue
				}
			}

			// Check for history selection: !1, !2, etc.
			if strings.HasPrefix(input, "!") && len(input) > 1 {
				if idx, err := strconv.Atoi(input[1:]); err == nil && idx >= 1 && idx <= len(history) {
					sql := history[idx-1]
					fmt.Printf(">> %s\n", sql)
					executeAndPrint(db, sql)
					continue
				}
				fmt.Println("Invalid history index.")
				continue
			}

			// Check for multi-line input (ends with GO or ;)
			sql := input
			if !strings.HasSuffix(strings.ToUpper(strings.TrimSpace(input)), "GO") &&
				!strings.HasSuffix(input, ";") {
				// Could be multi-line, but for simplicity treat single line as complete
			}

			// Remove trailing GO or ; for execution
			sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
			if strings.HasSuffix(strings.ToUpper(sql), " GO") {
				sql = strings.TrimSuffix(sql, " GO")
				sql = strings.TrimSuffix(sql, " go")
			}
			if strings.ToUpper(sql) == "GO" {
				continue
			}

			executeAndPrint(db, sql)
			history = addToHistory(history, sql)
		}
	}
}

func printHelp() {
	fmt.Println(`
Commands:
  <SQL>         Execute SQL statement
  p, presets    Show preset queries
  p<N>          Execute preset N (e.g., p1, p2)
  history       Show command history
  !<N>          Execute history item N (e.g., !1, !2)
  clear         Clear history
  help, h, ?    Show this help
  exit, quit, q Exit`)
}

func showPresets() {
	fmt.Println("\nPreset Queries:")
	for i, p := range presetQueries {
		fmt.Printf("  p%-2d  %s\n", i+1, p.Name)
	}
}

func showHistory(history []string) {
	if len(history) == 0 {
		fmt.Println("No history.")
		return
	}
	fmt.Println("\nHistory:")
	for i, sql := range history {
		// Truncate long queries for display
		display := sql
		if len(display) > 60 {
			display = display[:57] + "..."
		}
		fmt.Printf("  !%-2d  %s\n", i+1, display)
	}
}

func addToHistory(history []string, sql string) []string {
	// Don't add duplicates of the last entry
	if len(history) > 0 && history[len(history)-1] == sql {
		return history
	}
	history = append(history, sql)
	if len(history) > maxHistory {
		history = history[1:]
	}
	return history
}

func executeAndPrint(db *sql.DB, sqlStr string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := db.QueryContext(ctx, sqlStr)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer rows.Close()

	// Get columns
	cols, err := rows.Columns()
	if err != nil {
		fmt.Printf("Error getting columns: %v\n", err)
		return
	}

	if len(cols) == 0 {
		fmt.Printf("OK (%.2fms)\n", float64(elapsed.Microseconds())/1000)
		return
	}

	// Calculate column widths (start with header lengths)
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
		if widths[i] < 4 {
			widths[i] = 4
		}
	}

	// Collect all rows first to calculate widths
	var allRows [][]string
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Printf("Error scanning row: %v\n", err)
			return
		}

		row := make([]string, len(cols))
		for i, v := range values {
			row[i] = formatValue(v)
			if len(row[i]) > widths[i] {
				widths[i] = len(row[i])
			}
			// Cap column width
			if widths[i] > 50 {
				widths[i] = 50
			}
		}
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Print header
	fmt.Println()
	printRow(cols, widths)
	printSeparator(widths)

	// Print rows
	for _, row := range allRows {
		printRow(row, widths)
	}

	fmt.Printf("\n(%d rows, %.2fms)\n", len(allRows), float64(elapsed.Microseconds())/1000)
}

func formatValue(v interface{}) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		return string(val)
	case time.Time:
		return val.Format("2006-01-02 15:04:05")
	default:
		return fmt.Sprintf("%v", val)
	}
}

func printRow(values []string, widths []int) {
	for i, v := range values {
		if len(v) > widths[i] {
			v = v[:widths[i]-3] + "..."
		}
		fmt.Printf("%-*s  ", widths[i], v)
	}
	fmt.Println()
}

func printSeparator(widths []int) {
	for _, w := range widths {
		fmt.Print(strings.Repeat("-", w) + "  ")
	}
	fmt.Println()
}

func printServerInfo(ctx context.Context, db *sql.DB) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	if err != nil {
		return
	}
	if idx := strings.Index(version, "\n"); idx > 0 {
		version = version[:idx]
	}
	fmt.Printf("Server: %s\n", version)
}

// --- Config loading ---

func loadConfig(path string) Config {
	var cfg Config
	p := path
	if !filepath.IsAbs(p) {
		if wd, err := os.Getwd(); err == nil {
			p = filepath.Join(wd, p)
		}
	}
	b, err := os.ReadFile(p)
	if err != nil {
		return cfg
	}
	json.Unmarshal(b, &cfg)
	return cfg
}

func applyEnv(cfg *Config) {
	if v := os.Getenv("MSSQL_HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("MSSQL_PORT"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Port = n
		}
	}
	if v := os.Getenv("MSSQL_USER"); v != "" {
		cfg.User = v
	}
	if v := os.Getenv("MSSQL_PASSWORD"); v != "" {
		cfg.Password = v
	}
	if v := os.Getenv("MSSQL_DATABASE"); v != "" {
		cfg.Database = v
	}
	if v := os.Getenv("MSSQL_ENCRYPT"); v != "" {
		cfg.Encrypt = v
	}
}

func applyCLI(cfg *Config, host string, port int, user, password, database, encrypt string) {
	if host != "" {
		cfg.Host = host
	}
	if port != 0 {
		cfg.Port = port
	}
	if user != "" {
		cfg.User = user
	}
	if password != "" {
		cfg.Password = password
	}
	if database != "" {
		cfg.Database = database
	}
	if encrypt != "" {
		cfg.Encrypt = encrypt
	}
}

func applyDefaults(cfg *Config) {
	if cfg.Port == 0 {
		cfg.Port = defaultPort
	}
	if cfg.ConnectionTimeoutS <= 0 {
		cfg.ConnectionTimeoutS = defaultTimeoutS
	}
	if cfg.Encrypt == "" {
		cfg.Encrypt = defaultEncrypt
	}
	if cfg.TrustServerCert == nil {
		b := true
		cfg.TrustServerCert = &b
	}
}

func validate(cfg *Config) error {
	var missing []string
	if strings.TrimSpace(cfg.Host) == "" {
		missing = append(missing, "host")
	}
	if strings.TrimSpace(cfg.User) == "" {
		missing = append(missing, "user")
	}
	if strings.TrimSpace(cfg.Password) == "" {
		missing = append(missing, "password")
	}
	if strings.TrimSpace(cfg.Database) == "" {
		missing = append(missing, "database")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing: %s", strings.Join(missing, ", "))
	}
	return nil
}

func buildConnString(cfg Config) string {
	u := &url.URL{
		Scheme: "sqlserver",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
	}
	q := url.Values{}
	q.Set("database", cfg.Database)
	q.Set("encrypt", strings.ToLower(cfg.Encrypt))
	if cfg.TrustServerCert != nil {
		q.Set("trustservercertificate", strconv.FormatBool(*cfg.TrustServerCert))
	}
	if cfg.AppName != "" {
		q.Set("app name", cfg.AppName)
	}
	u.RawQuery = q.Encode()
	return u.String()
}