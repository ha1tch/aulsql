package main

import (
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
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`

	// Encrypt: "disable", "false", "true", or "strict"
	// - "disable": no encryption at all
	// - "false": encrypt login only
	// - "true": full encryption (requires TLS on server)
	// - "strict": TDS 8.0 strict encryption
	Encrypt         string `json:"encrypt"`
	TrustServerCert *bool  `json:"trust_server_cert"`
	AppName         string `json:"app_name"`
	ConnectionTimeoutS int `json:"connection_timeout_s"`
}

// Environment variable names
const (
	envHost         = "MSSQL_HOST"
	envPort         = "MSSQL_PORT"
	envUser         = "MSSQL_USER"
	envPassword     = "MSSQL_PASSWORD"
	envDatabase     = "MSSQL_DATABASE"
	envEncrypt      = "MSSQL_ENCRYPT"
	envTrustServer  = "MSSQL_TRUST_SERVER_CERT"
	envAppName      = "MSSQL_APP_NAME"
	envConnTimeoutS = "MSSQL_CONNECTION_TIMEOUT_S"

	defaultPort     = 1433
	defaultTimeoutS = 10
	defaultEncrypt  = "disable" // Safe default for dev servers like aul
)

func main() {
	var (
		cfgPath = flag.String("config", "config.json", "Path to JSON config file")

		host     = flag.String("host", "", "SQL Server host")
		port     = flag.Int("port", 0, "SQL Server port")
		user     = flag.String("user", "", "SQL Server user")
		password = flag.String("password", "", "SQL Server password")
		database = flag.String("database", "", "Database name")

		encrypt         = flag.String("encrypt", "", "Encryption: disable, false, true, strict")
		trustServerCert = flag.String("trust-server-cert", "", "Trust server cert: true/false")
		appName         = flag.String("app-name", "", "Application name")
		timeoutS        = flag.Int("timeout", 0, "Connection timeout in seconds")

		verbose = flag.Bool("v", false, "Verbose output (show connection string)")
	)
	flag.Parse()

	// Load config: JSON -> env -> CLI (increasing precedence)
	cfg := loadConfig(*cfgPath)
	applyEnv(&cfg)
	applyCLI(&cfg, *host, *port, *user, *password, *database, *encrypt, *trustServerCert, *appName, *timeoutS)
	applyDefaults(&cfg)

	if err := validate(&cfg); err != nil {
		log.Fatalf("Config error: %v", err)
	}

	connStr := buildConnString(cfg)
	if *verbose {
		// Redact password for display
		redacted := strings.Replace(connStr, url.QueryEscape(cfg.Password), "****", 1)
		fmt.Printf("Connection string: %s\n\n", redacted)
	}

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

	fmt.Printf("Connected to %s:%d/%s\n\n", cfg.Host, cfg.Port, cfg.Database)

	// Show server info
	if err := printServerInfo(ctx, db); err != nil {
		log.Printf("Warning: couldn't get server info: %v", err)
	}

	// List objects
	if err := printTables(ctx, db); err != nil {
		log.Printf("Warning: couldn't list tables: %v", err)
	}
	fmt.Println()

	if err := printStoredProcedures(ctx, db); err != nil {
		log.Printf("Warning: couldn't list procedures: %v", err)
	}
}

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
		// Config file is optional
		return cfg
	}

	if err := json.Unmarshal(b, &cfg); err != nil {
		log.Printf("Warning: invalid config file %s: %v", path, err)
	}
	return cfg
}

func applyEnv(cfg *Config) {
	if v := os.Getenv(envHost); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv(envPort); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.Port = n
		}
	}
	if v := os.Getenv(envUser); v != "" {
		cfg.User = v
	}
	if v := os.Getenv(envPassword); v != "" {
		cfg.Password = v
	}
	if v := os.Getenv(envDatabase); v != "" {
		cfg.Database = v
	}
	if v := os.Getenv(envEncrypt); v != "" {
		cfg.Encrypt = v
	}
	if v := os.Getenv(envTrustServer); v != "" {
		if b, err := parseBool(v); err == nil {
			cfg.TrustServerCert = &b
		}
	}
	if v := os.Getenv(envAppName); v != "" {
		cfg.AppName = v
	}
	if v := os.Getenv(envConnTimeoutS); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ConnectionTimeoutS = n
		}
	}
}

func applyCLI(cfg *Config, host string, port int, user, password, database, encrypt, trustServerCert, appName string, timeoutS int) {
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
	if trustServerCert != "" {
		if b, err := parseBool(trustServerCert); err == nil {
			cfg.TrustServerCert = &b
		}
	}
	if appName != "" {
		cfg.AppName = appName
	}
	if timeoutS != 0 {
		cfg.ConnectionTimeoutS = timeoutS
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
		b := true // Trust by default for dev convenience
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

	// Validate encrypt value
	switch strings.ToLower(cfg.Encrypt) {
	case "disable", "false", "true", "strict":
		// Valid
	default:
		return fmt.Errorf("invalid encrypt value %q (use: disable, false, true, strict)", cfg.Encrypt)
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

func printServerInfo(ctx context.Context, db *sql.DB) error {
	var version string
	err := db.QueryRowContext(ctx, "SELECT @@VERSION").Scan(&version)
	if err != nil {
		return err
	}

	// Truncate to first line
	if idx := strings.Index(version, "\n"); idx > 0 {
		version = version[:idx]
	}
	fmt.Printf("Server: %s\n\n", version)
	return nil
}

func printTables(ctx context.Context, db *sql.DB) error {
	// Query sys.tables - returns all columns, we only need name
	const q = `SELECT * FROM sys.tables WHERE is_ms_shipped = 0 ORDER BY name`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		// Fallback to SQLite-native query
		return printTablesSQLite(ctx, db)
	}
	defer rows.Close()

	// Get column count to handle variable result sets
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Println("Tables:")
	count := 0
	for rows.Next() {
		// Create slice for all columns
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// First column is name
		if name, ok := values[0].(string); ok {
			fmt.Printf("  %s\n", name)
		}
		count++
	}
	if count == 0 {
		fmt.Println("  (none)")
	}
	return rows.Err()
}

func printTablesSQLite(ctx context.Context, db *sql.DB) error {
	const q = `SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	fmt.Println("Tables:")
	count := 0
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return err
		}
		fmt.Printf("  %s\n", table)
		count++
	}
	if count == 0 {
		fmt.Println("  (none)")
	}
	return rows.Err()
}

func printStoredProcedures(ctx context.Context, db *sql.DB) error {
	// Query sys.procedures - returns all columns, we only need name
	const q = `SELECT * FROM sys.procedures WHERE is_ms_shipped = 0 ORDER BY name`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get column count to handle variable result sets
	cols, err := rows.Columns()
	if err != nil {
		return err
	}

	fmt.Println("Stored Procedures:")
	count := 0
	for rows.Next() {
		// Create slice for all columns
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return err
		}

		// First column is name
		if name, ok := values[0].(string); ok {
			fmt.Printf("  %s\n", name)
		}
		count++
	}
	if count == 0 {
		fmt.Println("  (none)")
	}
	return rows.Err()
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "1", "t", "true", "y", "yes", "on":
		return true, nil
	case "0", "f", "false", "n", "no", "off":
		return false, nil
	default:
		return false, fmt.Errorf("not a boolean: %q", s)
	}
}