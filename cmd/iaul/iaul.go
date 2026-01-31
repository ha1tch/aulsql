package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/chzyer/readline"
	_ "github.com/microsoft/go-mssqldb"
	"golang.org/x/term"
)

// ANSI colour codes
var (
	colReset  string
	colBold   string
	colDim    string
	colRed    string
	colGreen  string
	colYellow string
	colCyan   string
	colWhite  string
	useColour bool
)

// initColour detects terminal colour support or applies forced setting
func initColour(forceColour, noColour bool) {
	if noColour {
		useColour = false
	} else if forceColour {
		useColour = true
	} else {
		// Auto-detect: check if stdout is a terminal
		useColour = term.IsTerminal(int(os.Stdout.Fd()))

		// Check common environment variables
		if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
			useColour = false
		}
		if os.Getenv("FORCE_COLOR") != "" || os.Getenv("CLICOLOR_FORCE") != "" {
			useColour = true
		}
	}

	if useColour {
		colReset = "\033[0m"
		colBold = "\033[1m"    // Just bold, use terminal's default foreground
		colDim = "\033[2m"     // Dim (faint) - more compatible than \033[90m
		colRed = "\033[31m"    // Red (not bright, for better compatibility)
		colGreen = "\033[32m"
		colYellow = "\033[33m"
		colCyan = "\033[36m"
		colWhite = "\033[37m"
	} else {
		colReset = ""
		colBold = ""
		colDim = ""
		colRed = ""
		colGreen = ""
		colYellow = ""
		colCyan = ""
		colWhite = ""
	}
}

// Config holds connection and application settings
type Config struct {
	// Connection settings
	Host               string `json:"host"`
	Port               int    `json:"port"`
	User               string `json:"user"`
	Password           string `json:"password"`
	Database           string `json:"database"`
	Encrypt            string `json:"encrypt"`
	TrustServerCert    *bool  `json:"trust_server_cert"`
	AppName            string `json:"app_name"`
	ConnectionTimeoutS int    `json:"connection_timeout_s"`

	// Application settings
	HistoryFile   string  `json:"history_file"`
	MaxHistory    int     `json:"max_history"`
	QueryTimeoutS int     `json:"query_timeout_s"`
	Macros        []Macro `json:"macros"`
}

// Macro represents a named SQL query shortcut
type Macro struct {
	Name string `json:"name"`
	SQL  string `json:"sql"`
}

// Default macros when none configured
var defaultMacros = []Macro{
	{Name: "List tables", SQL: "SELECT * FROM sys.tables WHERE is_ms_shipped = 0"},
	{Name: "List procedures", SQL: "SELECT * FROM sys.procedures WHERE is_ms_shipped = 0"},
	{Name: "List schemas", SQL: "SELECT * FROM sys.schemas"},
	{Name: "List types", SQL: "SELECT * FROM sys.types"},
	{Name: "List databases", SQL: "SELECT * FROM sys.databases"},
	{Name: "Server version", SQL: "SELECT @@VERSION"},
	{Name: "Current database", SQL: "SELECT DB_NAME()"},
	{Name: "Row count", SQL: "SELECT @@ROWCOUNT"},
}

// SQL keywords for tab completion
var sqlKeywords = []string{
	"SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "LIKE", "BETWEEN",
	"ORDER", "BY", "ASC", "DESC", "GROUP", "HAVING", "LIMIT", "OFFSET",
	"INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
	"CREATE", "TABLE", "INDEX", "VIEW", "DROP", "ALTER", "ADD", "COLUMN",
	"PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK", "DEFAULT",
	"NULL", "NOT", "EXISTS", "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER", "FULL", "CROSS",
	"UNION", "ALL", "DISTINCT", "TOP", "WITH", "CASE", "WHEN", "THEN", "ELSE", "END",
	"BEGIN", "COMMIT", "ROLLBACK", "TRANSACTION", "EXEC", "EXECUTE", "DECLARE", "SET",
	"IF", "WHILE", "RETURN", "PRINT", "RAISERROR", "TRY", "CATCH", "THROW",
	"COUNT", "SUM", "AVG", "MIN", "MAX", "CAST", "CONVERT", "COALESCE", "ISNULL",
	"GETDATE", "DATEADD", "DATEDIFF", "DATEPART", "YEAR", "MONTH", "DAY",
	"LEN", "SUBSTRING", "CHARINDEX", "REPLACE", "UPPER", "LOWER", "TRIM", "LTRIM", "RTRIM",
	"sys.tables", "sys.columns", "sys.procedures", "sys.schemas", "sys.types", "sys.databases",
	"INFORMATION_SCHEMA.TABLES", "INFORMATION_SCHEMA.COLUMNS",
}

const (
	defaultPort         = 1433
	defaultTimeoutS     = 10
	defaultEncrypt      = "disable"
	defaultMaxHistory   = 500
	defaultQueryTimeout = 30
)

// Display format types
type DisplayFormat int

const (
	FormatDefault DisplayFormat = iota // Simple tabular (current default)
	FormatASCII                        // ASCII art table with borders
	FormatUnicode                      // Unicode box-drawing table
	FormatCSV                          // CSV output
	FormatJSON                         // JSON output
)

var formatNames = map[string]DisplayFormat{
	"default": FormatDefault,
	"ascii":   FormatASCII,
	"unicode": FormatUnicode,
	"csv":     FormatCSV,
	"json":    FormatJSON,
}

var (
	historyPath   string
	macros        []Macro
	maxHistory    int
	tableNames    []string             // Populated after connection for completion
	columnNames   map[string][]string  // Table -> columns mapping for completion
	displayFormat DisplayFormat        // Current display format
	verbosity     int                  // Output verbosity level
	showTiming    bool                 // Whether to show query timing
	outputFile    *os.File             // Output redirection file (nil = stdout)
	outputPath    string               // Path of output file for display
	usePager      bool                 // Whether to use pager for long output
	pagerCmd      string               // Pager command (default: less -R)
	variables     map[string]string    // User-defined variables
	currentDB     *sql.DB              // Current database connection (for completions)
)

// Verbosity levels
const (
	VerbositySilent  = 0 // No output except errors
	VerbosityQuiet   = 1 // Results only, no banner/stats
	VerbosityNormal  = 2 // Default: banner, results, stats
	VerbosityVerbose = 3 // Extra info: timing, connection details
)

func main() {
	var (
		cfgPath     = flag.String("config", "config.json", "Path to JSON config file")
		host        = flag.String("host", "", "SQL Server host")
		port        = flag.Int("port", 0, "SQL Server port")
		user        = flag.String("user", "", "SQL Server user")
		password    = flag.String("password", "", "SQL Server password")
		database    = flag.String("database", "", "Database name")
		encrypt     = flag.String("encrypt", "", "Encryption: disable, false, true, strict")
		histFile    = flag.String("history", "", "History file path (default: ~/.iaul_history)")
		forceColour = flag.Bool("color", false, "Force colour output")
		noColour    = flag.Bool("no-color", false, "Disable colour output")
		execSQL     = flag.String("e", "", "Execute SQL statement(s) and exit")
		execFile    = flag.String("f", "", "Execute SQL file and exit")
		formatFlag  = flag.String("format", "default", "Output format: default, ascii, unicode, csv, json")
		silent      = flag.Bool("silent", false, "Silent mode (errors only)")
		quiet       = flag.Bool("quiet", false, "Quiet mode (results only, no banner/stats)")
		verbose     = flag.Bool("verbose", false, "Verbose mode (extra timing info)")
	)
	flag.Parse()

	// Set verbosity level
	verbosity = VerbosityNormal
	if *silent {
		verbosity = VerbositySilent
	} else if *quiet {
		verbosity = VerbosityQuiet
	} else if *verbose {
		verbosity = VerbosityVerbose
	}

	// Initialise colour support
	initColour(*forceColour, *noColour)

	// Set display format from flag
	if f, ok := formatNames[strings.ToLower(*formatFlag)]; ok {
		displayFormat = f
	}

	// Load config
	cfg := loadConfig(*cfgPath)
	applyEnv(&cfg)
	applyCLI(&cfg, *host, *port, *user, *password, *database, *encrypt)
	applyDefaults(&cfg)

	// Override history file from CLI if provided
	if *histFile != "" {
		cfg.HistoryFile = *histFile
	}

	// Set globals from config
	historyPath = cfg.HistoryFile
	maxHistory = cfg.MaxHistory
	if len(cfg.Macros) > 0 {
		macros = cfg.Macros
	} else {
		macros = defaultMacros
	}

	if err := validate(&cfg); err != nil {
		log.Fatalf("Config error: %v", err)
	}

	connStr := buildConnString(cfg)

	// Connect
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ConnectionTimeoutS)*time.Second)
	defer cancel()

	connectStart := time.Now()
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		log.Fatalf("sql.Open failed: %v", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	connectElapsed := time.Since(connectStart)

	// Print connection info based on verbosity
	if verbosity >= VerbosityNormal {
		fmt.Printf("Connected to %s:%d/%s\n", cfg.Host, cfg.Port, cfg.Database)
		printServerInfo(context.Background(), db)
	}
	if verbosity >= VerbosityVerbose {
		fmt.Printf("%sConnection time: %.2fms%s\n", colDim, float64(connectElapsed.Microseconds())/1000, colReset)
	}

	// Initialize globals
	currentDB = db
	columnNames = make(map[string][]string)
	variables = make(map[string]string)
	showTiming = true // Default: show timing
	pagerCmd = "less -R"

	// Handle non-interactive modes
	if *execSQL != "" {
		// Execute SQL from command line
		exitCode := executeScript(db, *execSQL, cfg.QueryTimeoutS)
		os.Exit(exitCode)
	}

	if *execFile != "" {
		// Execute SQL file
		content, err := os.ReadFile(*execFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%sError reading file: %v%s\n", colRed, err, colReset)
			os.Exit(1)
		}
		exitCode := executeScript(db, string(content), cfg.QueryTimeoutS)
		os.Exit(exitCode)
	}

	// Load table names for completion (interactive mode only)
	loadTableNames(db)

	if verbosity >= VerbosityNormal {
		fmt.Println()
	}

	// Start interactive CLI with readline
	runCLI(db, cfg.QueryTimeoutS)
}

// executeScript executes one or more SQL statements from a string
// Returns exit code: 0 for success, 1 for any errors
func executeScript(db *sql.DB, script string, timeoutSec int) int {
	// Split on GO statements (batch separator)
	batches := splitBatches(script)
	
	exitCode := 0
	for i, batch := range batches {
		batch = strings.TrimSpace(batch)
		if batch == "" {
			continue
		}

		if verbosity >= VerbosityVerbose {
			fmt.Printf("%s-- Executing batch %d --%s\n", colDim, i+1, colReset)
		}

		if !executeAndPrintBatch(db, batch, timeoutSec) {
			exitCode = 1
			// Continue executing remaining batches unless silent
			if verbosity == VerbositySilent {
				return exitCode
			}
		}
	}
	return exitCode
}

// splitBatches splits a SQL script on GO statements
func splitBatches(script string) []string {
	var batches []string
	var current strings.Builder
	
	lines := strings.Split(script, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// GO on its own line (case-insensitive) is a batch separator
		if strings.EqualFold(trimmed, "GO") {
			if current.Len() > 0 {
				batches = append(batches, current.String())
				current.Reset()
			}
		} else {
			current.WriteString(line)
			current.WriteString("\n")
		}
	}
	// Don't forget the last batch
	if current.Len() > 0 {
		batches = append(batches, current.String())
	}
	return batches
}

// executeAndPrintBatch executes a single SQL batch and prints results
// Returns true on success, false on error
func executeAndPrintBatch(db *sql.DB, sqlStr string, timeoutSec int) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := db.QueryContext(ctx, sqlStr)
	elapsed := time.Since(start)

	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError: %v%s\n", colRed, err, colReset)
		return false
	}
	defer rows.Close()

	// Get columns
	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError getting columns: %v%s\n", colRed, err, colReset)
		return false
	}

	if len(cols) == 0 {
		if verbosity >= VerbosityNormal {
			fmt.Printf("%sOK (%.2fms)%s\n", colDim, float64(elapsed.Microseconds())/1000, colReset)
		}
		return true
	}

	// Collect all rows
	var allRows [][]string
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Fprintf(os.Stderr, "%sError scanning row: %v%s\n", colRed, err, colReset)
			return false
		}

		row := make([]string, len(cols))
		for i, v := range values {
			row[i] = formatValue(v)
		}
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%sError: %v%s\n", colRed, err, colReset)
		return false
	}

	// Output based on format (skip if silent with no results)
	if verbosity > VerbositySilent || len(allRows) > 0 {
		switch displayFormat {
		case FormatASCII:
			printASCIITable(cols, allRows)
		case FormatUnicode:
			printUnicodeTable(cols, allRows)
		case FormatCSV:
			printCSV(cols, allRows)
		case FormatJSON:
			printJSON(cols, allRows)
		default:
			printDefaultTable(cols, allRows)
		}
	}

	// Print stats based on verbosity
	if verbosity >= VerbosityNormal && displayFormat != FormatCSV && displayFormat != FormatJSON {
		fmt.Printf("\n%s(%d rows, %.2fms)%s\n", colDim, len(allRows), float64(elapsed.Microseconds())/1000, colReset)
	} else if verbosity >= VerbosityVerbose {
		fmt.Fprintf(os.Stderr, "%s(%d rows, %.2fms)%s\n", colDim, len(allRows), float64(elapsed.Microseconds())/1000, colReset)
	}

	return true
}

// loadTableNames fetches table names and column names for tab completion
func loadTableNames(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Load table names
	rows, err := db.QueryContext(ctx, "SELECT name FROM sys.tables WHERE is_ms_shipped = 0")
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tableNames = append(tableNames, name)
		}
	}

	// Load column names for each table
	loadColumnNames(db)
}

// loadColumnNames fetches column names for all user tables
func loadColumnNames(db *sql.DB) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	query := `
		SELECT t.name AS table_name, c.name AS column_name
		FROM sys.columns c
		JOIN sys.tables t ON c.object_id = t.object_id
		WHERE t.is_ms_shipped = 0
		ORDER BY t.name, c.column_id
	`
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return
	}
	defer rows.Close()

	for rows.Next() {
		var tableName, colName string
		if err := rows.Scan(&tableName, &colName); err == nil {
			if columnNames[tableName] == nil {
				columnNames[tableName] = make([]string, 0)
			}
			columnNames[tableName] = append(columnNames[tableName], colName)
		}
	}
}

// completer provides tab completion for SQL
func completer(line string) []string {
	var candidates []string

	// Get the word being typed
	line = strings.TrimLeft(line, " \t")
	words := strings.Fields(line)
	var prefix string
	if len(words) > 0 {
		prefix = strings.ToUpper(words[len(words)-1])
		// If line ends with space, we're starting a new word
		if strings.HasSuffix(line, " ") {
			prefix = ""
		}
	}

	// Match SQL keywords
	for _, kw := range sqlKeywords {
		if prefix == "" || strings.HasPrefix(strings.ToUpper(kw), prefix) {
			candidates = append(candidates, kw)
		}
	}

	// Match table names
	for _, tbl := range tableNames {
		if prefix == "" || strings.HasPrefix(strings.ToUpper(tbl), prefix) {
			candidates = append(candidates, tbl)
		}
	}

	// Match macro shortcuts
	if strings.HasPrefix("m", strings.ToLower(prefix)) || strings.HasPrefix("p", strings.ToLower(prefix)) {
		for i := range macros {
			candidates = append(candidates, fmt.Sprintf("m%d", i+1))
			candidates = append(candidates, fmt.Sprintf("p%d", i+1))
		}
	}

	// Match commands
	commands := []string{"help", "macros", "history", "clear", "quit", "exit"}
	for _, cmd := range commands {
		if prefix == "" || strings.HasPrefix(cmd, strings.ToLower(prefix)) {
			candidates = append(candidates, cmd)
		}
	}

	sort.Strings(candidates)
	return candidates
}

func runCLI(db *sql.DB, queryTimeout int) {
	// Configure readline
	config := &readline.Config{
		Prompt:          colGreen + "sql>" + colReset + " ",
		HistoryFile:     historyPath,
		HistoryLimit:    maxHistory,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",

		AutoComplete: readline.NewPrefixCompleter(
			readline.PcItemDynamic(completer),
		),

		HistorySearchFold: true, // Case-insensitive history search
	}

	rl, err := readline.NewEx(config)
	if err != nil {
		log.Fatalf("Failed to initialize readline: %v", err)
	}
	defer rl.Close()

	// Custom completer that works better with SQL
	rl.Config.AutoComplete = &sqlCompleter{}

	printHelp()

	var multiLine strings.Builder
	inMultiLine := false

	for {
		var prompt string
		if inMultiLine {
			prompt = colGreen + "  ->" + colReset + " "
		} else {
			prompt = colGreen + "sql>" + colReset + " "
		}
		rl.SetPrompt(prompt)

		line, err := rl.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				if inMultiLine {
					// Cancel multi-line input
					multiLine.Reset()
					inMultiLine = false
					fmt.Println("^C")
					continue
				}
				continue
			}
			if err == io.EOF {
				fmt.Println("\nGoodbye!")
				return
			}
			break
		}

		input := strings.TrimSpace(line)
		
		// Handle multi-line input
		if inMultiLine {
			if strings.ToUpper(input) == "GO" || input == ";" {
				// Execute the accumulated SQL
				sql := strings.TrimSpace(multiLine.String())
				multiLine.Reset()
				inMultiLine = false
				if sql != "" {
					executeAndPrint(db, sql, queryTimeout)
				}
				continue
			}
			multiLine.WriteString(" ")
			multiLine.WriteString(line)
			continue
		}

		if input == "" {
			continue
		}

		// Check for multi-line start (doesn't end with GO or ;)
		if !strings.HasSuffix(strings.ToUpper(input), "GO") &&
			!strings.HasSuffix(input, ";") &&
			!isCommand(input) {
			// Could be start of multi-line, but check if it looks complete
			upperInput := strings.ToUpper(input)
			if strings.HasPrefix(upperInput, "SELECT") ||
				strings.HasPrefix(upperInput, "INSERT") ||
				strings.HasPrefix(upperInput, "UPDATE") ||
				strings.HasPrefix(upperInput, "DELETE") ||
				strings.HasPrefix(upperInput, "CREATE") ||
				strings.HasPrefix(upperInput, "ALTER") ||
				strings.HasPrefix(upperInput, "DROP") ||
				strings.HasPrefix(upperInput, "EXEC") ||
				strings.HasPrefix(upperInput, "WITH") ||
				strings.HasPrefix(upperInput, "DECLARE") ||
				strings.HasPrefix(upperInput, "BEGIN") {
				// Looks like SQL, check if it seems incomplete
				// For now, just execute single-line statements
				// Multi-line requires explicit continuation
			}
		}

		// Process commands
		switch strings.ToLower(input) {
		case "exit", "quit", "q", "\\q":
			if outputFile != nil {
				outputFile.Close()
			}
			fmt.Println("Goodbye!")
			return

		case "help", "h", "?", "\\?":
			printHelp()
			continue

		case "macros", "m":
			showMacros()
			continue

		case "history", "hist":
			showHistory(rl)
			continue

		case "clear":
			if useColour {
				fmt.Print("\033[H\033[2J") // ANSI clear screen
			}
			continue

		case "clearhistory":
			rl.SaveHistory("") // Clear by saving empty
			fmt.Println("History cleared.")
			continue

		case "format":
			showCurrentFormat()
			continue

		case "format default", "\\g":
			setFormat(FormatDefault)
			continue

		case "format ascii":
			setFormat(FormatASCII)
			continue

		case "format unicode":
			setFormat(FormatUnicode)
			continue

		case "format csv":
			setFormat(FormatCSV)
			continue

		case "format json":
			setFormat(FormatJSON)
			continue

		// Backslash commands (psql-style)
		case "\\dt", "\\d":
			// List tables
			executeAndPrint(db, "SELECT name, type_desc FROM sys.tables WHERE is_ms_shipped = 0 ORDER BY name", queryTimeout)
			continue

		case "\\di":
			// List indexes
			executeAndPrint(db, "SELECT i.name AS index_name, t.name AS table_name, i.type_desc FROM sys.indexes i JOIN sys.tables t ON i.object_id = t.object_id WHERE t.is_ms_shipped = 0 AND i.name IS NOT NULL ORDER BY t.name, i.name", queryTimeout)
			continue

		case "\\dv":
			// List views
			executeAndPrint(db, "SELECT name, type_desc FROM sys.views WHERE is_ms_shipped = 0 ORDER BY name", queryTimeout)
			continue

		case "\\dp", "\\df":
			// List procedures/functions
			executeAndPrint(db, "SELECT name, type_desc FROM sys.procedures WHERE is_ms_shipped = 0 ORDER BY name", queryTimeout)
			continue

		case "\\ds":
			// List schemas
			executeAndPrint(db, "SELECT name FROM sys.schemas ORDER BY name", queryTimeout)
			continue

		case "\\dn":
			// List databases
			executeAndPrint(db, "SELECT name FROM sys.databases ORDER BY name", queryTimeout)
			continue

		case "\\timing", "\\t":
			// Toggle timing display
			showTiming = !showTiming
			if showTiming {
				fmt.Println("Timing is on.")
			} else {
				fmt.Println("Timing is off.")
			}
			continue

		case "\\o":
			// Close output file, return to stdout
			if outputFile != nil {
				outputFile.Close()
				outputFile = nil
				fmt.Printf("Output redirected to stdout.\n")
			} else {
				fmt.Println("Output is already going to stdout.")
			}
			outputPath = ""
			continue

		case "\\pager", "\\p":
			// Toggle pager
			usePager = !usePager
			if usePager {
				fmt.Printf("Pager is on (using: %s).\n", pagerCmd)
			} else {
				fmt.Println("Pager is off.")
			}
			continue

		case "\\set":
			// List all variables
			if len(variables) == 0 {
				fmt.Println("No variables defined.")
			} else {
				fmt.Println("Variables:")
				for k, v := range variables {
					fmt.Printf("  %s = %s\n", k, v)
				}
			}
			continue

		case "\\unset":
			fmt.Println("Usage: \\unset <varname>")
			continue

		case "\\r", "\\reset":
			// Reload schema (table/column names)
			tableNames = nil
			columnNames = make(map[string][]string)
			loadTableNames(db)
			fmt.Printf("Schema reloaded. %d tables, %d with columns.\n", len(tableNames), len(columnNames))
			continue
		}

		// Handle commands with arguments
		lowerInput := strings.ToLower(input)

		// \d tablename - describe table
		if strings.HasPrefix(lowerInput, "\\d ") {
			tableName := strings.TrimSpace(input[3:])
			describeTable(db, tableName, queryTimeout)
			continue
		}

		// \o filename - redirect output to file
		if strings.HasPrefix(lowerInput, "\\o ") {
			filename := strings.TrimSpace(input[3:])
			setOutputFile(filename)
			continue
		}

		// \i filename - include/execute file
		if strings.HasPrefix(lowerInput, "\\i ") {
			filename := strings.TrimSpace(input[3:])
			executeFile(db, filename, queryTimeout)
			continue
		}

		// \set varname value - set variable
		if strings.HasPrefix(lowerInput, "\\set ") {
			parts := strings.SplitN(input[5:], " ", 2)
			if len(parts) >= 1 {
				varName := strings.TrimSpace(parts[0])
				varValue := ""
				if len(parts) == 2 {
					varValue = strings.TrimSpace(parts[1])
				}
				variables[varName] = varValue
				fmt.Printf("Set %s = %s\n", varName, varValue)
			}
			continue
		}

		// \unset varname - unset variable
		if strings.HasPrefix(lowerInput, "\\unset ") {
			varName := strings.TrimSpace(input[7:])
			delete(variables, varName)
			fmt.Printf("Unset %s\n", varName)
			continue
		}

		// \pager command - set pager command
		if strings.HasPrefix(lowerInput, "\\pager ") {
			pagerCmd = strings.TrimSpace(input[7:])
			fmt.Printf("Pager set to: %s\n", pagerCmd)
			continue
		}

		// Check for macro selection: m1, m2, etc. (or legacy p1, p2)
		if (strings.HasPrefix(lowerInput, "m") || strings.HasPrefix(lowerInput, "p")) && len(input) > 1 {
			if idx, err := strconv.Atoi(input[1:]); err == nil && idx >= 1 && idx <= len(macros) {
				sql := macros[idx-1].SQL
				fmt.Printf("%s>> %s%s\n", colDim, sql, colReset)
				executeAndPrint(db, sql, queryTimeout)
				continue
			}
		}

		// Substitute variables in SQL
		sql := substituteVariables(input)

		// Remove trailing GO or ; for execution
		sql = strings.TrimSuffix(strings.TrimSpace(sql), ";")
		if strings.HasSuffix(strings.ToUpper(sql), " GO") {
			sql = sql[:len(sql)-3]
		}
		if strings.ToUpper(sql) == "GO" {
			continue
		}

		executeAndPrint(db, sql, queryTimeout)
	}
}

// isCommand checks if input is a CLI command (not SQL)
func isCommand(input string) bool {
	lower := strings.ToLower(input)
	
	// Exact match commands
	commands := []string{
		"exit", "quit", "q", "help", "h", "?", "macros", "m",
		"history", "hist", "clear", "clearhistory",
		"format", "format default", "format ascii", "format unicode",
		"format csv", "format json",
		"\\q", "\\?", "\\g", "\\dt", "\\d", "\\di", "\\dv", "\\dp", "\\df",
		"\\ds", "\\dn", "\\timing", "\\t", "\\o", "\\pager", "\\p",
		"\\set", "\\unset", "\\r", "\\reset",
	}
	for _, cmd := range commands {
		if lower == cmd {
			return true
		}
	}
	
	// Prefix match commands (commands with arguments)
	prefixes := []string{"\\d ", "\\o ", "\\i ", "\\set ", "\\unset ", "\\pager "}
	for _, prefix := range prefixes {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	
	// Macro shortcuts
	if (strings.HasPrefix(lower, "m") || strings.HasPrefix(lower, "p")) && len(input) > 1 {
		if _, err := strconv.Atoi(input[1:]); err == nil {
			return true
		}
	}
	return false
}

// sqlCompleter implements readline.AutoCompleter for SQL
type sqlCompleter struct{}

func (c *sqlCompleter) Do(line []rune, pos int) ([][]rune, int) {
	// Get the word being typed
	lineStr := string(line[:pos])
	words := strings.Fields(lineStr)
	
	var prefix string
	var prefixStart int
	
	if len(lineStr) > 0 && !strings.HasSuffix(lineStr, " ") && len(words) > 0 {
		prefix = words[len(words)-1]
		prefixStart = len(prefix)
	}
	
	upperPrefix := strings.ToUpper(prefix)
	lowerPrefix := strings.ToLower(prefix)
	var candidates [][]rune
	seen := make(map[string]bool)

	// Helper to add candidate
	addCandidate := func(s string) {
		if seen[strings.ToUpper(s)] {
			return
		}
		seen[strings.ToUpper(s)] = true
		candidates = append(candidates, []rune(s[len(prefix):]))
	}

	// Backslash commands at start of line
	if strings.HasPrefix(prefix, "\\") || pos == len(prefix) {
		backslashCmds := []string{
			"\\d", "\\dt", "\\di", "\\dv", "\\dp", "\\df", "\\ds", "\\dn",
			"\\o", "\\i", "\\timing", "\\t", "\\pager", "\\p",
			"\\set", "\\unset", "\\r", "\\reset", "\\q", "\\?", "\\g",
		}
		for _, cmd := range backslashCmds {
			if strings.HasPrefix(cmd, lowerPrefix) {
				addCandidate(cmd)
			}
		}
	}

	// SQL keywords
	for _, kw := range sqlKeywords {
		if strings.HasPrefix(strings.ToUpper(kw), upperPrefix) {
			addCandidate(kw)
		}
	}

	// Table names
	for _, tbl := range tableNames {
		if strings.HasPrefix(strings.ToUpper(tbl), upperPrefix) {
			addCandidate(tbl)
		}
	}

	// Column names - detect context (after FROM tablename or table.column)
	if len(words) >= 2 {
		// Check if previous word was a table name (for table.column completion)
		if strings.Contains(prefix, ".") {
			parts := strings.SplitN(prefix, ".", 2)
			tableName := parts[0]
			colPrefix := ""
			if len(parts) > 1 {
				colPrefix = strings.ToUpper(parts[1])
			}
			if cols, ok := columnNames[tableName]; ok {
				for _, col := range cols {
					if strings.HasPrefix(strings.ToUpper(col), colPrefix) {
						addCandidate(tableName + "." + col)
					}
				}
			}
		} else {
			// Check if we're after FROM, JOIN, UPDATE, INTO, or a table name
			prevWord := strings.ToUpper(words[len(words)-2])
			if prevWord == "FROM" || prevWord == "JOIN" || prevWord == "UPDATE" || 
			   prevWord == "INTO" || prevWord == "TABLE" {
				// Complete table names (already done above)
			} else {
				// Check if previous word is a table name - offer columns
				prevWordLower := words[len(words)-2]
				if cols, ok := columnNames[prevWordLower]; ok {
					for _, col := range cols {
						if strings.HasPrefix(strings.ToUpper(col), upperPrefix) {
							addCandidate(col)
						}
					}
				}
				// Also offer all columns from all tables for SELECT context
				upperLine := strings.ToUpper(lineStr)
				if strings.Contains(upperLine, "SELECT") && !strings.Contains(upperLine, "FROM") {
					for _, cols := range columnNames {
						for _, col := range cols {
							if strings.HasPrefix(strings.ToUpper(col), upperPrefix) {
								addCandidate(col)
							}
						}
					}
				}
			}
		}
	}

	// Commands at start of line
	if pos == len(prefix) {
		commands := []string{"help", "macros", "history", "clear", "quit", "exit", "format"}
		for _, cmd := range commands {
			if strings.HasPrefix(cmd, lowerPrefix) {
				addCandidate(cmd)
			}
		}
		// Macro shortcuts
		for i := range macros {
			m := fmt.Sprintf("m%d", i+1)
			if strings.HasPrefix(m, lowerPrefix) {
				addCandidate(m)
			}
		}
	}

	return candidates, prefixStart
}

func printHelp() {
	fmt.Println(`
Commands:
  <SQL>          Execute SQL statement (end with ; or GO, or press Enter for single line)
  m, macros      Show macro queries
  m<N> or p<N>   Execute macro N (e.g., m1, m2)
  history        Show command history
  clear          Clear screen
  clearhistory   Clear command history
  help, h, ?     Show this help
  exit, quit, q  Exit

Display formats:
  format         Show current format
  format default Simple tabular output (default)
  format ascii   ASCII art table with borders
  format unicode Unicode box-drawing table
  format csv     CSV output
  format json    JSON output

Backslash commands (psql-style):
  \d             List tables (same as \dt)
  \d TABLE       Describe table columns
  \dt            List tables
  \di            List indexes
  \dv            List views
  \dp, \df       List procedures/functions
  \ds            List schemas
  \dn            List databases
  \i FILE        Execute SQL file
  \o [FILE]      Redirect output to file (no arg = stdout)
  \timing, \t    Toggle query timing display
  \pager, \p     Toggle pager for long output
  \pager CMD     Set pager command (default: less -R)
  \set           List variables
  \set VAR VAL   Set variable
  \unset VAR     Unset variable
  \r, \reset     Reload schema (tables/columns)
  \q             Quit

Variables:
  Use :varname or $(varname) in SQL to substitute variable values.
  Example: \set id 42  then  SELECT * FROM users WHERE id = :id

Keyboard shortcuts:
  Up/Down        Navigate history
  Ctrl+R         Reverse search history
  Tab            Autocomplete (SQL, tables, columns, commands)
  Ctrl+A/E       Move to start/end of line
  Ctrl+W         Delete word backward
  Ctrl+U         Clear line
  Ctrl+L         Clear screen

Batch execution (non-interactive):
  -e "SQL"       Execute SQL statement(s) and exit
  -f file.sql    Execute SQL file and exit

Verbosity:
  --silent       Errors only (exit code indicates success/failure)
  --quiet        Results only, no banner or stats
  --verbose      Extra timing and connection info

Colour output:
  Auto-detected from terminal. Override with --color or --no-color flags.
  Environment: NO_COLOR disables, FORCE_COLOR enables.`)
}

func showCurrentFormat() {
	names := []string{"default", "ascii", "unicode", "csv", "json"}
	fmt.Printf("Current format: %s\n", names[displayFormat])
}

func setFormat(f DisplayFormat) {
	displayFormat = f
	names := []string{"default", "ascii", "unicode", "csv", "json"}
	fmt.Printf("Display format set to: %s\n", names[f])
}

// describeTable shows detailed information about a table
func describeTable(db *sql.DB, tableName string, timeout int) {
	query := fmt.Sprintf(`
		SELECT 
			c.name AS column_name,
			t.name AS data_type,
			c.max_length,
			c.precision,
			c.scale,
			CASE WHEN c.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS nullable,
			CASE WHEN c.is_identity = 1 THEN 'YES' ELSE 'NO' END AS identity
		FROM sys.columns c
		JOIN sys.types t ON c.user_type_id = t.user_type_id
		WHERE c.object_id = OBJECT_ID('%s')
		ORDER BY c.column_id
	`, tableName)
	executeAndPrint(db, query, timeout)
}

// setOutputFile redirects output to a file
func setOutputFile(filename string) {
	// Close existing file if any
	if outputFile != nil {
		outputFile.Close()
		outputFile = nil
	}

	if filename == "" {
		outputPath = ""
		fmt.Println("Output redirected to stdout.")
		return
	}

	f, err := os.Create(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError opening file: %v%s\n", colRed, err, colReset)
		return
	}

	outputFile = f
	outputPath = filename
	fmt.Printf("Output redirected to: %s\n", filename)
}

// executeFile reads and executes a SQL file
func executeFile(db *sql.DB, filename string, timeout int) {
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError reading file: %v%s\n", colRed, err, colReset)
		return
	}

	// Execute the script
	batches := splitBatches(string(content))
	for _, batch := range batches {
		batch = strings.TrimSpace(batch)
		if batch == "" {
			continue
		}
		executeAndPrint(db, batch, timeout)
	}
}

// substituteVariables replaces :varname with variable values
func substituteVariables(sql string) string {
	if len(variables) == 0 {
		return sql
	}

	result := sql
	for name, value := range variables {
		// Replace :varname (PostgreSQL style)
		result = strings.ReplaceAll(result, ":"+name, value)
		// Replace $(varname) (shell style)
		result = strings.ReplaceAll(result, "$("+name+")", value)
	}
	return result
}

// getOutput returns the current output writer (file or stdout)
func getOutput() io.Writer {
	if outputFile != nil {
		return outputFile
	}
	return os.Stdout
}

func showMacros() {
	fmt.Println("\nMacros:")
	for i, m := range macros {
		fmt.Printf("  %sm%-2d%s  %s\n", colYellow, i+1, colReset, m.Name)
		// Show SQL if it's short enough
		if len(m.SQL) <= 70 {
			fmt.Printf("       %s%s%s\n", colDim, m.SQL, colReset)
		} else {
			fmt.Printf("       %s%s...%s\n", colDim, m.SQL[:67], colReset)
		}
	}
	fmt.Println("\nMacros can be customized in config.json")
}

func showHistory(rl *readline.Instance) {
	// Read history from the readline instance
	historyFile := rl.Config.HistoryFile
	if historyFile == "" {
		fmt.Println("No history file configured.")
		return
	}

	data, err := os.ReadFile(historyFile)
	if err != nil {
		fmt.Println("No history available.")
		return
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) == 0 {
		fmt.Println("No history.")
		return
	}

	// Show last 20 items
	start := 0
	if len(lines) > 20 {
		start = len(lines) - 20
		fmt.Printf("\nHistory (last 20 of %d):\n", len(lines))
	} else {
		fmt.Println("\nHistory:")
	}

	for i := start; i < len(lines); i++ {
		line := lines[i]
		// Truncate long queries for display
		if len(line) > 70 {
			line = line[:67] + "..."
		}
		fmt.Printf("  %s%3d%s  %s\n", colDim, i+1, colReset, line)
	}
}

func executeAndPrint(db *sql.DB, sqlStr string, timeoutSec int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	start := time.Now()
	rows, err := db.QueryContext(ctx, sqlStr)
	elapsed := time.Since(start)

	out := getOutput()

	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError: %v%s\n", colRed, err, colReset)
		return
	}
	defer rows.Close()

	// Get columns
	cols, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%sError getting columns: %v%s\n", colRed, err, colReset)
		return
	}

	if len(cols) == 0 {
		if showTiming {
			fmt.Fprintf(out, "%sOK (%.2fms)%s\n", colDim, float64(elapsed.Microseconds())/1000, colReset)
		} else {
			fmt.Fprintln(out, "OK")
		}
		return
	}

	// Collect all rows
	var allRows [][]string
	for rows.Next() {
		values := make([]interface{}, len(cols))
		valuePtrs := make([]interface{}, len(cols))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			fmt.Fprintf(os.Stderr, "%sError scanning row: %v%s\n", colRed, err, colReset)
			return
		}

		row := make([]string, len(cols))
		for i, v := range values {
			row[i] = formatValue(v)
		}
		allRows = append(allRows, row)
	}

	if err := rows.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "%sError: %v%s\n", colRed, err, colReset)
		return
	}

	// Output based on format (using pager for large results if enabled)
	outputFunc := func(w io.Writer) {
		switch displayFormat {
		case FormatASCII:
			printASCIITableTo(w, cols, allRows)
		case FormatUnicode:
			printUnicodeTableTo(w, cols, allRows)
		case FormatCSV:
			printCSVTo(w, cols, allRows)
		case FormatJSON:
			printJSONTo(w, cols, allRows)
		default:
			printDefaultTableTo(w, cols, allRows)
		}
	}

	// Use pager for large output in interactive mode (>25 rows)
	if usePager && len(allRows) > 25 && outputFile == nil {
		runWithPager(outputFunc)
	} else {
		outputFunc(out)
	}

	// Print stats (except for CSV/JSON which should be clean)
	if showTiming && displayFormat != FormatCSV && displayFormat != FormatJSON {
		fmt.Fprintf(out, "\n%s(%d rows, %.2fms)%s\n", colDim, len(allRows), float64(elapsed.Microseconds())/1000, colReset)
	}
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

// calculateWidths computes column widths for tabular output
func calculateWidths(cols []string, rows [][]string, maxWidth int) []int {
	widths := make([]int, len(cols))
	for i, col := range cols {
		widths[i] = len(col)
		if widths[i] < 4 {
			widths[i] = 4
		}
	}
	for _, row := range rows {
		for i, v := range row {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}
	// Cap widths
	for i := range widths {
		if widths[i] > maxWidth {
			widths[i] = maxWidth
		}
	}
	return widths
}

// runWithPager pipes output through the configured pager
func runWithPager(outputFunc func(w io.Writer)) {
	// Parse pager command
	parts := strings.Fields(pagerCmd)
	if len(parts) == 0 {
		parts = []string{"less", "-R"}
	}

	cmd := exec.Command(parts[0], parts[1:]...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		// Fall back to direct output
		outputFunc(os.Stdout)
		return
	}

	if err := cmd.Start(); err != nil {
		// Fall back to direct output
		outputFunc(os.Stdout)
		return
	}

	outputFunc(stdin)
	stdin.Close()
	cmd.Wait()
}

// printDefaultTable prints to stdout
func printDefaultTable(cols []string, rows [][]string) {
	printDefaultTableTo(os.Stdout, cols, rows)
}

// printDefaultTableTo prints the simple tabular format to a writer
func printDefaultTableTo(w io.Writer, cols []string, rows [][]string) {
	widths := calculateWidths(cols, rows, 50)

	fmt.Fprintln(w)
	// Header
	for i, v := range cols {
		if len(v) > widths[i] {
			v = v[:widths[i]-3] + "..."
		}
		fmt.Fprintf(w, "%s%-*s%s  ", colBold, widths[i], v, colReset)
	}
	fmt.Fprintln(w)

	// Separator
	for _, wd := range widths {
		fmt.Fprint(w, strings.Repeat("-", wd)+"  ")
	}
	fmt.Fprintln(w)

	// Rows
	for _, row := range rows {
		for i, v := range row {
			if len(v) > widths[i] {
				v = v[:widths[i]-3] + "..."
			}
			if v == "NULL" {
				fmt.Fprintf(w, "%s%-*s%s  ", colDim, widths[i], v, colReset)
			} else {
				fmt.Fprintf(w, "%-*s  ", widths[i], v)
			}
		}
		fmt.Fprintln(w)
	}
}

// printASCIITable prints to stdout
func printASCIITable(cols []string, rows [][]string) {
	printASCIITableTo(os.Stdout, cols, rows)
}

// printASCIITableTo prints an ASCII art bordered table to a writer
func printASCIITableTo(w io.Writer, cols []string, rows [][]string) {
	widths := calculateWidths(cols, rows, 50)

	// Top border
	fmt.Fprint(w, "+")
	for _, wd := range widths {
		fmt.Fprint(w, strings.Repeat("-", wd+2)+"+")
	}
	fmt.Fprintln(w)

	// Header
	fmt.Fprint(w, "|")
	for i, v := range cols {
		if len(v) > widths[i] {
			v = v[:widths[i]-3] + "..."
		}
		fmt.Fprintf(w, " %s%-*s%s |", colBold, widths[i], v, colReset)
	}
	fmt.Fprintln(w)

	// Header separator
	fmt.Fprint(w, "+")
	for _, wd := range widths {
		fmt.Fprint(w, strings.Repeat("=", wd+2)+"+")
	}
	fmt.Fprintln(w)

	// Rows
	for _, row := range rows {
		fmt.Fprint(w, "|")
		for i, v := range row {
			if len(v) > widths[i] {
				v = v[:widths[i]-3] + "..."
			}
			if v == "NULL" {
				fmt.Fprintf(w, " %s%-*s%s |", colDim, widths[i], v, colReset)
			} else {
				fmt.Fprintf(w, " %-*s |", widths[i], v)
			}
		}
		fmt.Fprintln(w)
	}

	// Bottom border
	fmt.Fprint(w, "+")
	for _, wd := range widths {
		fmt.Fprint(w, strings.Repeat("-", wd+2)+"+")
	}
	fmt.Fprintln(w)
}

// printUnicodeTable prints to stdout
func printUnicodeTable(cols []string, rows [][]string) {
	printUnicodeTableTo(os.Stdout, cols, rows)
}

// printUnicodeTableTo prints a Unicode box-drawing bordered table to a writer
func printUnicodeTableTo(w io.Writer, cols []string, rows [][]string) {
	widths := calculateWidths(cols, rows, 50)

	// Box-drawing characters
	const (
		topLeft     = "┌"
		topRight    = "┐"
		bottomLeft  = "└"
		bottomRight = "┘"
		horizontal  = "─"
		vertical    = "│"
		topT        = "┬"
		bottomT     = "┴"
		leftT       = "├"
		rightT      = "┤"
		cross       = "┼"
		dblHoriz    = "═"
	)

	// Top border
	fmt.Fprint(w, topLeft)
	for i, wd := range widths {
		fmt.Fprint(w, strings.Repeat(horizontal, wd+2))
		if i < len(widths)-1 {
			fmt.Fprint(w, topT)
		}
	}
	fmt.Fprintln(w, topRight)

	// Header
	fmt.Fprint(w, vertical)
	for i, v := range cols {
		if len(v) > widths[i] {
			v = v[:widths[i]-3] + "..."
		}
		fmt.Fprintf(w, " %s%-*s%s %s", colBold, widths[i], v, colReset, vertical)
	}
	fmt.Fprintln(w)

	// Header separator (double line)
	fmt.Fprint(w, leftT)
	for i, wd := range widths {
		fmt.Fprint(w, strings.Repeat(dblHoriz, wd+2))
		if i < len(widths)-1 {
			fmt.Fprint(w, cross)
		}
	}
	fmt.Fprintln(w, rightT)

	// Rows
	for _, row := range rows {
		fmt.Fprint(w, vertical)
		for i, v := range row {
			if len(v) > widths[i] {
				v = v[:widths[i]-3] + "..."
			}
			if v == "NULL" {
				fmt.Fprintf(w, " %s%-*s%s %s", colDim, widths[i], v, colReset, vertical)
			} else {
				fmt.Fprintf(w, " %-*s %s", widths[i], v, vertical)
			}
		}
		fmt.Fprintln(w)
	}

	// Bottom border
	fmt.Fprint(w, bottomLeft)
	for i, wd := range widths {
		fmt.Fprint(w, strings.Repeat(horizontal, wd+2))
		if i < len(widths)-1 {
			fmt.Fprint(w, bottomT)
		}
	}
	fmt.Fprintln(w, bottomRight)
}

// printCSV prints to stdout
func printCSV(cols []string, rows [][]string) {
	printCSVTo(os.Stdout, cols, rows)
}

// printCSVTo prints CSV format output to a writer
func printCSVTo(w io.Writer, cols []string, rows [][]string) {
	// Header
	for i, col := range cols {
		if i > 0 {
			fmt.Fprint(w, ",")
		}
		fmt.Fprint(w, csvEscape(col))
	}
	fmt.Fprintln(w)

	// Rows
	for _, row := range rows {
		for i, v := range row {
			if i > 0 {
				fmt.Fprint(w, ",")
			}
			fmt.Fprint(w, csvEscape(v))
		}
		fmt.Fprintln(w)
	}
}

// csvEscape escapes a value for CSV output
func csvEscape(s string) string {
	needsQuote := strings.ContainsAny(s, ",\"\n\r")
	if needsQuote {
		return "\"" + strings.ReplaceAll(s, "\"", "\"\"") + "\""
	}
	return s
}

// printJSON prints to stdout
func printJSON(cols []string, rows [][]string) {
	printJSONTo(os.Stdout, cols, rows)
}

// printJSONTo prints JSON format output to a writer
func printJSONTo(w io.Writer, cols []string, rows [][]string) {
	fmt.Fprintln(w, "[")
	for i, row := range rows {
		fmt.Fprint(w, "  {")
		for j, col := range cols {
			if j > 0 {
				fmt.Fprint(w, ", ")
			}
			fmt.Fprintf(w, "%q: %s", col, jsonValue(row[j]))
		}
		fmt.Fprint(w, "}")
		if i < len(rows)-1 {
			fmt.Fprintln(w, ",")
		} else {
			fmt.Fprintln(w)
		}
	}
	fmt.Fprintln(w, "]")
}

// jsonValue formats a value for JSON output
func jsonValue(s string) string {
	if s == "NULL" {
		return "null"
	}
	// Try to detect if it's a number
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		// Check it's not something weird like "123abc"
		if _, err := strconv.Atoi(s); err == nil {
			return s // Integer
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			return s // Float
		}
	}
	// String - need to escape
	b, _ := json.Marshal(s)
	return string(b)
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
	if v := os.Getenv("IAUL_HISTORY_FILE"); v != "" {
		cfg.HistoryFile = v
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
	if cfg.MaxHistory <= 0 {
		cfg.MaxHistory = defaultMaxHistory
	}
	if cfg.QueryTimeoutS <= 0 {
		cfg.QueryTimeoutS = defaultQueryTimeout
	}
	if cfg.HistoryFile == "" {
		// Default to ~/.iaul_history
		if home, err := os.UserHomeDir(); err == nil {
			cfg.HistoryFile = filepath.Join(home, ".iaul_history")
		}
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
