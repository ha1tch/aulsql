package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ha1tch/aul/pkg/version"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/server"

	// Protocol implementations (register via init())
	_ "github.com/ha1tch/aul/protocol/http"
	_ "github.com/ha1tch/aul/protocol/postgres"
	_ "github.com/ha1tch/aul/protocol/tds"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("aul", flag.ContinueOnError)
	fs.SetOutput(stderr)

	var (
		// Server configuration
		configFile  = fs.String("c", "", "Configuration file path")
		configFileL = fs.String("config", "", "Configuration file path")
		procDir     = fs.String("d", "./procedures", "Directory containing stored procedures")
		procDirL    = fs.String("proc-dir", "./procedures", "Directory containing stored procedures")
		watchFiles  = fs.Bool("w", false, "Watch for file changes and hot-reload")
		watchFilesL = fs.Bool("watch", false, "Watch for file changes and hot-reload")

		// Protocol listeners
		tdsPort      = fs.Int("tds-port", 0, "TDS protocol port (0 = disabled)")
		postgresPort = fs.Int("pg-port", 0, "PostgreSQL protocol port (0 = disabled)")
		mysqlPort    = fs.Int("mysql-port", 0, "MySQL protocol port (0 = disabled)")
		httpPort     = fs.Int("http-port", 8080, "HTTP API port (0 = disabled)")
		grpcPort     = fs.Int("grpc-port", 0, "gRPC port (0 = disabled)")

		// Runtime options
		dialect      = fs.String("dialect", "tsql", "Default SQL dialect (tsql, postgres, mysql)")
		jitEnabled   = fs.Bool("jit", true, "Enable JIT compilation")
		jitThreshold = fs.Int("jit-threshold", 100, "Execution count before JIT compilation")
		maxConns     = fs.Int("max-conns", 1000, "Maximum concurrent connections")
		execTimeout  = fs.Duration("exec-timeout", 30*time.Second, "Default execution timeout")

		// Storage options
		storageType = fs.String("storage", "sqlite", "Storage backend: memory, sqlite")
		storagePath = fs.String("storage-path", ":memory:", "Storage path (for sqlite: file path or :memory:)")

		// Logging
		logLevel  = fs.String("log-level", "info", "Log level (debug, info, warn, error)")
		logFormat = fs.String("log-format", "text", "Log format (text, json)")

		// Help and version
		showHelp     = fs.Bool("h", false, "Show help")
		showHelpL    = fs.Bool("help", false, "Show help")
		showVersion  = fs.Bool("v", false, "Show version")
		showVersionL = fs.Bool("version", false, "Show version")
		noBanner     = fs.Bool("no-banner", false, "Suppress startup banner")
	)

	fs.Usage = func() {
		printUsage(stderr)
	}

	if err := fs.Parse(args); err != nil {
		return 2
	}

	// Coalesce short and long flags
	if *configFileL != "" {
		*configFile = *configFileL
	}
	if *procDirL != "./procedures" {
		*procDir = *procDirL
	}
	if *watchFilesL {
		*watchFiles = true
	}
	if *showHelpL {
		*showHelp = true
	}
	if *showVersionL {
		*showVersion = true
	}

	if *showHelp {
		printUsage(stdout)
		return 0
	}

	if *showVersion {
		fmt.Fprintln(stdout, version.Full())
		return 0
	}

	// Build configuration
	cfg := server.DefaultConfig()
	cfg.Version = version.Version
	cfg.ProcedureDir = *procDir
	cfg.WatchChanges = *watchFiles
	cfg.DefaultDialect = *dialect
	cfg.JITEnabled = *jitEnabled
	cfg.JITThreshold = *jitThreshold
	cfg.MaxConcurrency = *maxConns
	cfg.ExecTimeout = *execTimeout
	cfg.LogLevel = *logLevel
	cfg.LogFormat = *logFormat

	// Configure storage backend
	cfg.StorageConfig.Type = *storageType
	if cfg.StorageConfig.Options == nil {
		cfg.StorageConfig.Options = make(map[string]string)
	}
	cfg.StorageConfig.Options["path"] = *storagePath

	// Load config file if specified
	if *configFile != "" {
		if err := loadConfigFile(*configFile, &cfg); err != nil {
			fmt.Fprintf(stderr, "error loading config: %v\n", err)
			return 1
		}
	}

	// Configure protocol listeners
	if *tdsPort > 0 {
		cfg.Listeners = append(cfg.Listeners, protocol.ListenerConfig{
			Name:     "tds",
			Protocol: protocol.ProtocolTDS,
			Port:     *tdsPort,
		})
	}
	if *postgresPort > 0 {
		cfg.Listeners = append(cfg.Listeners, protocol.ListenerConfig{
			Name:     "postgres",
			Protocol: protocol.ProtocolPostgres,
			Port:     *postgresPort,
		})
	}
	if *mysqlPort > 0 {
		cfg.Listeners = append(cfg.Listeners, protocol.ListenerConfig{
			Name:     "mysql",
			Protocol: protocol.ProtocolMySQL,
			Port:     *mysqlPort,
		})
	}
	if *httpPort > 0 {
		cfg.Listeners = append(cfg.Listeners, protocol.ListenerConfig{
			Name:     "http",
			Protocol: protocol.ProtocolHTTP,
			Port:     *httpPort,
		})
	}
	if *grpcPort > 0 {
		cfg.Listeners = append(cfg.Listeners, protocol.ListenerConfig{
			Name:     "grpc",
			Protocol: protocol.ProtocolGRPC,
			Port:     *grpcPort,
		})
	}

	// Create server
	srv, err := server.New(cfg)
	if err != nil {
		fmt.Fprintf(stderr, "error creating server: %v\n", err)
		return 1
	}

	logger := srv.Logger()

	// Start server
	if err := srv.Start(); err != nil {
		fmt.Fprintf(stderr, "error starting server: %v\n", err)
		return 1
	}

	// Print startup banner to stdout for visibility
	if !*noBanner {
		fmt.Fprint(stdout, `
      ,___,
     (O,O )
     /)___)
      "--"
`)
	}
	fmt.Fprintf(stdout, "aul server started (version %s)\n", version.Version)
	fmt.Fprintf(stdout, "  Storage: %s (%s)\n", *storageType, *storagePath)
	fmt.Fprintf(stdout, "  Procedures loaded: %d\n", srv.Registry().Count())
	fmt.Fprintf(stdout, "  JIT enabled: %v (threshold: %d)\n", cfg.JITEnabled, cfg.JITThreshold)
	for _, l := range cfg.Listeners {
		fmt.Fprintf(stdout, "  Listening: %s on port %d\n", l.Protocol, l.Port)
	}

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	logger.System().Info("shutdown signal received", "signal", sig.String())
	fmt.Fprintln(stdout, "\nShutting down...")

	// Graceful shutdown
	if err := srv.Stop(); err != nil {
		fmt.Fprintf(stderr, "error stopping server: %v\n", err)
		return 1
	}

	fmt.Fprintln(stdout, "Server stopped")
	return 0
}

// loadConfigFile loads configuration from a file.
func loadConfigFile(path string, cfg *server.Config) error {
	// TODO: Implement YAML/JSON config file loading
	return fmt.Errorf("config file loading not yet implemented")
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `aul - Multi-protocol database server with JIT-compiled stored procedures

Usage:
  aul [options]

Server Options:
  -c, --config <file>      Configuration file path
  -d, --proc-dir <path>    Directory containing stored procedures (default: ./procedures)
  -w, --watch              Watch for file changes and hot-reload

Protocol Listeners:
  --tds-port <port>        TDS protocol port (SQL Server compatible, 0 = disabled)
  --pg-port <port>         PostgreSQL wire protocol port (0 = disabled)
  --mysql-port <port>      MySQL wire protocol port (0 = disabled)
  --http-port <port>       HTTP REST API port (default: 8080, 0 = disabled)
  --grpc-port <port>       gRPC port (0 = disabled)

Runtime Options:
  --dialect <name>         Default SQL dialect: tsql, postgres, mysql (default: tsql)
  --jit                    Enable JIT compilation (default: true)
  --jit-threshold <n>      Execution count before JIT compilation (default: 100)
  --max-conns <n>          Maximum concurrent connections (default: 1000)
  --exec-timeout <dur>     Default execution timeout (default: 30s)

Storage Options:
  --storage <type>         Storage backend: memory, sqlite (default: sqlite)
  --storage-path <path>    Storage path for sqlite (default: :memory:)

Logging:
  --log-level <level>      Log level: debug, info, warn, error (default: info)
  --log-format <format>    Log format: text, json (default: text)

General:
  -h, --help               Show help
  -v, --version            Show version
  --no-banner              Suppress startup banner

Examples:
  # Start with HTTP API only
  aul --http-port 8080

  # Start with TDS protocol (SQL Server compatible)
  aul --tds-port 1433

  # Start with multiple protocols
  aul --tds-port 1433 --pg-port 5432 --http-port 8080

  # Disable JIT compilation
  aul --jit=false

  # Watch for procedure file changes
  aul -w -d ./my_procedures

  # Use configuration file
  aul -c /etc/aul/config.yaml

Architecture:
  aul loads stored procedures from SQL files, executes them using tgpiler's
  runtime interpreter, and automatically JIT-compiles frequently-used
  procedures for optimised performance.

  Supported protocols allow connections from:
  - SQL Server clients (TDS)
  - PostgreSQL clients (pg wire protocol)
  - MySQL clients (MySQL protocol)
  - HTTP clients (REST API)
  - gRPC clients

Exit Codes:
  0  Success
  1  Runtime error
  2  CLI usage error
`)
}
