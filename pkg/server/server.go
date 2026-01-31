// Package server provides the core aul database server implementation.
//
// The server coordinates between protocol listeners (accepting client connections),
// the procedure registry (managing stored procedures), and the runtime (executing
// procedures either interpreted or JIT-compiled).
package server

import (
	"context"
	"io"
	"sync"
	"time"

	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/pkg/procedure"
	"github.com/ha1tch/aul/pkg/protocol"
	"github.com/ha1tch/aul/pkg/runtime"
	"github.com/ha1tch/aul/pkg/storage"
)

// Server is the main aul database server.
type Server struct {
	mu sync.RWMutex

	// Configuration
	config Config

	// Logging
	logger *log.Logger

	// Core components
	registry         *procedure.Registry
	runtime          *runtime.Runtime
	storage          runtime.StorageBackend
	tenantIdentifier *TenantIdentifier

	// Protocol listeners
	listeners map[string]protocol.Listener

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// State
	state     State
	startTime time.Time
}

// State represents the server's current state.
type State int

const (
	StateNew State = iota
	StateStarting
	StateRunning
	StateStopping
	StateStopped
)

func (s State) String() string {
	switch s {
	case StateNew:
		return "new"
	case StateStarting:
		return "starting"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateStopped:
		return "stopped"
	default:
		return "unknown"
	}
}

// Config holds server configuration.
type Config struct {
	// Server identification
	Name    string
	Version string

	// Procedure storage
	ProcedureDir string // Directory containing .sql files
	WatchChanges bool   // Hot-reload procedures on file changes

	// Runtime configuration
	DefaultDialect string        // Default SQL dialect (tsql, postgres, mysql)
	JITThreshold   int           // Execution count before JIT compilation
	JITEnabled     bool          // Enable JIT compilation
	MaxConcurrency int           // Maximum concurrent executions
	ExecTimeout    time.Duration // Default execution timeout

	// Multi-tenancy
	TenantConfig TenantConfig

	// Protocol listeners to enable
	Listeners []protocol.ListenerConfig

	// Storage backend configuration
	StorageConfig runtime.StorageConfig

	// Logging
	LogLevel            string
	LogFormat           string      // "text" or "json"
	LogQueries          bool        // Log all SQL queries
	LogQueriesRewritten bool        // Log queries after rewriting
	Logger              *log.Logger // Optional pre-configured logger
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Name:           "aul",
		Version:        "0.1.0",
		ProcedureDir:   "./procedures",
		WatchChanges:   false,
		DefaultDialect: "tsql",
		JITThreshold:   100,
		JITEnabled:     true,
		MaxConcurrency: 100,
		ExecTimeout:    30 * time.Second,
		LogLevel:       "info",
		LogFormat:      "text",
	}
}

// New creates a new server with the given configuration.
func New(cfg Config) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialise logger
	var logger *log.Logger
	if cfg.Logger != nil {
		logger = cfg.Logger
	} else {
		level, _ := log.ParseLevel(cfg.LogLevel)
		format := log.FormatText
		if cfg.LogFormat == "json" {
			format = log.FormatJSON
		}
		logger = log.New(log.Config{
			DefaultLevel:  level,
			Format:        format,
			IncludeCaller: level == log.LevelDebug,
		})
	}

	s := &Server{
		config:           cfg,
		logger:           logger,
		listeners:        make(map[string]protocol.Listener),
		tenantIdentifier: NewTenantIdentifier(cfg.TenantConfig),
		ctx:              ctx,
		cancel:           cancel,
		state:            StateNew,
	}

	// Initialise procedure registry
	s.registry = procedure.NewRegistry()

	// Initialise runtime with logger
	rtCfg := runtime.Config{
		DefaultDialect:      cfg.DefaultDialect,
		JITEnabled:          cfg.JITEnabled,
		JITThreshold:        cfg.JITThreshold,
		MaxConcurrency:      cfg.MaxConcurrency,
		ExecTimeout:         cfg.ExecTimeout,
		LogQueriesRewritten: cfg.LogQueriesRewritten,
	}
	s.runtime = runtime.New(rtCfg, s.registry, logger)

	logger.System().Info("server initialised",
		"name", cfg.Name,
		"version", cfg.Version,
		"jit_enabled", cfg.JITEnabled,
		"tenancy_enabled", cfg.TenantConfig.Enabled,
	)

	return s, nil
}

// Start starts the server and all configured listeners.
func (s *Server) Start() error {
	s.mu.Lock()
	if s.state != StateNew && s.state != StateStopped {
		s.mu.Unlock()
		return aulerrors.Newf(aulerrors.ErrCodeExecInvalidState,
			"server cannot start from state %s", s.state).
			WithOp("Server.Start").
			Err()
	}
	s.state = StateStarting
	s.mu.Unlock()

	s.logger.System().Info("server starting")

	// Load procedures from directory
	if s.config.ProcedureDir != "" {
		if err := s.loadProcedures(); err != nil {
			return aulerrors.Wrap(err, aulerrors.ErrCodeProcLoadError,
				"failed to load procedures").
				WithOp("Server.Start").
				WithField("directory", s.config.ProcedureDir).
				Err()
		}
	}

	// Initialise storage backend
	if err := s.initStorage(); err != nil {
		return aulerrors.Wrap(err, aulerrors.ErrCodeStorageConnect,
			"failed to initialise storage").
			WithOp("Server.Start").
			Err()
	}

	// Start protocol listeners
	for _, lcfg := range s.config.Listeners {
		if err := s.startListener(lcfg); err != nil {
			s.Stop() // Clean up any started listeners
			return aulerrors.Wrap(err, aulerrors.ErrCodeConnectionFailed,
				"failed to start listener").
				WithOp("Server.Start").
				WithField("protocol", lcfg.Protocol).
				WithField("port", lcfg.Port).
				Err()
		}
	}

	s.mu.Lock()
	s.state = StateRunning
	s.startTime = time.Now()
	s.mu.Unlock()

	s.logger.System().Info("server started",
		"state", "running",
		"procedures", s.registry.Count(),
		"listeners", len(s.listeners),
	)

	return nil
}

// Stop gracefully stops the server.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.state != StateRunning && s.state != StateStarting {
		s.mu.Unlock()
		return nil
	}
	s.state = StateStopping
	s.mu.Unlock()

	s.logger.System().Info("server stopping")

	// Signal all goroutines to stop
	s.cancel()

	// Stop all listeners
	for name, listener := range s.listeners {
		if err := listener.Close(); err != nil {
			s.logger.System().Error("failed to close listener", err,
				"listener", name,
				"protocol", listener.Protocol(),
			)
		}
	}

	// Wait for all goroutines
	s.wg.Wait()

	// Close storage
	if s.storage != nil {
		s.storage.Close()
	}

	// Close logger
	if s.logger != nil {
		s.logger.Close()
	}

	s.mu.Lock()
	s.state = StateStopped
	s.mu.Unlock()

	s.logger.System().Info("server stopped")

	return nil
}

// State returns the current server state.
func (s *Server) State() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// Uptime returns how long the server has been running.
func (s *Server) Uptime() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.state != StateRunning {
		return 0
	}
	return time.Since(s.startTime)
}

// Registry returns the procedure registry.
func (s *Server) Registry() *procedure.Registry {
	return s.registry
}

// Runtime returns the execution runtime.
func (s *Server) Runtime() *runtime.Runtime {
	return s.runtime
}

// Stats returns server statistics.
func (s *Server) Stats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := Stats{
		State:       s.state.String(),
		Uptime:      s.Uptime(),
		Procedures:  s.registry.Count(),
		Listeners:   len(s.listeners),
		JITEnabled:  s.config.JITEnabled,
		JITCompiled: s.runtime.JITStats().CompiledCount,
	}

	// Collect listener stats
	for name, listener := range s.listeners {
		stats.ListenerStats = append(stats.ListenerStats, ListenerStats{
			Name:        name,
			Protocol:    string(listener.Protocol()),
			Connections: listener.ConnectionCount(),
		})
	}

	return stats
}

// Stats holds server statistics.
type Stats struct {
	State         string
	Uptime        time.Duration
	Procedures    int
	Listeners     int
	JITEnabled    bool
	JITCompiled   int
	ListenerStats []ListenerStats
}

// ListenerStats holds statistics for a single listener.
type ListenerStats struct {
	Name        string
	Protocol    string
	Connections int
}

// loadProcedures loads all procedures from the configured directory.
func (s *Server) loadProcedures() error {
	s.logger.Application().Info("loading procedures",
		"directory", s.config.ProcedureDir,
	)

	loader := procedure.NewLoader(s.config.DefaultDialect, s.logger)
	procs, err := loader.LoadDir(s.config.ProcedureDir)
	if err != nil {
		return err
	}

	for _, proc := range procs {
		if err := s.registry.Register(proc); err != nil {
			return aulerrors.Wrap(err, aulerrors.ErrCodeProcAlreadyExists,
				"failed to register procedure").
				WithField("procedure", proc.Name).
				Err()
		}
		s.logger.Application().Debug("procedure loaded",
			"name", proc.QualifiedName(),
			"dialect", proc.Dialect,
			"source_file", proc.SourceFile,
		)
	}

	s.logger.Application().Info("procedures loaded",
		"count", len(procs),
	)

	return nil
}

// initStorage initialises the storage backend.
func (s *Server) initStorage() error {
	var err error

	switch s.config.StorageConfig.Type {
	case "sqlite":
		s.storage, err = s.initSQLiteStorage()
		if err != nil {
			return err
		}
		// Wire up registry to storage for system catalog queries
		if sqliteStorage, ok := s.storage.(*storage.SQLiteStorage); ok {
			sqliteStorage.SetRegistry(s.registry)
		}
		s.logger.System().Info("SQLite storage initialised",
			"path", s.config.StorageConfig.Options["path"],
		)

	case "memory", "":
		s.storage = runtime.NewMemoryStorage()
		s.logger.System().Info("in-memory storage initialised")

	default:
		return aulerrors.Newf(aulerrors.ErrCodeConfigInvalid,
			"unsupported storage type: %s", s.config.StorageConfig.Type).
			WithOp("Server.initStorage").
			Err()
	}

	s.runtime.SetStorage(s.storage)
	return nil
}

// initSQLiteStorage creates a SQLite storage backend.
func (s *Server) initSQLiteStorage() (runtime.StorageBackend, error) {
	cfg := s.config.StorageConfig

	// Get path from options or use default
	path := ":memory:"
	if p, ok := cfg.Options["path"]; ok && p != "" {
		path = p
	}

	// Build SQLite config
	sqliteCfg := storage.SQLiteConfig{
		Path:         path,
		MaxOpenConns: cfg.MaxOpenConns,
		MaxIdleConns: cfg.MaxIdleConns,
		JournalMode:  "WAL",
		Synchronous:  "NORMAL",
		CacheSize:    -2000,
		BusyTimeout:  5000,
	}

	// Override with options if provided
	if jm, ok := cfg.Options["journal_mode"]; ok {
		sqliteCfg.JournalMode = jm
	}
	if sync, ok := cfg.Options["synchronous"]; ok {
		sqliteCfg.Synchronous = sync
	}

	return storage.NewSQLiteStorage(sqliteCfg)
}

// startListener starts a protocol listener.
func (s *Server) startListener(cfg protocol.ListenerConfig) error {
	s.logger.System().Info("starting listener",
		"protocol", cfg.Protocol,
		"port", cfg.Port,
		"name", cfg.Name,
	)

	listener, err := protocol.NewListener(cfg, s.logger)
	if err != nil {
		return err
	}

	// Start listening before launching the accept goroutine
	if err := listener.Listen(); err != nil {
		return err
	}

	s.listeners[cfg.Name] = listener

	// Start accepting connections
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.acceptLoop(listener)
	}()

	s.logger.System().Info("listener started",
		"protocol", cfg.Protocol,
		"address", listener.Addr().String(),
	)

	return nil
}

// acceptLoop accepts connections from a listener.
func (s *Server) acceptLoop(listener protocol.Listener) {
	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		conn, err := listener.Accept()
		if err != nil {
			// Check if we're shutting down
			select {
			case <-s.ctx.Done():
				return
			default:
				// Only log real errors, not temporary ones
				if err != io.EOF && !isTemporaryError(err) {
					s.logger.Application().Error("accept failed", err,
						"protocol", listener.Protocol(),
					)
				}
				continue
			}
		}

		s.logger.Application().Debug("connection accepted",
			"protocol", listener.Protocol(),
			"remote_addr", conn.RemoteAddr().String(),
		)

		// Handle connection in new goroutine
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

// isTemporaryError checks if an error is temporary and can be ignored.
func isTemporaryError(err error) bool {
	if err == nil {
		return false
	}
	// Check for common temporary error messages
	errStr := err.Error()
	return errStr == "listener not started" ||
		errStr == "use of closed network connection"
}

// handleConnection handles a single client connection.
// handleConnection handles a single client connection.
func (s *Server) handleConnection(conn protocol.Connection) {
	defer conn.Close()

	// Extract tenant from connection if multi-tenancy is enabled
	var tenant string
	if s.tenantIdentifier.IsEnabled() {
		// Build tenant sources from connection properties
		sources := &MapTenantSources{
			TDSProperties: conn.Properties(),
		}
		
		var err error
		tenant, err = s.tenantIdentifier.Identify(sources)
		if err != nil {
			s.logger.Application().Error("tenant identification failed", err,
				"remote_addr", conn.RemoteAddr().String(),
			)
			// Connection will proceed without tenant (single-tenant mode)
		}
	}

	handler := NewConnectionHandlerWithTenant(conn, s.runtime, s.registry, s.logger, tenant, s.config.LogQueries)
	handler.Serve(s.ctx)
}

// Logger returns the server's logger.
func (s *Server) Logger() *log.Logger {
	return s.logger
}
