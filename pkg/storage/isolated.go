// Package storage provides storage backend implementations for aul.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ha1tch/aul/pkg/annotations"
)

// IsolatedTableManager manages per-table SQLite files for isolated tables.
//
// Directory structure:
//
//	{BaseDir}/
//	├── {database}/
//	│   ├── {schema}.{table}.db
//	│   └── {schema}.{table2}.db
//	└── {database2}/
//	    └── {schema}.{table}.db
type IsolatedTableManager struct {
	mu sync.RWMutex

	// Configuration
	baseDir string

	// Default SQLite settings (can be overridden by annotations)
	defaultJournalMode string
	defaultSynchronous string
	defaultCacheSize   int

	// Connection pools per isolated table
	// Key: "database.schema.table"
	connections map[string]*sql.DB

	// Metadata for each table
	metadata map[string]*TableMetadata
}

// IsolatedTableConfig holds configuration for the isolated table manager.
type IsolatedTableConfig struct {
	BaseDir        string
	JournalMode    string
	Synchronous    string
	CacheSize      int
	MaxOpenConns   int
	MaxIdleConns   int
}

// DefaultIsolatedTableConfig returns sensible defaults.
func DefaultIsolatedTableConfig() IsolatedTableConfig {
	return IsolatedTableConfig{
		BaseDir:      "./data/isolated",
		JournalMode:  "WAL",
		Synchronous:  "NORMAL",
		CacheSize:    -2000, // 2MB
		MaxOpenConns: 3,
		MaxIdleConns: 1,
	}
}

// NewIsolatedTableManager creates a new isolated table manager.
func NewIsolatedTableManager(cfg IsolatedTableConfig) (*IsolatedTableManager, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(cfg.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &IsolatedTableManager{
		baseDir:            cfg.BaseDir,
		defaultJournalMode: cfg.JournalMode,
		defaultSynchronous: cfg.Synchronous,
		defaultCacheSize:   cfg.CacheSize,
		connections:        make(map[string]*sql.DB),
		metadata:           make(map[string]*TableMetadata),
	}, nil
}

// tableKey generates a unique key for a table.
func (m *IsolatedTableManager) tableKey(database, schema, table string) string {
	if schema == "" {
		schema = "dbo"
	}
	return fmt.Sprintf("%s.%s.%s", database, schema, table)
}

// tablePath generates the file path for an isolated table.
func (m *IsolatedTableManager) tablePath(database, schema, table string) string {
	if schema == "" {
		schema = "dbo"
	}
	// Sanitise names
	database = sanitisePathComponent(database)
	schema = sanitisePathComponent(schema)
	table = sanitisePathComponent(table)

	filename := fmt.Sprintf("%s.%s.db", schema, table)
	return filepath.Join(m.baseDir, database, filename)
}

// IsIsolated checks if a table is managed as isolated.
func (m *IsolatedTableManager) IsIsolated(database, schema, table string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := m.tableKey(database, schema, table)
	_, ok := m.metadata[key]
	return ok
}

// GetConnection returns the database connection for an isolated table.
// Returns nil if the table is not isolated.
func (m *IsolatedTableManager) GetConnection(database, schema, table string) (*sql.DB, error) {
	key := m.tableKey(database, schema, table)

	// Fast path: check if connection exists
	m.mu.RLock()
	if conn, ok := m.connections[key]; ok {
		m.mu.RUnlock()
		return conn, nil
	}
	m.mu.RUnlock()

	// Check if table is registered
	m.mu.RLock()
	meta, ok := m.metadata[key]
	m.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("table not registered as isolated: %s", key)
	}

	// Slow path: open connection
	m.mu.Lock()
	defer m.mu.Unlock()

	// Double-check after acquiring write lock
	if conn, ok := m.connections[key]; ok {
		return conn, nil
	}

	// Open the database file
	dbPath := m.tablePath(database, schema, table)
	dsn := m.buildDSN(dbPath, meta.Annotations)

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open isolated table database: %w", err)
	}

	// Configure connection pool (smaller for isolated tables)
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping isolated table database: %w", err)
	}

	m.connections[key] = db
	return db, nil
}

// buildDSN builds a SQLite DSN with settings from annotations.
func (m *IsolatedTableManager) buildDSN(dbPath string, ann annotations.AnnotationSet) string {
	dsn := dbPath
	opts := []string{}

	// Journal mode from annotation or default
	journalMode := ann.GetString("journal-mode", m.defaultJournalMode)
	if journalMode != "" {
		opts = append(opts, fmt.Sprintf("_journal_mode=%s", journalMode))
	}

	// Synchronous from annotation or default
	synchronous := ann.GetString("synchronous", m.defaultSynchronous)
	if synchronous != "" {
		opts = append(opts, fmt.Sprintf("_synchronous=%s", synchronous))
	}

	// Cache size from annotation or default
	cacheSize := ann.GetInt("cache-size", m.defaultCacheSize)
	if cacheSize != 0 {
		opts = append(opts, fmt.Sprintf("_cache_size=%d", cacheSize))
	}

	// Busy timeout
	opts = append(opts, "_busy_timeout=5000")

	// Foreign keys
	opts = append(opts, "_foreign_keys=ON")

	if len(opts) > 0 {
		dsn = dsn + "?" + strings.Join(opts, "&")
	}

	return dsn
}

// CreateTable creates an isolated table from DDL.
func (m *IsolatedTableManager) CreateTable(ctx context.Context, database, schema, table, ddl string, ann annotations.AnnotationSet) error {
	if schema == "" {
		schema = "dbo"
	}

	key := m.tableKey(database, schema, table)
	dbPath := m.tablePath(database, schema, table)

	// Ensure database directory exists
	dbDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("failed to create database directory: %w", err)
	}

	// Check if already exists
	m.mu.RLock()
	_, exists := m.metadata[key]
	m.mu.RUnlock()

	if exists {
		return fmt.Errorf("isolated table already exists: %s", key)
	}

	// Create and open the database
	dsn := m.buildDSN(dbPath, ann)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("failed to create isolated table database: %w", err)
	}

	// Configure pool
	db.SetMaxOpenConns(3)
	db.SetMaxIdleConns(1)

	// Execute the DDL to create the table
	// Strip annotations from DDL before executing
	cleanDDL := stripAnnotations(ddl)

	_, err = db.ExecContext(ctx, cleanDDL)
	if err != nil {
		db.Close()
		os.Remove(dbPath) // Clean up on failure
		return fmt.Errorf("failed to execute CREATE TABLE: %w", err)
	}

	// Store metadata
	meta := &TableMetadata{
		Database:    database,
		Schema:      schema,
		Name:        table,
		Annotations: ann,
	}

	m.mu.Lock()
	m.connections[key] = db
	m.metadata[key] = meta
	m.mu.Unlock()

	return nil
}

// stripAnnotations removes -- @aul: lines from DDL.
func stripAnnotations(ddl string) string {
	lines := strings.Split(ddl, "\n")
	var clean []string
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "-- @aul:") {
			clean = append(clean, line)
		}
	}
	return strings.Join(clean, "\n")
}

// DropTable removes an isolated table and its database file.
func (m *IsolatedTableManager) DropTable(ctx context.Context, database, schema, table string) error {
	if schema == "" {
		schema = "dbo"
	}

	key := m.tableKey(database, schema, table)
	dbPath := m.tablePath(database, schema, table)

	m.mu.Lock()
	defer m.mu.Unlock()

	// Close connection if open
	if conn, ok := m.connections[key]; ok {
		conn.Close()
		delete(m.connections, key)
	}

	// Remove metadata
	delete(m.metadata, key)

	// Remove database file
	if err := os.Remove(dbPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove isolated table file: %w", err)
	}

	// Also try to remove WAL and SHM files
	os.Remove(dbPath + "-wal")
	os.Remove(dbPath + "-shm")

	return nil
}

// RegisterExisting registers an existing isolated table without creating it.
// Use this when loading table metadata from a catalogue.
func (m *IsolatedTableManager) RegisterExisting(meta *TableMetadata) error {
	if !meta.IsIsolated() {
		return fmt.Errorf("table is not marked as isolated: %s", meta.QualifiedName())
	}

	key := m.tableKey(meta.Database, meta.Schema, meta.Name)
	dbPath := m.tablePath(meta.Database, meta.Schema, meta.Name)

	// Check file exists
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		return fmt.Errorf("isolated table file not found: %s", dbPath)
	}

	m.mu.Lock()
	m.metadata[key] = meta
	m.mu.Unlock()

	return nil
}

// Query executes a query on an isolated table.
func (m *IsolatedTableManager) Query(ctx context.Context, database, schema, table, sqlStr string, args ...interface{}) (*sql.Rows, error) {
	db, err := m.GetConnection(database, schema, table)
	if err != nil {
		return nil, err
	}

	return db.QueryContext(ctx, sqlStr, args...)
}

// Exec executes a statement on an isolated table.
func (m *IsolatedTableManager) Exec(ctx context.Context, database, schema, table, sqlStr string, args ...interface{}) (sql.Result, error) {
	// Check for read-only
	m.mu.RLock()
	key := m.tableKey(database, schema, table)
	meta, ok := m.metadata[key]
	m.mu.RUnlock()

	if ok && meta.IsReadOnly() {
		return nil, fmt.Errorf("table is read-only: %s", key)
	}

	db, err := m.GetConnection(database, schema, table)
	if err != nil {
		return nil, err
	}

	return db.ExecContext(ctx, sqlStr, args...)
}

// ListTables returns all registered isolated tables.
func (m *IsolatedTableManager) ListTables() []*TableMetadata {
	m.mu.RLock()
	defer m.mu.RUnlock()

	tables := make([]*TableMetadata, 0, len(m.metadata))
	for _, meta := range m.metadata {
		tables = append(tables, meta)
	}
	return tables
}

// Close closes all connections.
func (m *IsolatedTableManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var errs []error
	for key, conn := range m.connections {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close %s: %w", key, err))
		}
		delete(m.connections, key)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}

	return nil
}

// Stats returns statistics about the isolated table manager.
type IsolatedTableStats struct {
	TableCount      int
	ConnectionCount int
	Tables          []string
}

func (m *IsolatedTableManager) Stats() IsolatedTableStats {
	m.mu.RLock()
	defer m.mu.RUnlock()

	stats := IsolatedTableStats{
		TableCount:      len(m.metadata),
		ConnectionCount: len(m.connections),
		Tables:          make([]string, 0, len(m.metadata)),
	}

	for key := range m.metadata {
		stats.Tables = append(stats.Tables, key)
	}

	return stats
}
