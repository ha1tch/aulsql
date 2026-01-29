// Package storage provides storage backend implementations for aul.
package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ha1tch/aul/runtime"
)

// SQLiteStorage provides a SQLite storage backend.
type SQLiteStorage struct {
	mu sync.RWMutex
	db *sql.DB

	// Active transactions (txnID -> *sql.Tx)
	transactions map[string]*sql.Tx

	// Path to database file (":memory:" for in-memory)
	path string
}

// SQLiteConfig holds SQLite-specific configuration.
type SQLiteConfig struct {
	// Path to database file. Use ":memory:" for in-memory database.
	Path string

	// Connection pool settings
	MaxOpenConns int
	MaxIdleConns int

	// SQLite-specific options
	JournalMode string // WAL, DELETE, TRUNCATE, PERSIST, MEMORY, OFF
	Synchronous string // OFF, NORMAL, FULL, EXTRA
	CacheSize   int    // Number of pages (negative = KB)
	BusyTimeout int    // Milliseconds
}

// DefaultSQLiteConfig returns sensible defaults for SQLite.
func DefaultSQLiteConfig() SQLiteConfig {
	return SQLiteConfig{
		Path:         ":memory:",
		MaxOpenConns: 1, // SQLite prefers single writer
		MaxIdleConns: 1,
		JournalMode:  "WAL",
		Synchronous:  "NORMAL",
		CacheSize:    -2000, // 2MB
		BusyTimeout:  5000,  // 5 seconds
	}
}

// NewSQLiteStorage creates a new SQLite storage backend.
func NewSQLiteStorage(cfg SQLiteConfig) (*SQLiteStorage, error) {
	// Build DSN with options
	dsn := cfg.Path
	opts := []string{}

	if cfg.CacheSize != 0 {
		opts = append(opts, fmt.Sprintf("_cache_size=%d", cfg.CacheSize))
	}
	if cfg.BusyTimeout > 0 {
		opts = append(opts, fmt.Sprintf("_busy_timeout=%d", cfg.BusyTimeout))
	}
	if cfg.JournalMode != "" {
		opts = append(opts, fmt.Sprintf("_journal_mode=%s", cfg.JournalMode))
	}
	if cfg.Synchronous != "" {
		opts = append(opts, fmt.Sprintf("_synchronous=%s", cfg.Synchronous))
	}

	// Enable foreign keys by default
	opts = append(opts, "_foreign_keys=ON")

	if len(opts) > 0 {
		dsn = dsn + "?" + strings.Join(opts, "&")
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open SQLite database: %w", err)
	}

	// Configure connection pool
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}

	// Test connection
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping SQLite database: %w", err)
	}

	return &SQLiteStorage{
		db:           db,
		transactions: make(map[string]*sql.Tx),
		path:         cfg.Path,
	}, nil
}

// NewInMemorySQLiteStorage creates a new in-memory SQLite storage backend.
// This is a convenience function for testing and simple use cases.
func NewInMemorySQLiteStorage() (*SQLiteStorage, error) {
	return NewSQLiteStorage(DefaultSQLiteConfig())
}

// Query executes a query and returns result sets.
func (s *SQLiteStorage) Query(ctx context.Context, sqlStr string, args ...interface{}) ([]runtime.ResultSet, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	return s.scanResultSet(rows)
}

// QueryRow executes a query expecting a single row.
func (s *SQLiteStorage) QueryRow(ctx context.Context, sqlStr string, args ...interface{}) ([]interface{}, error) {
	results, err := s.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}
	return results[0].Rows[0], nil
}

// Exec executes a statement and returns rows affected.
func (s *SQLiteStorage) Exec(ctx context.Context, sqlStr string, args ...interface{}) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	result, err := s.db.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return 0, fmt.Errorf("exec error: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}

	return rowsAffected, nil
}

// Begin starts a transaction.
func (s *SQLiteStorage) Begin(ctx context.Context) (*runtime.TransactionContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	txnCtx := &runtime.TransactionContext{
		ID:           generateTxnID(),
		StartTime:    time.Now(),
		NestingLevel: 1,
		State:        runtime.TxnActive,
	}

	s.transactions[txnCtx.ID] = tx

	return txnCtx, nil
}

// Commit commits a transaction.
func (s *SQLiteStorage) Commit(ctx context.Context, txn *runtime.TransactionContext) error {
	if txn == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.transactions[txn.ID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	delete(s.transactions, txn.ID)
	txn.State = runtime.TxnCommitted

	return nil
}

// Rollback rolls back a transaction.
func (s *SQLiteStorage) Rollback(ctx context.Context, txn *runtime.TransactionContext) error {
	if txn == nil {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.transactions[txn.ID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}

	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	delete(s.transactions, txn.ID)
	txn.State = runtime.TxnRolledBack

	return nil
}

// Savepoint creates a savepoint.
func (s *SQLiteStorage) Savepoint(ctx context.Context, txn *runtime.TransactionContext, name string) error {
	if txn == nil {
		return fmt.Errorf("no transaction active")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.transactions[txn.ID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}

	_, err := tx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", name))
	if err != nil {
		return fmt.Errorf("savepoint failed: %w", err)
	}

	txn.Savepoints = append(txn.Savepoints, name)
	return nil
}

// RollbackTo rolls back to a savepoint.
func (s *SQLiteStorage) RollbackTo(ctx context.Context, txn *runtime.TransactionContext, name string) error {
	if txn == nil {
		return fmt.Errorf("no transaction active")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	tx, ok := s.transactions[txn.ID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}

	_, err := tx.ExecContext(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name))
	if err != nil {
		return fmt.Errorf("rollback to savepoint failed: %w", err)
	}

	// Remove savepoints after the target
	for i, sp := range txn.Savepoints {
		if sp == name {
			txn.Savepoints = txn.Savepoints[:i+1]
			break
		}
	}

	return nil
}

// CreateTempTable creates a temporary table.
func (s *SQLiteStorage) CreateTempTable(ctx context.Context, name string, columns []runtime.ColumnInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Build CREATE TEMP TABLE statement
	var colDefs []string
	for _, col := range columns {
		colDef := fmt.Sprintf("%s %s", col.Name, mapTypeToSQLite(col.Type))
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		colDefs = append(colDefs, colDef)
	}

	sql := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (%s)", name, strings.Join(colDefs, ", "))

	_, err := s.db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("create temp table failed: %w", err)
	}

	return nil
}

// DropTempTable drops a temporary table.
func (s *SQLiteStorage) DropTempTable(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	if err != nil {
		return fmt.Errorf("drop temp table failed: %w", err)
	}

	return nil
}

// TempTableExists checks if a temp table exists.
func (s *SQLiteStorage) TempTableExists(ctx context.Context, name string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var count int
	err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_temp_master WHERE type='table' AND name=?",
		name,
	).Scan(&count)

	return err == nil && count > 0
}

// Dialect returns the storage dialect.
func (s *SQLiteStorage) Dialect() string {
	return "sqlite"
}

// Close closes the storage backend.
func (s *SQLiteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rollback any active transactions
	for id, tx := range s.transactions {
		tx.Rollback()
		delete(s.transactions, id)
	}

	return s.db.Close()
}

// GetDB returns the underlying database connection.
func (s *SQLiteStorage) GetDB() *sql.DB {
	return s.db
}

// GetTx returns the transaction for a given context, if one exists.
func (s *SQLiteStorage) GetTx(txnID string) *sql.Tx {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.transactions[txnID]
}

// scanResultSet scans rows into a ResultSet.
func (s *SQLiteStorage) scanResultSet(rows *sql.Rows) ([]runtime.ResultSet, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	rs := runtime.ResultSet{
		Columns: make([]runtime.ColumnInfo, len(columns)),
	}

	for i, col := range columns {
		rs.Columns[i] = runtime.ColumnInfo{
			Name:    col,
			Type:    mapSQLiteType(colTypes[i].DatabaseTypeName()),
			Ordinal: i,
		}
		if nullable, ok := colTypes[i].Nullable(); ok {
			rs.Columns[i].Nullable = nullable
		}
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		rs.Rows = append(rs.Rows, values)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return []runtime.ResultSet{rs}, nil
}

// mapTypeToSQLite maps a generic SQL type to SQLite type.
func mapTypeToSQLite(sqlType string) string {
	switch strings.ToUpper(sqlType) {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT":
		return "INTEGER"
	case "FLOAT", "REAL", "DOUBLE", "DECIMAL", "NUMERIC":
		return "REAL"
	case "VARCHAR", "NVARCHAR", "CHAR", "NCHAR", "TEXT", "NTEXT":
		return "TEXT"
	case "VARBINARY", "BINARY", "IMAGE":
		return "BLOB"
	case "BIT", "BOOLEAN":
		return "INTEGER"
	case "DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", "TIME":
		return "TEXT" // SQLite stores dates as TEXT
	case "UNIQUEIDENTIFIER":
		return "TEXT"
	default:
		return "TEXT"
	}
}

// mapSQLiteType maps a SQLite type to a generic SQL type name.
func mapSQLiteType(sqliteType string) string {
	switch strings.ToUpper(sqliteType) {
	case "INTEGER":
		return "INT"
	case "REAL":
		return "FLOAT"
	case "TEXT":
		return "NVARCHAR"
	case "BLOB":
		return "VARBINARY"
	case "NUMERIC":
		return "DECIMAL"
	case "":
		// SQLite may return empty type for expressions like SELECT 1
		return "INT"
	default:
		return sqliteType
	}
}

// generateTxnID creates a unique transaction ID.
func generateTxnID() string {
	return fmt.Sprintf("txn_%d", time.Now().UnixNano())
}
