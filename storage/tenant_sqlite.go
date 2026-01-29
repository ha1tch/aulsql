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
	"time"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ha1tch/aul/runtime"
)

// TenantSQLiteStorage provides a multi-tenant SQLite storage backend.
// Each tenant+database combination gets its own SQLite file.
//
// Directory structure:
//
//	{BaseDir}/
//	├── _default/           # Default tenant (single-tenant mode)
//	│   ├── master.db
//	│   └── salesdb.db
//	└── {tenant}/           # Tenant-specific databases
//	    ├── master.db
//	    └── salesdb.db
type TenantSQLiteStorage struct {
	mu sync.RWMutex

	// Configuration
	config TenantSQLiteConfig

	// Connection pools per tenant+database
	// Key format: "{tenant}:{database}" or "_default:{database}"
	pools map[string]*sql.DB

	// Active transactions per pool
	// Key format: "{txnID}"
	transactions map[string]*tenantTxn
}

// tenantTxn tracks a transaction and its associated pool.
type tenantTxn struct {
	tx       *sql.Tx
	poolKey  string
	tenantID string
	database string
}

// TenantSQLiteConfig holds configuration for tenant SQLite storage.
type TenantSQLiteConfig struct {
	// BaseDir is the root directory for all tenant databases.
	BaseDir string

	// AutoCreate controls whether databases are auto-created on first access.
	AutoCreate bool

	// DefaultTenant is used when no tenant is specified (single-tenant mode).
	DefaultTenant string

	// SQLite-specific options (applied to all databases)
	JournalMode string // WAL, DELETE, TRUNCATE, PERSIST, MEMORY, OFF
	Synchronous string // OFF, NORMAL, FULL, EXTRA
	CacheSize   int    // Number of pages (negative = KB)
	BusyTimeout int    // Milliseconds

	// Connection pool settings per database
	MaxOpenConns int
	MaxIdleConns int
}

// DefaultTenantSQLiteConfig returns sensible defaults.
func DefaultTenantSQLiteConfig() TenantSQLiteConfig {
	return TenantSQLiteConfig{
		BaseDir:       "./data/tenants",
		AutoCreate:    true,
		DefaultTenant: "_default",
		JournalMode:   "WAL",
		Synchronous:   "NORMAL",
		CacheSize:     -2000, // 2MB
		BusyTimeout:   5000,  // 5 seconds
		MaxOpenConns:  5,
		MaxIdleConns:  2,
	}
}

// NewTenantSQLiteStorage creates a new tenant-aware SQLite storage backend.
func NewTenantSQLiteStorage(cfg TenantSQLiteConfig) (*TenantSQLiteStorage, error) {
	// Ensure base directory exists
	if err := os.MkdirAll(cfg.BaseDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	return &TenantSQLiteStorage{
		config:       cfg,
		pools:        make(map[string]*sql.DB),
		transactions: make(map[string]*tenantTxn),
	}, nil
}

// resolveDatabasePath returns the file path for a tenant+database combination.
func (s *TenantSQLiteStorage) resolveDatabasePath(database, tenant string) string {
	if tenant == "" {
		tenant = s.config.DefaultTenant
	}

	// Sanitise names to prevent path traversal
	tenant = sanitisePathComponent(tenant)
	database = sanitisePathComponent(database)

	return filepath.Join(s.config.BaseDir, tenant, database+".db")
}

// sanitisePathComponent removes potentially dangerous characters from path components.
func sanitisePathComponent(name string) string {
	// Replace path separators and other dangerous characters
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.ReplaceAll(name, "\\", "_")
	name = strings.ReplaceAll(name, "..", "_")
	name = strings.TrimSpace(name)

	if name == "" {
		name = "_empty"
	}

	return name
}

// poolKey returns the key for a tenant+database pool.
func poolKey(tenant, database string) string {
	if tenant == "" {
		tenant = "_default"
	}
	return tenant + ":" + database
}

// getPool returns (or creates) a connection pool for the tenant+database.
func (s *TenantSQLiteStorage) getPool(ctx context.Context, tenant, database string) (*sql.DB, error) {
	key := poolKey(tenant, database)

	// Fast path: check if pool exists
	s.mu.RLock()
	if pool, ok := s.pools[key]; ok {
		s.mu.RUnlock()
		return pool, nil
	}
	s.mu.RUnlock()

	// Slow path: create pool
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after acquiring write lock
	if pool, ok := s.pools[key]; ok {
		return pool, nil
	}

	// Resolve database path
	dbPath := s.resolveDatabasePath(database, tenant)

	// Check if database exists
	exists := fileExists(dbPath)

	if !exists && !s.config.AutoCreate {
		return nil, fmt.Errorf("database not found and auto-create disabled: %s", dbPath)
	}

	// Ensure tenant directory exists
	tenantDir := filepath.Dir(dbPath)
	if err := os.MkdirAll(tenantDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create tenant directory: %w", err)
	}

	// Build DSN with options
	dsn := s.buildDSN(dbPath)

	// Open database
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	if s.config.MaxOpenConns > 0 {
		db.SetMaxOpenConns(s.config.MaxOpenConns)
	}
	if s.config.MaxIdleConns > 0 {
		db.SetMaxIdleConns(s.config.MaxIdleConns)
	}

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Store pool
	s.pools[key] = db

	return db, nil
}

// buildDSN builds a SQLite DSN with configured options.
func (s *TenantSQLiteStorage) buildDSN(dbPath string) string {
	dsn := dbPath
	opts := []string{}

	if s.config.CacheSize != 0 {
		opts = append(opts, fmt.Sprintf("_cache_size=%d", s.config.CacheSize))
	}
	if s.config.BusyTimeout > 0 {
		opts = append(opts, fmt.Sprintf("_busy_timeout=%d", s.config.BusyTimeout))
	}
	if s.config.JournalMode != "" {
		opts = append(opts, fmt.Sprintf("_journal_mode=%s", s.config.JournalMode))
	}
	if s.config.Synchronous != "" {
		opts = append(opts, fmt.Sprintf("_synchronous=%s", s.config.Synchronous))
	}

	// Enable foreign keys by default
	opts = append(opts, "_foreign_keys=ON")

	if len(opts) > 0 {
		dsn = dsn + "?" + strings.Join(opts, "&")
	}

	return dsn
}

// fileExists checks if a file exists.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// QueryForTenant executes a query for a specific tenant.
func (s *TenantSQLiteStorage) QueryForTenant(ctx context.Context, tenant, database, sqlStr string, args ...interface{}) ([]runtime.ResultSet, error) {
	pool, err := s.getPool(ctx, tenant, database)
	if err != nil {
		return nil, err
	}

	rows, err := pool.QueryContext(ctx, sqlStr, args...)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	return scanResultSet(rows)
}

// ExecForTenant executes a statement for a specific tenant.
func (s *TenantSQLiteStorage) ExecForTenant(ctx context.Context, tenant, database, sqlStr string, args ...interface{}) (int64, error) {
	pool, err := s.getPool(ctx, tenant, database)
	if err != nil {
		return 0, err
	}

	result, err := pool.ExecContext(ctx, sqlStr, args...)
	if err != nil {
		return 0, fmt.Errorf("exec error: %w", err)
	}

	return result.RowsAffected()
}

// BeginForTenant starts a transaction for a specific tenant.
func (s *TenantSQLiteStorage) BeginForTenant(ctx context.Context, tenant, database string) (*runtime.TransactionContext, error) {
	pool, err := s.getPool(ctx, tenant, database)
	if err != nil {
		return nil, err
	}

	tx, err := pool.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	txnCtx := &runtime.TransactionContext{
		ID:           generateTenantTxnID(),
		StartTime:    time.Now(),
		NestingLevel: 1,
		State:        runtime.TxnActive,
	}

	s.mu.Lock()
	s.transactions[txnCtx.ID] = &tenantTxn{
		tx:       tx,
		poolKey:  poolKey(tenant, database),
		tenantID: tenant,
		database: database,
	}
	s.mu.Unlock()

	return txnCtx, nil
}

// Commit commits a transaction.
func (s *TenantSQLiteStorage) Commit(ctx context.Context, txn *runtime.TransactionContext) error {
	if txn == nil {
		return nil
	}

	s.mu.Lock()
	ttxn, ok := s.transactions[txn.ID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}
	delete(s.transactions, txn.ID)
	s.mu.Unlock()

	if err := ttxn.tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	txn.State = runtime.TxnCommitted
	return nil
}

// Rollback rolls back a transaction.
func (s *TenantSQLiteStorage) Rollback(ctx context.Context, txn *runtime.TransactionContext) error {
	if txn == nil {
		return nil
	}

	s.mu.Lock()
	ttxn, ok := s.transactions[txn.ID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}
	delete(s.transactions, txn.ID)
	s.mu.Unlock()

	if err := ttxn.tx.Rollback(); err != nil {
		return fmt.Errorf("rollback failed: %w", err)
	}

	txn.State = runtime.TxnRolledBack
	return nil
}

// DatabaseExists checks if a tenant database exists.
func (s *TenantSQLiteStorage) DatabaseExists(tenant, database string) bool {
	path := s.resolveDatabasePath(database, tenant)
	return fileExists(path)
}

// ListTenants returns all tenants with databases.
func (s *TenantSQLiteStorage) ListTenants() ([]string, error) {
	entries, err := os.ReadDir(s.config.BaseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var tenants []string
	for _, entry := range entries {
		if entry.IsDir() && !strings.HasPrefix(entry.Name(), ".") {
			tenants = append(tenants, entry.Name())
		}
	}

	return tenants, nil
}

// ListDatabases returns all databases for a tenant.
func (s *TenantSQLiteStorage) ListDatabases(tenant string) ([]string, error) {
	if tenant == "" {
		tenant = s.config.DefaultTenant
	}

	tenantDir := filepath.Join(s.config.BaseDir, sanitisePathComponent(tenant))
	entries, err := os.ReadDir(tenantDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var databases []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".db") {
			dbName := strings.TrimSuffix(entry.Name(), ".db")
			databases = append(databases, dbName)
		}
	}

	return databases, nil
}

// Close closes all connection pools.
func (s *TenantSQLiteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Rollback any active transactions
	for id, ttxn := range s.transactions {
		ttxn.tx.Rollback()
		delete(s.transactions, id)
	}

	// Close all pools
	var errs []error
	for key, pool := range s.pools {
		if err := pool.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close pool %s: %w", key, err))
		}
		delete(s.pools, key)
	}

	if len(errs) > 0 {
		return fmt.Errorf("errors closing pools: %v", errs)
	}

	return nil
}

// Dialect returns the storage dialect.
func (s *TenantSQLiteStorage) Dialect() string {
	return "sqlite"
}

// GetDB returns the default database connection.
// For multi-tenant storage, this returns the default tenant's master database.
// Prefer GetDBForTenant() for explicit tenant control.
func (s *TenantSQLiteStorage) GetDB() *sql.DB {
	ctx := context.Background()
	db, err := s.getPool(ctx, s.config.DefaultTenant, "master")
	if err != nil {
		return nil
	}
	return db
}

// GetDBForTenant returns a database connection for the specified tenant and database.
// This allows global procedures to access the calling tenant's data.
func (s *TenantSQLiteStorage) GetDBForTenant(tenant, database string) (*sql.DB, error) {
	return s.getPool(context.Background(), tenant, database)
}

// Query executes a query using the default tenant.
func (s *TenantSQLiteStorage) Query(ctx context.Context, sqlStr string, args ...interface{}) ([]runtime.ResultSet, error) {
	return s.QueryForTenant(ctx, s.config.DefaultTenant, "master", sqlStr, args...)
}

// QueryRow executes a query expecting a single row using the default tenant.
func (s *TenantSQLiteStorage) QueryRow(ctx context.Context, sqlStr string, args ...interface{}) ([]interface{}, error) {
	results, err := s.Query(ctx, sqlStr, args...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}
	return results[0].Rows[0], nil
}

// Exec executes a statement using the default tenant.
func (s *TenantSQLiteStorage) Exec(ctx context.Context, sqlStr string, args ...interface{}) (int64, error) {
	return s.ExecForTenant(ctx, s.config.DefaultTenant, "master", sqlStr, args...)
}

// Begin starts a transaction using the default tenant.
func (s *TenantSQLiteStorage) Begin(ctx context.Context) (*runtime.TransactionContext, error) {
	return s.BeginForTenant(ctx, s.config.DefaultTenant, "master")
}

// Savepoint creates a savepoint within a transaction.
func (s *TenantSQLiteStorage) Savepoint(ctx context.Context, txn *runtime.TransactionContext, name string) error {
	if txn == nil {
		return fmt.Errorf("no transaction active")
	}

	s.mu.RLock()
	ttxn, ok := s.transactions[txn.ID]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}

	_, err := ttxn.tx.ExecContext(ctx, fmt.Sprintf("SAVEPOINT %s", name))
	if err != nil {
		return fmt.Errorf("savepoint failed: %w", err)
	}

	txn.Savepoints = append(txn.Savepoints, name)
	return nil
}

// RollbackTo rolls back to a savepoint.
func (s *TenantSQLiteStorage) RollbackTo(ctx context.Context, txn *runtime.TransactionContext, name string) error {
	if txn == nil {
		return fmt.Errorf("no transaction active")
	}

	s.mu.RLock()
	ttxn, ok := s.transactions[txn.ID]
	s.mu.RUnlock()

	if !ok {
		return fmt.Errorf("transaction not found: %s", txn.ID)
	}

	_, err := ttxn.tx.ExecContext(ctx, fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", name))
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

// CreateTempTable creates a temporary table for a tenant.
func (s *TenantSQLiteStorage) CreateTempTable(ctx context.Context, name string, columns []runtime.ColumnInfo) error {
	return s.CreateTempTableForTenant(ctx, s.config.DefaultTenant, "master", name, columns)
}

// CreateTempTableForTenant creates a temporary table for a specific tenant.
func (s *TenantSQLiteStorage) CreateTempTableForTenant(ctx context.Context, tenant, database, name string, columns []runtime.ColumnInfo) error {
	pool, err := s.getPool(ctx, tenant, database)
	if err != nil {
		return err
	}

	var colDefs []string
	for _, col := range columns {
		colDef := fmt.Sprintf("%s %s", col.Name, mapGenericTypeToSQLite(col.Type))
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		colDefs = append(colDefs, colDef)
	}

	sqlStr := fmt.Sprintf("CREATE TEMP TABLE IF NOT EXISTS %s (%s)", name, strings.Join(colDefs, ", "))
	_, err = pool.ExecContext(ctx, sqlStr)
	return err
}

// DropTempTable drops a temporary table.
func (s *TenantSQLiteStorage) DropTempTable(ctx context.Context, name string) error {
	return s.DropTempTableForTenant(ctx, s.config.DefaultTenant, "master", name)
}

// DropTempTableForTenant drops a temporary table for a specific tenant.
func (s *TenantSQLiteStorage) DropTempTableForTenant(ctx context.Context, tenant, database, name string) error {
	pool, err := s.getPool(ctx, tenant, database)
	if err != nil {
		return err
	}

	_, err = pool.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
	return err
}

// TempTableExists checks if a temp table exists.
func (s *TenantSQLiteStorage) TempTableExists(ctx context.Context, name string) bool {
	return s.TempTableExistsForTenant(ctx, s.config.DefaultTenant, "master", name)
}

// TempTableExistsForTenant checks if a temp table exists for a specific tenant.
func (s *TenantSQLiteStorage) TempTableExistsForTenant(ctx context.Context, tenant, database, name string) bool {
	pool, err := s.getPool(ctx, tenant, database)
	if err != nil {
		return false
	}

	var count int
	err = pool.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_temp_master WHERE type='table' AND name=?",
		name,
	).Scan(&count)

	return err == nil && count > 0
}

// mapGenericTypeToSQLite maps a generic SQL type to SQLite type.
func mapGenericTypeToSQLite(sqlType string) string {
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
		return "TEXT"
	case "UNIQUEIDENTIFIER":
		return "TEXT"
	default:
		return "TEXT"
	}
}

// Stats returns storage statistics.
func (s *TenantSQLiteStorage) Stats() TenantStorageStats {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := TenantStorageStats{
		PoolCount:       len(s.pools),
		ActiveTxnCount:  len(s.transactions),
		PoolsPerTenant:  make(map[string]int),
	}

	for key := range s.pools {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) == 2 {
			stats.PoolsPerTenant[parts[0]]++
		}
	}

	return stats
}

// TenantStorageStats holds statistics for tenant storage.
type TenantStorageStats struct {
	PoolCount      int
	ActiveTxnCount int
	PoolsPerTenant map[string]int
}

// generateTenantTxnID creates a unique transaction ID.
func generateTenantTxnID() string {
	return fmt.Sprintf("ttxn_%d", time.Now().UnixNano())
}

// scanResultSet scans rows into a ResultSet.
func scanResultSet(rows *sql.Rows) ([]runtime.ResultSet, error) {
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
			Type:    mapSQLiteTypeToGeneric(colTypes[i].DatabaseTypeName()),
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

// mapSQLiteTypeToGeneric maps a SQLite type to a generic SQL type name.
func mapSQLiteTypeToGeneric(sqliteType string) string {
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
		return "INT"
	default:
		return sqliteType
	}
}
