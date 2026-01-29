package runtime

import (
	"context"
	"database/sql"
	"sync"
)

// StorageBackend provides data access for procedure execution.
type StorageBackend interface {
	// Query operations
	Query(ctx context.Context, sql string, args ...interface{}) ([]ResultSet, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) ([]interface{}, error)
	Exec(ctx context.Context, sql string, args ...interface{}) (int64, error)

	// Transaction support
	Begin(ctx context.Context) (*TransactionContext, error)
	Commit(ctx context.Context, txn *TransactionContext) error
	Rollback(ctx context.Context, txn *TransactionContext) error
	Savepoint(ctx context.Context, txn *TransactionContext, name string) error
	RollbackTo(ctx context.Context, txn *TransactionContext, name string) error

	// Temp table support
	CreateTempTable(ctx context.Context, name string, columns []ColumnInfo) error
	DropTempTable(ctx context.Context, name string) error
	TempTableExists(ctx context.Context, name string) bool

	// Metadata
	Dialect() string
	Close() error

	// Database access for tsqlruntime integration
	GetDB() *sql.DB
}

// TenantAwareStorageBackend extends StorageBackend with tenant-specific operations.
// Use this interface when multi-tenancy is enabled.
type TenantAwareStorageBackend interface {
	StorageBackend

	// GetDBForTenant returns a database connection for the specified tenant and database.
	// This allows global procedures to access the calling tenant's data.
	GetDBForTenant(tenant, database string) (*sql.DB, error)

	// QueryForTenant executes a query for a specific tenant.
	QueryForTenant(ctx context.Context, tenant, database, sql string, args ...interface{}) ([]ResultSet, error)

	// ExecForTenant executes a statement for a specific tenant.
	ExecForTenant(ctx context.Context, tenant, database, sql string, args ...interface{}) (int64, error)

	// BeginForTenant starts a transaction for a specific tenant.
	BeginForTenant(ctx context.Context, tenant, database string) (*TransactionContext, error)
}

// StorageConfig holds storage backend configuration.
type StorageConfig struct {
	// Backend type: memory, postgres, mysql, sqlserver
	Type string

	// Connection settings
	Host     string
	Port     int
	Database string
	Username string
	Password string

	// Connection pool
	MaxOpenConns int
	MaxIdleConns int

	// Options
	Options map[string]string
}

// DefaultStorageConfig returns sensible defaults.
func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		Type:         "memory",
		MaxOpenConns: 25,
		MaxIdleConns: 5,
		Options:      make(map[string]string),
	}
}

// MemoryStorage provides an in-memory storage backend for testing.
type MemoryStorage struct {
	mu sync.RWMutex

	// Tables (name -> rows)
	tables map[string]*memTable

	// Temp tables (session-scoped)
	tempTables map[string]*memTable

	// Active transactions
	transactions map[string]*memTransaction
}

type memTable struct {
	columns []ColumnInfo
	rows    [][]interface{}
}

type memTransaction struct {
	ctx      *TransactionContext
	snapshot map[string]*memTable // Snapshot for rollback
}

// NewMemoryStorage creates a new in-memory storage backend.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		tables:       make(map[string]*memTable),
		tempTables:   make(map[string]*memTable),
		transactions: make(map[string]*memTransaction),
	}
}

// Query executes a query and returns result sets.
func (m *MemoryStorage) Query(ctx context.Context, sql string, args ...interface{}) ([]ResultSet, error) {
	// TODO: Implement SQL parsing and execution
	// For now, return empty result
	return []ResultSet{}, nil
}

// QueryRow executes a query expecting a single row.
func (m *MemoryStorage) QueryRow(ctx context.Context, sql string, args ...interface{}) ([]interface{}, error) {
	results, err := m.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 || len(results[0].Rows) == 0 {
		return nil, nil
	}
	return results[0].Rows[0], nil
}

// Exec executes a statement and returns rows affected.
func (m *MemoryStorage) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	// TODO: Implement SQL parsing and execution
	return 0, nil
}

// Begin starts a transaction.
func (m *MemoryStorage) Begin(ctx context.Context) (*TransactionContext, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	txn := &TransactionContext{
		ID:           generateID(),
		NestingLevel: 1,
		State:        TxnActive,
	}

	// Create snapshot for rollback
	snapshot := make(map[string]*memTable)
	for name, table := range m.tables {
		snapshot[name] = m.cloneTable(table)
	}

	m.transactions[txn.ID] = &memTransaction{
		ctx:      txn,
		snapshot: snapshot,
	}

	return txn, nil
}

// Commit commits a transaction.
func (m *MemoryStorage) Commit(ctx context.Context, txn *TransactionContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txn == nil {
		return nil
	}

	delete(m.transactions, txn.ID)
	txn.State = TxnCommitted
	return nil
}

// Rollback rolls back a transaction.
func (m *MemoryStorage) Rollback(ctx context.Context, txn *TransactionContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if txn == nil {
		return nil
	}

	memTxn, ok := m.transactions[txn.ID]
	if !ok {
		return nil
	}

	// Restore snapshot
	m.tables = memTxn.snapshot

	delete(m.transactions, txn.ID)
	txn.State = TxnRolledBack
	return nil
}

// Savepoint creates a savepoint.
func (m *MemoryStorage) Savepoint(ctx context.Context, txn *TransactionContext, name string) error {
	if txn == nil {
		return nil
	}
	txn.Savepoints = append(txn.Savepoints, name)
	return nil
}

// RollbackTo rolls back to a savepoint.
func (m *MemoryStorage) RollbackTo(ctx context.Context, txn *TransactionContext, name string) error {
	// TODO: Implement savepoint rollback
	return nil
}

// CreateTempTable creates a temporary table.
func (m *MemoryStorage) CreateTempTable(ctx context.Context, name string, columns []ColumnInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.tempTables[name] = &memTable{
		columns: columns,
		rows:    make([][]interface{}, 0),
	}
	return nil
}

// DropTempTable drops a temporary table.
func (m *MemoryStorage) DropTempTable(ctx context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.tempTables, name)
	return nil
}

// TempTableExists checks if a temp table exists.
func (m *MemoryStorage) TempTableExists(ctx context.Context, name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.tempTables[name]
	return ok
}

// Dialect returns the storage dialect.
func (m *MemoryStorage) Dialect() string {
	return "memory"
}

// Close closes the storage backend.
func (m *MemoryStorage) Close() error {
	return nil
}

// GetDB returns the underlying database connection.
// For MemoryStorage, this returns nil as there is no actual database.
func (m *MemoryStorage) GetDB() *sql.DB {
	return nil
}

// cloneTable creates a deep copy of a table.
func (m *MemoryStorage) cloneTable(t *memTable) *memTable {
	clone := &memTable{
		columns: make([]ColumnInfo, len(t.columns)),
		rows:    make([][]interface{}, len(t.rows)),
	}
	copy(clone.columns, t.columns)
	for i, row := range t.rows {
		clone.rows[i] = make([]interface{}, len(row))
		copy(clone.rows[i], row)
	}
	return clone
}

// generateID generates a unique ID.
func generateID() string {
	// Simple implementation - production would use UUID
	return "id_" + randomString(16)
}

func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[i%len(letters)]
	}
	return string(b)
}
