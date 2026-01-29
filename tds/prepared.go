package tds

import (
	"context"
	"sync"
	"time"
)

// PreparedStatement holds a parsed statement ready for execution.
type PreparedStatement struct {
	Handle     int32
	SQL        string
	ParamDefs  string       // Parameter definitions: "@p1 int, @p2 nvarchar(100)"
	ParamCount int          // Number of parameters
	Columns    []Column     // Result column metadata (if known from prepare)
	CreatedAt  time.Time
	ExecCount  int64        // Execution count for statistics
}

// PreparedStatementStore manages prepared statements for a connection.
type PreparedStatementStore interface {
	// Prepare parses a statement and returns a handle.
	// paramDefs contains parameter definitions like "@p1 int, @p2 nvarchar(100)".
	// Returns the handle, result column metadata (may be nil), and any error.
	Prepare(ctx context.Context, stmt string, paramDefs string) (handle int32, columns []Column, err error)

	// Execute runs a prepared statement by handle with the given parameters.
	Execute(ctx context.Context, handle int32, params map[string]interface{}) (*ExecuteResult, error)

	// Unprepare releases a prepared statement handle.
	Unprepare(ctx context.Context, handle int32) error

	// GetStatement returns the prepared statement for a handle, if it exists.
	GetStatement(handle int32) (*PreparedStatement, bool)
}

// ExecuteResult holds the result of executing a prepared statement.
type ExecuteResult struct {
	Columns      []Column
	Rows         [][]interface{}
	RowsAffected int64
	HasResultSet bool
}

// HandlePool manages integer handles for prepared statements and cursors.
// Thread-safe, reuses released handles to keep handle values low.
type HandlePool struct {
	mu       sync.Mutex
	next     int32
	released []int32
}

// NewHandlePool creates a new handle pool starting from 1.
func NewHandlePool() *HandlePool {
	return &HandlePool{
		next:     1,
		released: make([]int32, 0, 16),
	}
}

// Acquire returns the next available handle.
func (p *HandlePool) Acquire() int32 {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Reuse released handle if available
	if len(p.released) > 0 {
		handle := p.released[len(p.released)-1]
		p.released = p.released[:len(p.released)-1]
		return handle
	}

	// Allocate new handle
	handle := p.next
	p.next++
	return handle
}

// Release returns a handle to the pool for reuse.
func (p *HandlePool) Release(handle int32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.released = append(p.released, handle)
}

// PreparedStatementCache is an in-memory cache of prepared statements.
// This is the default implementation used by connections.
type PreparedStatementCache struct {
	mu         sync.RWMutex
	statements map[int32]*PreparedStatement
	handlePool *HandlePool
	executor   PreparedStatementExecutor
}

// PreparedStatementExecutor is called to actually execute prepared statements.
// This is implemented by the application layer (aul).
type PreparedStatementExecutor interface {
	// ParseAndValidate parses the SQL and returns column metadata.
	// This is called during sp_prepare.
	ParseAndValidate(ctx context.Context, sql string, paramDefs string) (columns []Column, err error)

	// ExecutePrepared executes a previously parsed statement.
	ExecutePrepared(ctx context.Context, stmt *PreparedStatement, params map[string]interface{}) (*ExecuteResult, error)
}

// NewPreparedStatementCache creates a new cache with the given executor.
func NewPreparedStatementCache(executor PreparedStatementExecutor) *PreparedStatementCache {
	return &PreparedStatementCache{
		statements: make(map[int32]*PreparedStatement),
		handlePool: NewHandlePool(),
		executor:   executor,
	}
}

// Prepare implements PreparedStatementStore.
func (c *PreparedStatementCache) Prepare(ctx context.Context, stmt string, paramDefs string) (int32, []Column, error) {
	// Validate with executor if available
	var columns []Column
	var err error
	if c.executor != nil {
		columns, err = c.executor.ParseAndValidate(ctx, stmt, paramDefs)
		if err != nil {
			return 0, nil, err
		}
	}

	// Allocate handle and store
	handle := c.handlePool.Acquire()
	ps := &PreparedStatement{
		Handle:     handle,
		SQL:        stmt,
		ParamDefs:  paramDefs,
		ParamCount: countParams(paramDefs),
		Columns:    columns,
		CreatedAt:  time.Now(),
	}

	c.mu.Lock()
	c.statements[handle] = ps
	c.mu.Unlock()

	return handle, columns, nil
}

// Execute implements PreparedStatementStore.
func (c *PreparedStatementCache) Execute(ctx context.Context, handle int32, params map[string]interface{}) (*ExecuteResult, error) {
	c.mu.RLock()
	ps, ok := c.statements[handle]
	c.mu.RUnlock()

	if !ok {
		return nil, &PreparedStatementError{Handle: handle, Message: "invalid prepared statement handle"}
	}

	// Update stats
	c.mu.Lock()
	ps.ExecCount++
	c.mu.Unlock()

	// Execute via executor
	if c.executor == nil {
		return nil, ErrNotImplemented("prepared statement execution")
	}

	return c.executor.ExecutePrepared(ctx, ps, params)
}

// Unprepare implements PreparedStatementStore.
func (c *PreparedStatementCache) Unprepare(ctx context.Context, handle int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.statements[handle]; !ok {
		return &PreparedStatementError{Handle: handle, Message: "invalid prepared statement handle"}
	}

	delete(c.statements, handle)
	c.handlePool.Release(handle)
	return nil
}

// GetStatement implements PreparedStatementStore.
func (c *PreparedStatementCache) GetStatement(handle int32) (*PreparedStatement, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ps, ok := c.statements[handle]
	return ps, ok
}

// countParams counts the number of parameters in a parameter definition string.
func countParams(paramDefs string) int {
	if paramDefs == "" {
		return 0
	}
	count := 1
	for _, c := range paramDefs {
		if c == ',' {
			count++
		}
	}
	return count
}

// PreparedStatementError indicates an error with a prepared statement.
type PreparedStatementError struct {
	Handle  int32
	Message string
}

func (e *PreparedStatementError) Error() string {
	return e.Message
}

// NullPreparedStatementStore is a stub that rejects all prepared statement operations.
type NullPreparedStatementStore struct{}

func (NullPreparedStatementStore) Prepare(ctx context.Context, stmt string, paramDefs string) (int32, []Column, error) {
	return 0, nil, ErrNotImplemented("prepared statements")
}

func (NullPreparedStatementStore) Execute(ctx context.Context, handle int32, params map[string]interface{}) (*ExecuteResult, error) {
	return nil, ErrNotImplemented("prepared statements")
}

func (NullPreparedStatementStore) Unprepare(ctx context.Context, handle int32) error {
	return ErrNotImplemented("prepared statements")
}

func (NullPreparedStatementStore) GetStatement(handle int32) (*PreparedStatement, bool) {
	return nil, false
}
