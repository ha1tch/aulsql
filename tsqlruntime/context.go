package tsqlruntime

import (
	"context"
	"database/sql"
	"strings"
	"sync"
)

// ExecutionContext holds all state for a T-SQL execution session
type ExecutionContext struct {
	// Database connection
	DB *sql.DB
	Tx *sql.Tx

	// Dialect for query generation
	Dialect Dialect

	// Variables
	Variables map[string]Value
	varMu     sync.RWMutex

	// Temp tables and table variables
	TempTables *TempTableManager

	// Cursors
	Cursors *CursorManager

	// Error handling
	ErrorHandler *TryCatchHandler

	// System variables
	RowCount     int64
	LastInsertID int64
	FetchStatus  int
	TranCount    int
	Error        int
	NoCount      bool
	XactAbort    bool

	// Execution state
	ReturnValue *Value
	HasReturned bool

	// Result sets
	ResultSets []ResultSet

	// Parent context for nested execution
	Parent *ExecutionContext

	// Debugging
	Debug bool
}

// NewExecutionContext creates a new execution context
func NewExecutionContext(db *sql.DB, dialect Dialect) *ExecutionContext {
	return &ExecutionContext{
		DB:           db,
		Dialect:      dialect,
		Variables:    make(map[string]Value),
		TempTables:   NewTempTableManager(),
		Cursors:      NewCursorManager(),
		ErrorHandler: NewTryCatchHandler(),
		FetchStatus:  -1,
		ResultSets:   make([]ResultSet, 0),
	}
}

// NewChildContext creates a child context for nested execution
func (ec *ExecutionContext) NewChildContext() *ExecutionContext {
	child := &ExecutionContext{
		DB:           ec.DB,
		Tx:           ec.Tx,
		Dialect:      ec.Dialect,
		Variables:    make(map[string]Value),
		TempTables:   ec.TempTables, // Share temp tables
		Cursors:      ec.Cursors,    // Share cursors
		ErrorHandler: ec.ErrorHandler,
		FetchStatus:  -1,
		ResultSets:   make([]ResultSet, 0),
		Parent:       ec,
		Debug:        ec.Debug,
		NoCount:      ec.NoCount,
		XactAbort:    ec.XactAbort,
	}

	// Copy variables to child
	ec.varMu.RLock()
	for k, v := range ec.Variables {
		child.Variables[k] = v.Clone()
	}
	ec.varMu.RUnlock()

	return child
}

// SetVariable sets a variable value
func (ec *ExecutionContext) SetVariable(name string, value Value) {
	ec.varMu.Lock()
	defer ec.varMu.Unlock()

	name = normalizeVarName(name)
	ec.Variables[name] = value

	// Update system variables if needed
	switch name {
	case "@@rowcount":
		ec.RowCount = value.AsInt()
	case "@@identity", "@@scope_identity":
		ec.LastInsertID = value.AsInt()
	case "@@fetch_status":
		ec.FetchStatus = int(value.AsInt())
	case "@@trancount":
		ec.TranCount = int(value.AsInt())
	case "@@error":
		ec.Error = int(value.AsInt())
	}
}

// GetVariable gets a variable value
func (ec *ExecutionContext) GetVariable(name string) (Value, bool) {
	ec.varMu.RLock()
	defer ec.varMu.RUnlock()

	name = normalizeVarName(name)

	// Check for system variables
	switch name {
	case "@@rowcount":
		return NewInt(ec.RowCount), true
	case "@@identity", "@@scope_identity":
		return NewBigInt(ec.LastInsertID), true
	case "@@fetch_status":
		return NewInt(int64(ec.FetchStatus)), true
	case "@@trancount":
		return NewInt(int64(ec.TranCount)), true
	case "@@error":
		return NewInt(int64(ec.Error)), true
	case "@@version":
		return NewVarChar("T-SQL Runtime 1.0 (Stage 2)", -1), true
	case "@@servername":
		return NewVarChar("localhost", -1), true
	case "@@spid":
		return NewInt(1), true
	}

	// Error functions (only valid in CATCH block)
	if ec.ErrorHandler.HasCaughtError() {
		switch name {
		case "error_number":
			return NewInt(int64(ec.ErrorHandler.GetErrorNumber())), true
		case "error_message":
			return NewVarChar(ec.ErrorHandler.GetErrorMessage(), -1), true
		case "error_line":
			return NewInt(int64(ec.ErrorHandler.GetErrorLine())), true
		case "error_procedure":
			proc := ec.ErrorHandler.GetErrorProcedure()
			if proc == "" {
				return Null(TypeVarChar), true
			}
			return NewVarChar(proc, -1), true
		case "error_state":
			return NewInt(int64(ec.ErrorHandler.GetErrorState())), true
		case "error_severity":
			return NewInt(int64(ec.ErrorHandler.GetErrorSeverity())), true
		case "xact_state":
			return NewInt(int64(ec.ErrorHandler.GetXactState())), true
		}
	}

	// User variable
	if v, ok := ec.Variables[name]; ok {
		return v, true
	}

	// Check parent context
	if ec.Parent != nil {
		return ec.Parent.GetVariable(name)
	}

	return Null(TypeUnknown), false
}

// DeclareVariable declares a new variable with a type
func (ec *ExecutionContext) DeclareVariable(name string, dt DataType, precision, scale, maxLen int) {
	ec.varMu.Lock()
	defer ec.varMu.Unlock()

	name = normalizeVarName(name)
	v := Null(dt)
	v.Precision = precision
	v.Scale = scale
	v.MaxLen = maxLen
	ec.Variables[name] = v
}

// UpdateRowCount updates @@ROWCOUNT
func (ec *ExecutionContext) UpdateRowCount(count int64) {
	ec.RowCount = count
	ec.varMu.Lock()
	ec.Variables["@@rowcount"] = NewInt(count)
	ec.varMu.Unlock()
}

// UpdateLastInsertID updates @@IDENTITY
func (ec *ExecutionContext) UpdateLastInsertID(id int64) {
	ec.LastInsertID = id
	ec.varMu.Lock()
	ec.Variables["@@identity"] = NewBigInt(id)
	ec.Variables["@@scope_identity"] = NewBigInt(id)
	ec.varMu.Unlock()
}

// UpdateFetchStatus updates @@FETCH_STATUS
func (ec *ExecutionContext) UpdateFetchStatus(status int) {
	ec.FetchStatus = status
	ec.varMu.Lock()
	ec.Variables["@@fetch_status"] = NewInt(int64(status))
	ec.varMu.Unlock()
}

// UpdateError updates @@ERROR
func (ec *ExecutionContext) UpdateError(errNum int) {
	ec.Error = errNum
	ec.varMu.Lock()
	ec.Variables["@@error"] = NewInt(int64(errNum))
	ec.varMu.Unlock()
}

// BeginTransaction starts a transaction
func (ec *ExecutionContext) BeginTransaction(ctx context.Context) error {
	if ec.Tx != nil {
		// Nested transaction - just increment count
		ec.TranCount++
		return nil
	}

	tx, err := ec.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	ec.Tx = tx
	ec.TranCount = 1
	ec.ErrorHandler.SetXactState(1)
	return nil
}

// CommitTransaction commits the current transaction
func (ec *ExecutionContext) CommitTransaction() error {
	if ec.Tx == nil {
		return NewSQLError(3902, "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION")
	}

	ec.TranCount--
	if ec.TranCount == 0 {
		err := ec.Tx.Commit()
		ec.Tx = nil
		ec.ErrorHandler.SetXactState(0)
		return err
	}
	return nil
}

// RollbackTransaction rolls back the current transaction
func (ec *ExecutionContext) RollbackTransaction() error {
	if ec.Tx == nil {
		return NewSQLError(3903, "The ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION")
	}

	err := ec.Tx.Rollback()
	ec.Tx = nil
	ec.TranCount = 0
	ec.ErrorHandler.SetXactState(0)
	return err
}

// AddResultSet adds a result set to the output
func (ec *ExecutionContext) AddResultSet(rs ResultSet) {
	ec.ResultSets = append(ec.ResultSets, rs)
}

// ClearResultSets clears all result sets
func (ec *ExecutionContext) ClearResultSets() {
	ec.ResultSets = ec.ResultSets[:0]
}

// GetExecutor returns the query executor (Tx if in transaction, DB otherwise)
func (ec *ExecutionContext) GetExecutor() QueryExecutor {
	if ec.Tx != nil {
		return ec.Tx
	}
	return ec.DB
}

// QueryExecutor is an interface for executing queries
type QueryExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

// normalizeVarName normalizes a variable name
func normalizeVarName(name string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	// Keep @@ for system variables, remove single @ for user variables
	if strings.HasPrefix(name, "@@") {
		return name
	}
	return strings.TrimPrefix(name, "@")
}
