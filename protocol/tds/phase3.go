package tds

import (
	"context"

	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/tds"
)

// Phase3Handlers groups the handler interfaces for Phase 3 features.
// The application (aul) provides implementations; defaults are null stubs.
type Phase3Handlers struct {
	Transactions tds.TransactionManager
	Prepared     tds.PreparedStatementStore
	Cursors      tds.CursorManager
}

// DefaultPhase3Handlers returns handlers that reject all Phase 3 operations
// with "not implemented" errors.
func DefaultPhase3Handlers() *Phase3Handlers {
	return &Phase3Handlers{
		Transactions: tds.NullTransactionManager{},
		Prepared:     tds.NullPreparedStatementStore{},
		Cursors:      tds.NullCursorManager{},
	}
}

// ConnectionPhase3State holds Phase 3 state for a TDS connection.
type ConnectionPhase3State struct {
	// Transaction state
	ActiveTransaction         *tds.TransactionDescriptor
	TransactionName           string
	TransactionNestingLevel   int // For nested BEGIN TRAN
	IsolationLevel            tds.IsolationLevel

	// Prepared statement cache (handle -> statement)
	PreparedStatements map[int32]*tds.PreparedStatement
	stmtHandlePool     *tds.HandlePool

	// Cursor registry (handle -> cursor)
	Cursors          map[int32]*tds.Cursor
	cursorHandlePool *tds.HandlePool
}

// NewConnectionPhase3State creates new Phase 3 state for a connection.
func NewConnectionPhase3State() *ConnectionPhase3State {
	return &ConnectionPhase3State{
		IsolationLevel:     tds.IsolationReadCommitted, // Default
		PreparedStatements: make(map[int32]*tds.PreparedStatement),
		stmtHandlePool:     tds.NewHandlePool(),
		Cursors:            make(map[int32]*tds.Cursor),
		cursorHandlePool:   tds.NewHandlePool(),
	}
}

// InTransaction returns true if a transaction is active.
func (s *ConnectionPhase3State) InTransaction() bool {
	return s.ActiveTransaction != nil && !s.ActiveTransaction.IsZero()
}

// Phase3RequestType identifies Phase 3 specific request types.
type Phase3RequestType int

const (
	Phase3None Phase3RequestType = iota

	// Transaction requests
	Phase3BeginTran
	Phase3CommitTran
	Phase3RollbackTran
	Phase3Savepoint

	// Prepared statement requests
	Phase3Prepare
	Phase3Execute
	Phase3Unprepare

	// Cursor requests
	Phase3CursorOpen
	Phase3CursorFetch
	Phase3CursorClose
	Phase3CursorOption
)

// Phase3Request holds parsed Phase 3 request data.
type Phase3Request struct {
	Type Phase3RequestType

	// Transaction fields
	TransactionName string
	IsolationLevel  tds.IsolationLevel
	SavepointName   string

	// Prepared statement fields
	PrepareHandle int32
	SQL           string
	ParamDefs     string
	Parameters    map[string]interface{}

	// Cursor fields
	CursorHandle int32
	ScrollOpt    tds.CursorScrollOpt
	CCOpt        tds.CursorConcurrencyOpt
	FetchType    tds.CursorFetchType
	FetchRowNum  int64
	FetchNRows   int32
}

// classifyRPCRequest determines if an RPC request is a Phase 3 operation.
// Returns the Phase 3 request type or Phase3None if not a Phase 3 operation.
func classifyRPCRequest(rpcReq *tds.RPCRequest) Phase3RequestType {
	switch rpcReq.ProcID {
	// Prepared statements
	case tds.ProcIDPrepare:
		return Phase3Prepare
	case tds.ProcIDExecute:
		return Phase3Execute
	case tds.ProcIDUnprepare:
		return Phase3Unprepare

	// Cursors
	case tds.ProcIDCursorOpen:
		return Phase3CursorOpen
	case tds.ProcIDCursorFetch:
		return Phase3CursorFetch
	case tds.ProcIDCursorClose:
		return Phase3CursorClose
	case tds.ProcIDCursorOption:
		return Phase3CursorOption

	default:
		return Phase3None
	}
}

// handlePhase3Request processes a Phase 3 RPC request.
// Returns (handled, result, error). If handled is false, the request
// should be processed by the normal RPC handler.
func (c *Connection) handlePhase3Request(ctx context.Context, rpcReq *tds.RPCRequest) (bool, protocol.Result, error) {
	reqType := classifyRPCRequest(rpcReq)
	if reqType == Phase3None {
		return false, protocol.Result{}, nil
	}

	var result protocol.Result
	var err error

	switch reqType {
	case Phase3Prepare:
		result, err = c.handlePrepare(ctx, rpcReq)
	case Phase3Execute:
		result, err = c.handleExecutePrepared(ctx, rpcReq)
	case Phase3Unprepare:
		result, err = c.handleUnprepare(ctx, rpcReq)
	case Phase3CursorOpen:
		result, err = c.handleCursorOpen(ctx, rpcReq)
	case Phase3CursorFetch:
		result, err = c.handleCursorFetch(ctx, rpcReq)
	case Phase3CursorClose:
		result, err = c.handleCursorClose(ctx, rpcReq)
	case Phase3CursorOption:
		result, err = c.handleCursorOption(ctx, rpcReq)
	default:
		return false, protocol.Result{}, nil
	}

	return true, result, err
}

// Prepared statement handlers

func (c *Connection) handlePrepare(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	// Extract parameters: @handle OUTPUT, @params, @stmt
	// Parameter order: handle (output), params (nvarchar), stmt (nvarchar)

	if len(rpcReq.Parameters) < 3 {
		return errorResult("sp_prepare requires at least 3 parameters"), nil
	}

	// Get SQL statement (typically 3rd parameter)
	var stmt, paramDefs string
	for _, p := range rpcReq.Parameters {
		if p.Name == "stmt" || p.Name == "statement" {
			if s, ok := p.Value.(string); ok {
				stmt = s
			}
		} else if p.Name == "params" {
			if s, ok := p.Value.(string); ok {
				paramDefs = s
			}
		}
	}

	// Fallback to positional parameters
	if stmt == "" && len(rpcReq.Parameters) >= 3 {
		if s, ok := rpcReq.Parameters[2].Value.(string); ok {
			stmt = s
		}
		if s, ok := rpcReq.Parameters[1].Value.(string); ok {
			paramDefs = s
		}
	}

	if stmt == "" {
		return errorResult("sp_prepare: statement is required"), nil
	}

	// Call the prepared statement store
	handle, columns, err := c.phase3.Prepared.Prepare(ctx, stmt, paramDefs)
	if err != nil {
		return errorResult("sp_prepare failed: " + err.Error()), nil
	}

	// Return the handle as an output parameter
	return protocol.Result{
		Type: protocol.ResultOK,
		OutputParams: map[string]interface{}{
			"handle": handle,
		},
		ResultSets: columnsToResultSets(columns),
	}, nil
}

func (c *Connection) handleExecutePrepared(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	// Extract handle (first parameter)
	if len(rpcReq.Parameters) < 1 {
		return errorResult("sp_execute requires handle parameter"), nil
	}

	var handle int32
	if h, ok := rpcReq.Parameters[0].Value.(int64); ok {
		handle = int32(h)
	} else if h, ok := rpcReq.Parameters[0].Value.(int32); ok {
		handle = h
	} else {
		return errorResult("sp_execute: invalid handle type"), nil
	}

	// Collect remaining parameters
	params := make(map[string]interface{})
	for i := 1; i < len(rpcReq.Parameters); i++ {
		p := rpcReq.Parameters[i]
		name := p.Name
		if name == "" {
			name = string(rune('1' + i - 1)) // p1, p2, etc.
		}
		params[name] = p.Value
	}

	// Execute
	result, err := c.phase3.Prepared.Execute(ctx, handle, params)
	if err != nil {
		return errorResult("sp_execute failed: " + err.Error()), nil
	}

	return executionResultToProtocol(result), nil
}

func (c *Connection) handleUnprepare(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	if len(rpcReq.Parameters) < 1 {
		return errorResult("sp_unprepare requires handle parameter"), nil
	}

	var handle int32
	if h, ok := rpcReq.Parameters[0].Value.(int64); ok {
		handle = int32(h)
	} else if h, ok := rpcReq.Parameters[0].Value.(int32); ok {
		handle = h
	} else {
		return errorResult("sp_unprepare: invalid handle type"), nil
	}

	if err := c.phase3.Prepared.Unprepare(ctx, handle); err != nil {
		return errorResult("sp_unprepare failed: " + err.Error()), nil
	}

	return protocol.Result{Type: protocol.ResultOK}, nil
}

// Cursor handlers

func (c *Connection) handleCursorOpen(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	// Parameters: @cursor OUTPUT, @stmt, @scrollopt, @ccopt, @rowcount OUTPUT
	if len(rpcReq.Parameters) < 2 {
		return errorResult("sp_cursoropen requires statement parameter"), nil
	}

	var stmt string
	var scrollOpt tds.CursorScrollOpt = tds.ScrollOptForwardOnly
	var ccOpt tds.CursorConcurrencyOpt = tds.CCOptReadOnly

	// Extract parameters
	for _, p := range rpcReq.Parameters {
		switch p.Name {
		case "stmt", "statement":
			if s, ok := p.Value.(string); ok {
				stmt = s
			}
		case "scrollopt":
			if v, ok := p.Value.(int64); ok {
				scrollOpt = tds.CursorScrollOpt(v)
			}
		case "ccopt":
			if v, ok := p.Value.(int64); ok {
				ccOpt = tds.CursorConcurrencyOpt(v)
			}
		}
	}

	// Fallback to positional
	if stmt == "" && len(rpcReq.Parameters) >= 2 {
		if s, ok := rpcReq.Parameters[1].Value.(string); ok {
			stmt = s
		}
	}

	if stmt == "" {
		return errorResult("sp_cursoropen: statement is required"), nil
	}

	handle, rowCount, columns, err := c.phase3.Cursors.Open(ctx, stmt, scrollOpt, ccOpt)
	if err != nil {
		return errorResult("sp_cursoropen failed: " + err.Error()), nil
	}

	return protocol.Result{
		Type: protocol.ResultOK,
		OutputParams: map[string]interface{}{
			"cursor":   handle,
			"rowcount": rowCount,
		},
		ResultSets: columnsToResultSets(columns),
	}, nil
}

func (c *Connection) handleCursorFetch(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	// Parameters: @cursor, @fetchtype, @rownum, @nrows
	if len(rpcReq.Parameters) < 1 {
		return errorResult("sp_cursorfetch requires cursor handle"), nil
	}

	var handle int32
	var fetchType tds.CursorFetchType = tds.FetchNext
	var rowNum int64 = 0
	var nRows int32 = 1

	for _, p := range rpcReq.Parameters {
		switch p.Name {
		case "cursor":
			if v, ok := p.Value.(int64); ok {
				handle = int32(v)
			}
		case "fetchtype":
			if v, ok := p.Value.(int64); ok {
				fetchType = tds.CursorFetchType(v)
			}
		case "rownum":
			if v, ok := p.Value.(int64); ok {
				rowNum = v
			}
		case "nrows":
			if v, ok := p.Value.(int64); ok {
				nRows = int32(v)
			}
		}
	}

	// Positional fallback
	if handle == 0 && len(rpcReq.Parameters) >= 1 {
		if v, ok := rpcReq.Parameters[0].Value.(int64); ok {
			handle = int32(v)
		}
	}

	result, err := c.phase3.Cursors.Fetch(ctx, handle, fetchType, rowNum, nRows)
	if err != nil {
		return errorResult("sp_cursorfetch failed: " + err.Error()), nil
	}

	return cursorFetchResultToProtocol(result, c.phase3.Cursors, handle), nil
}

func (c *Connection) handleCursorClose(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	if len(rpcReq.Parameters) < 1 {
		return errorResult("sp_cursorclose requires cursor handle"), nil
	}

	var handle int32
	if v, ok := rpcReq.Parameters[0].Value.(int64); ok {
		handle = int32(v)
	} else if v, ok := rpcReq.Parameters[0].Value.(int32); ok {
		handle = v
	}

	if err := c.phase3.Cursors.Close(ctx, handle); err != nil {
		return errorResult("sp_cursorclose failed: " + err.Error()), nil
	}

	return protocol.Result{Type: protocol.ResultOK}, nil
}

func (c *Connection) handleCursorOption(ctx context.Context, rpcReq *tds.RPCRequest) (protocol.Result, error) {
	if len(rpcReq.Parameters) < 3 {
		return errorResult("sp_cursoroption requires cursor, option, and value"), nil
	}

	var handle, option, value int32

	for i, p := range rpcReq.Parameters {
		var v int32
		if iv, ok := p.Value.(int64); ok {
			v = int32(iv)
		} else if iv, ok := p.Value.(int32); ok {
			v = iv
		}

		switch i {
		case 0:
			handle = v
		case 1:
			option = v
		case 2:
			value = v
		}
	}

	if err := c.phase3.Cursors.SetOption(ctx, handle, option, value); err != nil {
		return errorResult("sp_cursoroption failed: " + err.Error()), nil
	}

	return protocol.Result{Type: protocol.ResultOK}, nil
}

// Helper functions

func errorResult(msg string) protocol.Result {
	return protocol.Result{
		Type:    protocol.ResultError,
		Message: msg,
	}
}

func columnsToResultSets(columns []tds.Column) []protocol.ResultSet {
	if len(columns) == 0 {
		return nil
	}

	cols := make([]protocol.ColumnInfo, len(columns))
	for i, c := range columns {
		cols[i] = protocol.ColumnInfo{
			Name:     c.Name,
			Type:     c.Type.String(),
			Nullable: c.Nullable,
			Ordinal:  i,
		}
	}

	return []protocol.ResultSet{{Columns: cols}}
}

func executionResultToProtocol(result *tds.ExecuteResult) protocol.Result {
	if result == nil {
		return protocol.Result{Type: protocol.ResultOK}
	}

	pr := protocol.Result{
		Type:         protocol.ResultOK,
		RowsAffected: result.RowsAffected,
	}

	if result.HasResultSet && len(result.Columns) > 0 {
		pr.Type = protocol.ResultRows
		pr.ResultSets = []protocol.ResultSet{{
			Columns: tdsColumnsToProtocol(result.Columns),
			Rows:    result.Rows,
		}}
	}

	return pr
}

func cursorFetchResultToProtocol(result *tds.CursorFetchResult, mgr tds.CursorManager, handle int32) protocol.Result {
	if result == nil {
		return protocol.Result{Type: protocol.ResultOK}
	}

	pr := protocol.Result{
		Type:         protocol.ResultOK,
		RowsAffected: int64(result.RowCount),
	}

	if len(result.Rows) > 0 {
		pr.Type = protocol.ResultRows

		// Get columns from cursor
		var cols []protocol.ColumnInfo
		if cursor, ok := mgr.GetCursor(handle); ok && len(cursor.Columns) > 0 {
			cols = tdsColumnsToProtocol(cursor.Columns)
		}

		pr.ResultSets = []protocol.ResultSet{{
			Columns: cols,
			Rows:    result.Rows,
		}}
	}

	return pr
}

func tdsColumnsToProtocol(columns []tds.Column) []protocol.ColumnInfo {
	result := make([]protocol.ColumnInfo, len(columns))
	for i, c := range columns {
		result[i] = protocol.ColumnInfo{
			Name:     c.Name,
			Type:     c.Type.String(),
			Nullable: c.Nullable,
			Ordinal:  i,
		}
	}
	return result
}
