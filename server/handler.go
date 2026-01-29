package server

import (
	"context"
	"fmt"
	"time"

	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/procedure"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/runtime"
)

// ConnectionHandler handles a single client connection.
type ConnectionHandler struct {
	conn     protocol.Connection
	runtime  *runtime.Runtime
	registry *procedure.Registry
	logger   *log.Logger

	// Session state
	sessionID   string
	currentDB   string
	tenant      string // Tenant ID (empty for single-tenant mode)
	inTxn       bool
	txnCtx      *runtime.TransactionContext
}

// NewConnectionHandler creates a new connection handler.
func NewConnectionHandler(conn protocol.Connection, rt *runtime.Runtime, reg *procedure.Registry, logger *log.Logger) *ConnectionHandler {
	return NewConnectionHandlerWithTenant(conn, rt, reg, logger, "")
}

// NewConnectionHandlerWithTenant creates a new connection handler with tenant context.
func NewConnectionHandlerWithTenant(conn protocol.Connection, rt *runtime.Runtime, reg *procedure.Registry, logger *log.Logger, tenant string) *ConnectionHandler {
	sessionID := generateSessionID()
	
	fields := []interface{}{
		"session_id", sessionID,
		"remote_addr", conn.RemoteAddr().String(),
	}
	if tenant != "" {
		fields = append(fields, "tenant", tenant)
	}
	logger.Application().Debug("connection handler created", fields...)
	
	return &ConnectionHandler{
		conn:      conn,
		runtime:   rt,
		registry:  reg,
		logger:    logger,
		sessionID: sessionID,
		currentDB: "master", // Default database
		tenant:    tenant,
	}
}

// Serve handles requests from the connection until it closes.
func (h *ConnectionHandler) Serve(ctx context.Context) {
	execLog := h.logger.Execution().WithFields("session_id", h.sessionID)

	h.logger.Application().Info("session started",
		"session_id", h.sessionID,
		"database", h.currentDB,
	)

	requestCount := 0
	for {
		select {
		case <-ctx.Done():
			h.logger.Application().Debug("session context cancelled",
				"session_id", h.sessionID,
				"requests_handled", requestCount,
			)
			return
		default:
		}

		// Read next request
		req, err := h.conn.ReadRequest()
		if err != nil {
			// Connection closed or error
			h.logger.Application().Debug("session ended",
				"session_id", h.sessionID,
				"requests_handled", requestCount,
				"reason", err.Error(),
			)
			return
		}

		requestCount++
		startTime := time.Now()

		// Process request
		result := h.processRequest(ctx, req)

		elapsed := time.Since(startTime)

		// Log execution
		if result.Type == protocol.ResultError {
			execLog.Error("request failed", result.Error,
				"request_type", req.Type.String(),
				"procedure", req.ProcedureName,
				"duration_ms", elapsed.Milliseconds(),
			)
		} else {
			execLog.Debug("request completed",
				"request_type", req.Type.String(),
				"procedure", req.ProcedureName,
				"rows_affected", result.RowsAffected,
				"duration_ms", elapsed.Milliseconds(),
			)
		}

		// Send result
		if err := h.conn.SendResult(result); err != nil {
			h.logger.Application().Error("failed to send result", err,
				"session_id", h.sessionID,
			)
			return
		}
	}
}

// processRequest handles a single request.
func (h *ConnectionHandler) processRequest(ctx context.Context, req protocol.Request) protocol.Result {
	switch req.Type {
	case protocol.RequestExec:
		return h.handleExec(ctx, req)
	case protocol.RequestQuery:
		return h.handleQuery(ctx, req)
	case protocol.RequestPrepare:
		return h.handlePrepare(ctx, req)
	case protocol.RequestCall:
		return h.handleCall(ctx, req)
	case protocol.RequestBeginTxn:
		return h.handleBeginTxn(ctx, req)
	case protocol.RequestCommit:
		return h.handleCommit(ctx, req)
	case protocol.RequestRollback:
		return h.handleRollback(ctx, req)
	case protocol.RequestPing:
		return protocol.Result{Type: protocol.ResultOK, Message: "pong"}
	default:
		err := aulerrors.Newf(aulerrors.ErrCodeProtocolError,
			"unknown request type: %d", req.Type).
			WithOp("ConnectionHandler.processRequest").
			Err()
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}
}

// handleExec handles EXEC procedure_name calls.
func (h *ConnectionHandler) handleExec(ctx context.Context, req protocol.Request) protocol.Result {
	// Look up procedure with tenant context
	proc, err := h.registry.LookupForTenant(req.ProcedureName, h.currentDB, h.tenant)
	if err != nil {
		err = aulerrors.Wrap(err, aulerrors.ErrCodeProcNotFound,
			"procedure not found").
			WithOp("ConnectionHandler.handleExec").
			WithField("procedure", req.ProcedureName).
			WithField("database", h.currentDB).
			WithField("tenant", h.tenant).
			Err()
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	// Build execution context
	execCtx := &runtime.ExecContext{
		SessionID:   h.sessionID,
		Database:    h.currentDB,
		Tenant:      h.tenant,
		Parameters:  req.Parameters,
		Timeout:     30 * time.Second,
		InTxn:       h.inTxn,
		TxnContext:  h.txnCtx,
	}

	// Execute
	execResult, err := h.runtime.Execute(ctx, proc, execCtx)
	if err != nil {
		// Wrap if not already a structured error
		if _, ok := err.(*aulerrors.Error); !ok {
			err = aulerrors.Wrap(err, aulerrors.ErrCodeExecFailed,
				"execution failed").
				WithOp("ConnectionHandler.handleExec").
				WithField("procedure", req.ProcedureName).
				Err()
		}
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	return protocol.Result{
		Type:         protocol.ResultOK,
		RowsAffected: execResult.RowsAffected,
		ResultSets:   convertResultSets(execResult.ResultSets),
		ReturnValue:  execResult.ReturnValue,
		OutputParams: execResult.OutputParams,
		Message:      fmt.Sprintf("(%d rows affected)", execResult.RowsAffected),
	}
}

// handleQuery handles direct SQL queries.
func (h *ConnectionHandler) handleQuery(ctx context.Context, req protocol.Request) protocol.Result {
	// Build execution context
	execCtx := &runtime.ExecContext{
		SessionID:  h.sessionID,
		Database:   h.currentDB,
		Tenant:     h.tenant,
		Parameters: req.Parameters,
		Timeout:    30 * time.Second,
		InTxn:      h.inTxn,
		TxnContext: h.txnCtx,
	}

	// Execute ad-hoc SQL
	execResult, err := h.runtime.ExecuteSQL(ctx, req.SQL, execCtx)
	if err != nil {
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	// If there are result sets, return ResultRows
	resultType := protocol.ResultOK
	if len(execResult.ResultSets) > 0 {
		resultType = protocol.ResultRows
	}

	return protocol.Result{
		Type:         resultType,
		RowsAffected: execResult.RowsAffected,
		ResultSets:   convertResultSets(execResult.ResultSets),
		Message:      fmt.Sprintf("(%d rows affected)", execResult.RowsAffected),
	}
}

// handlePrepare handles prepared statement creation.
func (h *ConnectionHandler) handlePrepare(ctx context.Context, req protocol.Request) protocol.Result {
	err := aulerrors.NotImplemented("prepared statements").
		WithOp("ConnectionHandler.handlePrepare").
		Err()
	return protocol.Result{
		Type:    protocol.ResultError,
		Error:   err,
		Message: err.Error(),
	}
}

// handleCall handles procedure calls (like EXEC but returns results differently).
func (h *ConnectionHandler) handleCall(ctx context.Context, req protocol.Request) protocol.Result {
	// For now, delegate to handleExec
	return h.handleExec(ctx, req)
}

// handleBeginTxn starts a transaction.
func (h *ConnectionHandler) handleBeginTxn(ctx context.Context, req protocol.Request) protocol.Result {
	if h.inTxn {
		// Nested transaction - increment count
		if h.txnCtx != nil {
			h.txnCtx.NestingLevel++
		}
		h.logger.Execution().Debug("nested transaction started",
			"session_id", h.sessionID,
			"nesting_level", h.txnCtx.NestingLevel,
		)
		return protocol.Result{
			Type:    protocol.ResultOK,
			Message: fmt.Sprintf("nested transaction level %d", h.txnCtx.NestingLevel),
		}
	}

	h.inTxn = true
	h.txnCtx = &runtime.TransactionContext{
		ID:           generateTxnID(),
		StartTime:    time.Now(),
		NestingLevel: 1,
	}

	h.logger.Execution().Debug("transaction started",
		"session_id", h.sessionID,
		"txn_id", h.txnCtx.ID,
	)

	return protocol.Result{
		Type:    protocol.ResultOK,
		Message: "transaction started",
	}
}

// handleCommit commits the current transaction.
func (h *ConnectionHandler) handleCommit(ctx context.Context, req protocol.Request) protocol.Result {
	if !h.inTxn {
		err := aulerrors.New(aulerrors.ErrCodeExecNoTransaction, "no transaction active").
			WithOp("ConnectionHandler.handleCommit").
			Err()
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	if h.txnCtx != nil && h.txnCtx.NestingLevel > 1 {
		h.txnCtx.NestingLevel--
		h.logger.Execution().Debug("nested transaction committed",
			"session_id", h.sessionID,
			"txn_id", h.txnCtx.ID,
			"nesting_level", h.txnCtx.NestingLevel,
		)
		return protocol.Result{
			Type:    protocol.ResultOK,
			Message: fmt.Sprintf("nested transaction committed, level %d", h.txnCtx.NestingLevel),
		}
	}

	// Commit the actual transaction
	if err := h.runtime.CommitTransaction(ctx, h.txnCtx); err != nil {
		err = aulerrors.Wrap(err, aulerrors.ErrCodeStorageTxn, "commit failed").
			WithOp("ConnectionHandler.handleCommit").
			WithField("txn_id", h.txnCtx.ID).
			Err()
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	h.logger.Execution().Debug("transaction committed",
		"session_id", h.sessionID,
		"txn_id", h.txnCtx.ID,
	)

	h.inTxn = false
	h.txnCtx = nil

	return protocol.Result{
		Type:    protocol.ResultOK,
		Message: "transaction committed",
	}
}

// handleRollback rolls back the current transaction.
func (h *ConnectionHandler) handleRollback(ctx context.Context, req protocol.Request) protocol.Result {
	if !h.inTxn {
		err := aulerrors.New(aulerrors.ErrCodeExecNoTransaction, "no transaction active").
			WithOp("ConnectionHandler.handleRollback").
			Err()
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	txnID := h.txnCtx.ID

	// Rollback clears all nesting levels
	if err := h.runtime.RollbackTransaction(ctx, h.txnCtx); err != nil {
		err = aulerrors.Wrap(err, aulerrors.ErrCodeStorageTxn, "rollback failed").
			WithOp("ConnectionHandler.handleRollback").
			WithField("txn_id", txnID).
			Err()
		return protocol.Result{
			Type:    protocol.ResultError,
			Error:   err,
			Message: err.Error(),
		}
	}

	h.logger.Execution().Debug("transaction rolled back",
		"session_id", h.sessionID,
		"txn_id", txnID,
	)

	h.inTxn = false
	h.txnCtx = nil

	return protocol.Result{
		Type:    protocol.ResultOK,
		Message: "transaction rolled back",
	}
}

// generateSessionID creates a unique session identifier.
func generateSessionID() string {
	return fmt.Sprintf("sess_%d", time.Now().UnixNano())
}

// generateTxnID creates a unique transaction identifier.
func generateTxnID() string {
	return fmt.Sprintf("txn_%d", time.Now().UnixNano())
}

// convertResultSets converts runtime.ResultSet to protocol.ResultSet.
func convertResultSets(rsSets []runtime.ResultSet) []protocol.ResultSet {
	result := make([]protocol.ResultSet, len(rsSets))
	for i, rs := range rsSets {
		cols := make([]protocol.ColumnInfo, len(rs.Columns))
		for j, col := range rs.Columns {
			cols[j] = protocol.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
				Length:   col.Length,
				Ordinal:  col.Ordinal,
			}
		}
		result[i] = protocol.ResultSet{
			Columns: cols,
			Rows:    rs.Rows,
		}
	}
	return result
}
