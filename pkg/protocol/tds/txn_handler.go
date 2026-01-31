package tds

import (
	"context"

	"github.com/ha1tch/aul/pkg/protocol"
	"github.com/ha1tch/aul/pkg/tds"
)

// handleTransactionSQL checks if SQL is a transaction statement and handles it.
// Returns (handled, result, error). If handled is false, the SQL should be
// executed normally.
func (c *Connection) handleTransactionSQL(ctx context.Context, sql string) (bool, protocol.Result, error) {
	txnReq := tds.ParseTransactionSQL(sql)
	if txnReq == nil {
		return false, protocol.Result{}, nil
	}

	var result protocol.Result
	var err error

	switch txnReq.Type {
	case tds.TxnBegin:
		result, err = c.handleBeginTransaction(ctx, txnReq)
	case tds.TxnCommit:
		result, err = c.handleCommitTransaction(ctx, txnReq)
	case tds.TxnRollback:
		result, err = c.handleRollbackTransaction(ctx, txnReq)
	case tds.TxnSavepoint:
		result, err = c.handleSavepoint(ctx, txnReq)
	case tds.TxnRollbackToSavepoint:
		result, err = c.handleRollbackToSavepoint(ctx, txnReq)
	case tds.TxnSetIsolation:
		result, err = c.handleSetIsolation(ctx, txnReq)
	default:
		return false, protocol.Result{}, nil
	}

	return true, result, err
}

func (c *Connection) handleBeginTransaction(ctx context.Context, req *tds.TransactionRequest) (protocol.Result, error) {
	// Check if already in transaction
	if c.phase3State.InTransaction() {
		// Nested transaction - increment nesting level
		// SQL Server supports nested BEGIN TRAN but only commits on outermost
		c.phase3State.TransactionNestingLevel++
		return protocol.Result{Type: protocol.ResultOK}, nil
	}

	// Start new transaction
	isolation := req.IsolationLevel
	if isolation == 0 {
		isolation = c.phase3State.IsolationLevel
	}

	desc, err := c.phase3.Transactions.BeginTransaction(ctx, req.Name, isolation)
	if err != nil {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "BEGIN TRANSACTION failed: " + err.Error(),
		}, nil
	}

	// Update connection state
	c.phase3State.ActiveTransaction = &desc
	c.phase3State.TransactionName = req.Name
	c.phase3State.TransactionNestingLevel = 1

	// Build response with ENVCHANGE
	return protocol.Result{
		Type: protocol.ResultOK,
		// The ENVCHANGE token will be added by SendResult based on this info
		OutputParams: map[string]interface{}{
			"_txn_descriptor": desc.Bytes(),
		},
	}, nil
}

func (c *Connection) handleCommitTransaction(ctx context.Context, req *tds.TransactionRequest) (protocol.Result, error) {
	if !c.phase3State.InTransaction() {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION",
		}, nil
	}

	// Handle nested transactions
	if c.phase3State.TransactionNestingLevel > 1 {
		c.phase3State.TransactionNestingLevel--
		return protocol.Result{Type: protocol.ResultOK}, nil
	}

	// Commit the transaction
	desc := *c.phase3State.ActiveTransaction
	err := c.phase3.Transactions.CommitTransaction(ctx, desc)
	if err != nil {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "COMMIT TRANSACTION failed: " + err.Error(),
		}, nil
	}

	// Clear transaction state
	c.phase3State.ActiveTransaction = nil
	c.phase3State.TransactionName = ""
	c.phase3State.TransactionNestingLevel = 0

	return protocol.Result{
		Type: protocol.ResultOK,
		OutputParams: map[string]interface{}{
			"_txn_committed": desc.Bytes(),
		},
	}, nil
}

func (c *Connection) handleRollbackTransaction(ctx context.Context, req *tds.TransactionRequest) (protocol.Result, error) {
	if !c.phase3State.InTransaction() {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "ROLLBACK TRANSACTION request has no corresponding BEGIN TRANSACTION",
		}, nil
	}

	desc := *c.phase3State.ActiveTransaction
	err := c.phase3.Transactions.RollbackTransaction(ctx, desc, "")
	if err != nil {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "ROLLBACK TRANSACTION failed: " + err.Error(),
		}, nil
	}

	// Clear transaction state (rollback clears all nesting levels)
	c.phase3State.ActiveTransaction = nil
	c.phase3State.TransactionName = ""
	c.phase3State.TransactionNestingLevel = 0

	return protocol.Result{
		Type: protocol.ResultOK,
		OutputParams: map[string]interface{}{
			"_txn_rolledback": desc.Bytes(),
		},
	}, nil
}

func (c *Connection) handleSavepoint(ctx context.Context, req *tds.TransactionRequest) (protocol.Result, error) {
	if !c.phase3State.InTransaction() {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "SAVE TRANSACTION can only be used inside a transaction",
		}, nil
	}

	desc := *c.phase3State.ActiveTransaction
	err := c.phase3.Transactions.CreateSavepoint(ctx, desc, req.SavepointName)
	if err != nil {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "SAVE TRANSACTION failed: " + err.Error(),
		}, nil
	}

	return protocol.Result{Type: protocol.ResultOK}, nil
}

func (c *Connection) handleRollbackToSavepoint(ctx context.Context, req *tds.TransactionRequest) (protocol.Result, error) {
	if !c.phase3State.InTransaction() {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "ROLLBACK to savepoint can only be used inside a transaction",
		}, nil
	}

	desc := *c.phase3State.ActiveTransaction
	err := c.phase3.Transactions.RollbackTransaction(ctx, desc, req.SavepointName)
	if err != nil {
		return protocol.Result{
			Type:    protocol.ResultError,
			Message: "ROLLBACK to savepoint failed: " + err.Error(),
		}, nil
	}

	// Note: Rolling back to a savepoint doesn't end the transaction
	return protocol.Result{Type: protocol.ResultOK}, nil
}

func (c *Connection) handleSetIsolation(ctx context.Context, req *tds.TransactionRequest) (protocol.Result, error) {
	// SET TRANSACTION ISOLATION LEVEL takes effect for the next transaction
	c.phase3State.IsolationLevel = req.IsolationLevel
	return protocol.Result{Type: protocol.ResultOK}, nil
}
