// Package runtime provides the execution runtime for aul.
//
// The runtime executes stored procedures using two strategies:
// 1. Interpreted execution via tgpiler's tsqlruntime (flexible, dynamic SQL support)
// 2. JIT-compiled execution via transpiled Go code (fast, optimised)
//
// The runtime automatically promotes frequently-executed procedures to JIT
// based on the configured threshold.
package runtime

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ha1tch/aul/pkg/jit"
	"github.com/ha1tch/aul/pkg/jit/abi"
	aulerrors "github.com/ha1tch/aul/pkg/errors"
	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/pkg/procedure"
)

// Runtime manages procedure execution.
type Runtime struct {
	mu sync.RWMutex

	// Configuration
	config Config

	// Logging
	logger *log.Logger

	// Components
	registry   *procedure.Registry
	storage    StorageBackend
	jitManager *jit.Manager

	// Execution tracking
	activeExecs   int64 // Atomic counter
	totalExecs    int64 // Atomic counter
	totalTimeNs   int64 // Atomic counter
	execSemaphore chan struct{}

	// Interpreter instance (reused across executions)
	interpreterPool sync.Pool
}

// Config holds runtime configuration.
type Config struct {
	// Dialect settings
	DefaultDialect string

	// JIT compilation
	JITEnabled   bool
	JITThreshold int // Executions before JIT compilation

	// Concurrency
	MaxConcurrency int

	// Execution limits
	ExecTimeout    time.Duration
	MaxResultRows  int
	MaxResultSets  int
	MaxNestingLevel int

	// Logging
	LogQueriesRewritten bool // Log queries after rewriting
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		DefaultDialect:  "tsql",
		JITEnabled:      true,
		JITThreshold:    100,
		MaxConcurrency:  100,
		ExecTimeout:     30 * time.Second,
		MaxResultRows:   100000,
		MaxResultSets:   100,
		MaxNestingLevel: 32,
	}
}

// New creates a new runtime.
func New(cfg Config, registry *procedure.Registry, logger *log.Logger) *Runtime {
	r := &Runtime{
		config:        cfg,
		logger:        logger,
		registry:      registry,
		execSemaphore: make(chan struct{}, cfg.MaxConcurrency),
	}

	// Initialise JIT manager if enabled
	if cfg.JITEnabled {
		r.jitManager = jit.NewManager(jit.Config{
			Threshold:    cfg.JITThreshold,
			OutputDir:    "./jit_cache",
			KeepSource:   true,
			Optimisation: jit.OptLevel2,
		}, logger)
		
		logger.System().Info("JIT compilation enabled",
			"threshold", cfg.JITThreshold,
		)
	}

	// Initialise interpreter pool
	r.interpreterPool = sync.Pool{
		New: func() interface{} {
			return newInterpreter(cfg, logger, registry)
		},
	}

	return r
}

// SetStorage sets the storage backend.
func (r *Runtime) SetStorage(storage StorageBackend) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.storage = storage
}

// Execute runs a procedure.
func (r *Runtime) Execute(ctx context.Context, proc *procedure.Procedure, execCtx *ExecContext) (*ExecResult, error) {
	// Acquire semaphore for concurrency limiting
	select {
	case r.execSemaphore <- struct{}{}:
		defer func() { <-r.execSemaphore }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Track execution
	atomic.AddInt64(&r.activeExecs, 1)
	defer atomic.AddInt64(&r.activeExecs, -1)
	atomic.AddInt64(&r.totalExecs, 1)

	startTime := time.Now()
	defer func() {
		elapsed := time.Since(startTime).Nanoseconds()
		atomic.AddInt64(&r.totalTimeNs, elapsed)
		atomic.AddInt64(&proc.TotalTimeNs, elapsed)
		atomic.AddInt64(&proc.ExecCount, 1)
		proc.LastExecAt = time.Now()
	}()

	// Apply timeout
	if execCtx.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, execCtx.Timeout)
		defer cancel()
	}

	// Choose execution strategy
	if proc.JITCompiled && proc.JITCode != nil {
		return r.executeJIT(ctx, proc, execCtx)
	}

	// Interpreted execution
	result, err := r.executeInterpreted(ctx, proc, execCtx)
	if err != nil {
		return nil, err
	}

	// Check if we should trigger JIT compilation
	if r.config.JITEnabled && !proc.JITCompiled {
		if int(atomic.LoadInt64(&proc.ExecCount)) >= r.config.JITThreshold {
			// Trigger async JIT compilation
			go r.triggerJIT(proc)
		}
	}

	return result, nil
}

// ExecuteSQL runs ad-hoc SQL.
func (r *Runtime) ExecuteSQL(ctx context.Context, sql string, execCtx *ExecContext) (*ExecResult, error) {
	// Acquire semaphore
	select {
	case r.execSemaphore <- struct{}{}:
		defer func() { <-r.execSemaphore }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	atomic.AddInt64(&r.activeExecs, 1)
	defer atomic.AddInt64(&r.activeExecs, -1)
	atomic.AddInt64(&r.totalExecs, 1)

	// Apply timeout
	if execCtx.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, execCtx.Timeout)
		defer cancel()
	}

	// Get interpreter from pool
	interp := r.interpreterPool.Get().(*interpreter)
	defer r.interpreterPool.Put(interp)

	// Execute
	return interp.ExecuteSQL(ctx, sql, execCtx, r.storage)
}

// executeInterpreted runs a procedure using the interpreter.
func (r *Runtime) executeInterpreted(ctx context.Context, proc *procedure.Procedure, execCtx *ExecContext) (*ExecResult, error) {
	// Get interpreter from pool
	interp := r.interpreterPool.Get().(*interpreter)
	defer r.interpreterPool.Put(interp)

	return interp.Execute(ctx, proc, execCtx, r.storage)
}

// executeJIT runs a procedure using JIT-compiled code.
func (r *Runtime) executeJIT(ctx context.Context, proc *procedure.Procedure, execCtx *ExecContext) (*ExecResult, error) {
	if r.jitManager == nil {
		return nil, aulerrors.New(aulerrors.ErrCodeJITDisabled, "JIT manager not initialised").
			WithOp("Runtime.executeJIT").
			Err()
	}

	// Create adapter for storage
	storageAdapter := &jitStorageAdapter{storage: r.storage}

	// Execute - JIT Manager takes params directly, not ExecContext
	start := time.Now()
	jitResult, err := r.jitManager.Execute(ctx, proc, execCtx.Parameters, storageAdapter)
	execTime := time.Since(start)
	if err != nil {
		return nil, err
	}

	// Convert result back
	result := &ExecResult{
		RowsAffected: jitResult.RowsAffected,
		ReturnValue:  jitResult.ReturnValue,
		OutputParams: jitResult.OutputParams,
		ExecTimeNs:   execTime.Nanoseconds(),
		Warnings:     jitResult.Warnings,
	}

	for _, rs := range jitResult.ResultSets {
		resultSet := ResultSet{
			Rows: rs.Rows,
		}
		for _, col := range rs.Columns {
			resultSet.Columns = append(resultSet.Columns, ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
				Length:   col.Length,
				Ordinal:  col.Ordinal,
			})
		}
		result.ResultSets = append(result.ResultSets, resultSet)
	}

	return result, nil
}

// jitStorageAdapter adapts runtime.StorageBackend to abi.StorageBackend
type jitStorageAdapter struct {
	storage StorageBackend
}

func (a *jitStorageAdapter) Query(ctx context.Context, sql string, args ...interface{}) ([]abi.ResultSet, error) {
	results, err := a.storage.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	
	var abiResults []abi.ResultSet
	for _, rs := range results {
		abiRS := abi.ResultSet{
			Rows: rs.Rows,
		}
		for _, col := range rs.Columns {
			abiRS.Columns = append(abiRS.Columns, abi.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
				Length:   col.Length,
				Ordinal:  col.Ordinal,
			})
		}
		abiResults = append(abiResults, abiRS)
	}
	return abiResults, nil
}

func (a *jitStorageAdapter) QueryRow(ctx context.Context, sql string, args ...interface{}) ([]interface{}, error) {
	return a.storage.QueryRow(ctx, sql, args...)
}

func (a *jitStorageAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	return a.storage.Exec(ctx, sql, args...)
}

func (a *jitStorageAdapter) Begin(ctx context.Context) (abi.Transaction, error) {
	txnCtx, err := a.storage.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &jitTxnAdapter{storage: a.storage, txnCtx: txnCtx}, nil
}

// jitTxnAdapter adapts runtime transaction to abi.Transaction
type jitTxnAdapter struct {
	storage StorageBackend
	txnCtx  *TransactionContext
}

func (t *jitTxnAdapter) Query(ctx context.Context, sql string, args ...interface{}) ([]abi.ResultSet, error) {
	// Use same storage, queries happen in transaction context
	results, err := t.storage.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	
	var abiResults []abi.ResultSet
	for _, rs := range results {
		abiRS := abi.ResultSet{
			Rows: rs.Rows,
		}
		for _, col := range rs.Columns {
			abiRS.Columns = append(abiRS.Columns, abi.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
				Length:   col.Length,
				Ordinal:  col.Ordinal,
			})
		}
		abiResults = append(abiResults, abiRS)
	}
	return abiResults, nil
}

func (t *jitTxnAdapter) Exec(ctx context.Context, sql string, args ...interface{}) (int64, error) {
	return t.storage.Exec(ctx, sql, args...)
}

func (t *jitTxnAdapter) Commit() error {
	return t.storage.Commit(context.Background(), t.txnCtx)
}

func (t *jitTxnAdapter) Rollback() error {
	return t.storage.Rollback(context.Background(), t.txnCtx)
}

// triggerJIT initiates JIT compilation for a procedure.
func (r *Runtime) triggerJIT(proc *procedure.Procedure) {
	if r.jitManager == nil {
		return
	}

	r.logger.Execution().Info("triggering JIT compilation",
		"procedure", proc.QualifiedName(),
		"exec_count", proc.ExecCount,
	)

	if err := r.jitManager.Compile(proc); err != nil {
		r.logger.Execution().Error("JIT compilation failed", err,
			"procedure", proc.QualifiedName(),
		)
		return
	}

	proc.JITCompiled = true
	proc.JITCompiledAt = time.Now()
	
	r.logger.Execution().Info("JIT compilation completed",
		"procedure", proc.QualifiedName(),
	)
}

// CommitTransaction commits a transaction.
func (r *Runtime) CommitTransaction(ctx context.Context, txn *TransactionContext) error {
	if r.storage == nil {
		return aulerrors.New(aulerrors.ErrCodeStorageConnect, "no storage backend configured").
			WithOp("Runtime.CommitTransaction").
			Err()
	}
	return r.storage.Commit(ctx, txn)
}

// RollbackTransaction rolls back a transaction.
func (r *Runtime) RollbackTransaction(ctx context.Context, txn *TransactionContext) error {
	if r.storage == nil {
		return aulerrors.New(aulerrors.ErrCodeStorageConnect, "no storage backend configured").
			WithOp("Runtime.RollbackTransaction").
			Err()
	}
	return r.storage.Rollback(ctx, txn)
}

// Stats returns runtime statistics.
func (r *Runtime) Stats() RuntimeStats {
	return RuntimeStats{
		ActiveExecutions: atomic.LoadInt64(&r.activeExecs),
		TotalExecutions:  atomic.LoadInt64(&r.totalExecs),
		TotalTimeNs:      atomic.LoadInt64(&r.totalTimeNs),
		JITStats:         r.JITStats(),
	}
}

// JITStats returns JIT compilation statistics.
func (r *Runtime) JITStats() JITStats {
	if r.jitManager == nil {
		return JITStats{}
	}
	stats := r.jitManager.Stats()
	return JITStats{
		CompiledCount:   stats.CompiledCount,
		CompilationTime: stats.CompilationTime,
		CacheHits:       stats.CacheHits,
		CacheMisses:     stats.CacheMisses,
	}
}

// RuntimeStats holds runtime statistics.
type RuntimeStats struct {
	ActiveExecutions int64
	TotalExecutions  int64
	TotalTimeNs      int64
	JITStats         JITStats
}

// JITStats holds JIT compilation statistics.
type JITStats struct {
	CompiledCount   int
	CompilationTime time.Duration
	CacheHits       int64
	CacheMisses     int64
}

// AvgExecTimeMs returns the average execution time in milliseconds.
func (s RuntimeStats) AvgExecTimeMs() float64 {
	if s.TotalExecutions == 0 {
		return 0
	}
	return float64(s.TotalTimeNs) / float64(s.TotalExecutions) / 1_000_000
}

// ExecContext holds execution context for a procedure call.
type ExecContext struct {
	// Session context
	SessionID string
	Database  string
	Tenant    string // Tenant ID for multi-tenant deployments
	User      string

	// Parameters
	Parameters map[string]interface{}

	// Execution options
	Timeout       time.Duration
	NoCount       bool
	MaxRows       int
	NestingLevel  int

	// Transaction context
	InTxn      bool
	TxnContext *TransactionContext

	// Caller info (for nested EXEC)
	CallerProc string
	CallStack  []string
}

// ExecResult holds the result of a procedure execution.
type ExecResult struct {
	// Rows affected
	RowsAffected int64

	// Result sets
	ResultSets []ResultSet

	// Return value (for functions or RETURN statement)
	ReturnValue interface{}

	// Output parameters
	OutputParams map[string]interface{}

	// Execution metadata
	ExecTimeNs int64
	Warnings   []string
}

// ResultSet represents a tabular result.
type ResultSet struct {
	Columns []ColumnInfo
	Rows    [][]interface{}
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Length   int
	Ordinal  int
}

// TransactionContext holds transaction state.
type TransactionContext struct {
	ID           string
	StartTime    time.Time
	NestingLevel int
	Savepoints   []string
	State        TxnState
}

// TxnState represents transaction state.
type TxnState int

const (
	TxnActive TxnState = iota
	TxnCommitted
	TxnRolledBack
	TxnError
)

func (s TxnState) String() string {
	switch s {
	case TxnActive:
		return "ACTIVE"
	case TxnCommitted:
		return "COMMITTED"
	case TxnRolledBack:
		return "ROLLED_BACK"
	case TxnError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}
