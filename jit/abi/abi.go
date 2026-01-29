// Package abi defines the shared types used between the JIT host and generated plugins.
//
// This package exists to solve the Go plugin type compatibility requirement:
// types must match exactly (same package path) for plugin symbol type assertions
// to succeed. Both the aul host and generated plugin code import this package.
package abi

import "context"

// StorageBackend is the interface for database access.
// Generated plugins use this to execute SQL against the backend.
type StorageBackend interface {
	// Query executes a query that returns rows.
	Query(ctx context.Context, sql string, args ...interface{}) ([]ResultSet, error)

	// QueryRow executes a query that returns a single row.
	QueryRow(ctx context.Context, sql string, args ...interface{}) ([]interface{}, error)

	// Exec executes a statement that doesn't return rows.
	Exec(ctx context.Context, sql string, args ...interface{}) (int64, error)

	// Begin starts a transaction.
	Begin(ctx context.Context) (Transaction, error)
}

// Transaction represents a database transaction.
type Transaction interface {
	// Query executes a query within the transaction.
	Query(ctx context.Context, sql string, args ...interface{}) ([]ResultSet, error)

	// Exec executes a statement within the transaction.
	Exec(ctx context.Context, sql string, args ...interface{}) (int64, error)

	// Commit commits the transaction.
	Commit() error

	// Rollback rolls back the transaction.
	Rollback() error
}

// ExecResult holds the result of a procedure execution.
type ExecResult struct {
	// RowsAffected is the number of rows affected by the last statement.
	RowsAffected int64

	// ResultSets contains the result sets returned by the procedure.
	ResultSets []ResultSet

	// ReturnValue is the RETURN value from the procedure (if any).
	ReturnValue interface{}

	// OutputParams contains OUTPUT parameter values.
	OutputParams map[string]interface{}

	// Warnings contains any warnings generated during execution.
	Warnings []string
}

// ResultSet represents a tabular result.
type ResultSet struct {
	// Columns describes the columns in the result set.
	Columns []ColumnInfo

	// Rows contains the data rows.
	Rows [][]interface{}
}

// ColumnInfo describes a result column.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Length   int
	Ordinal  int
}

// CompiledFunc is the signature for compiled procedure functions.
// Generated plugins must export a variable of this type named "Execute".
//
// Example usage in generated plugin:
//
//	package main
//
//	import "github.com/ha1tch/aul/jit/abi"
//
//	func execute(ctx context.Context, params map[string]interface{}, storage abi.StorageBackend) (*abi.ExecResult, error) {
//	    // ... procedure logic ...
//	}
//
//	var Execute abi.CompiledFunc = execute
type CompiledFunc func(ctx context.Context, params map[string]interface{}, storage StorageBackend) (*ExecResult, error)

// ExecContext provides additional context for procedure execution.
// This may be expanded in the future to include session info, tenant context, etc.
type ExecContext struct {
	SessionID  string
	Database   string
	Schema     string
	User       string
	Tenant     string
	Parameters map[string]interface{}
}
