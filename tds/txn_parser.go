package tds

import (
	"github.com/ha1tch/aul/tsqlparser/ast"
	"github.com/ha1tch/aul/tsqlparser/lexer"
	"github.com/ha1tch/aul/tsqlparser/parser"
)

// ParseTransactionSQL attempts to parse a SQL statement as a transaction command
// using the tsqlparser. Returns nil if the SQL is not a transaction statement.
func ParseTransactionSQL(sql string) *TransactionRequest {
	l := lexer.New(sql)
	p := parser.New(l)
	program := p.ParseProgram()

	// Check for parse errors - if there are errors, it might still be
	// a transaction statement that the parser couldn't fully handle
	if len(p.Errors()) > 0 || len(program.Statements) == 0 {
		return nil
	}

	// We only handle single-statement transaction commands
	if len(program.Statements) != 1 {
		return nil
	}

	return classifyTransactionStatement(program.Statements[0])
}

// classifyTransactionStatement examines an AST statement and returns
// a TransactionRequest if it's a transaction-related statement.
func classifyTransactionStatement(stmt ast.Statement) *TransactionRequest {
	switch s := stmt.(type) {
	case *ast.BeginTransactionStatement:
		return &TransactionRequest{
			Type: TxnBegin,
			Name: identifierValue(s.Name),
		}

	case *ast.CommitTransactionStatement:
		return &TransactionRequest{
			Type: TxnCommit,
			Name: identifierValue(s.Name),
		}

	case *ast.RollbackTransactionStatement:
		name := identifierValue(s.Name)
		if name != "" {
			// Named rollback could be to a savepoint
			return &TransactionRequest{
				Type:          TxnRollbackToSavepoint,
				SavepointName: name,
			}
		}
		return &TransactionRequest{
			Type: TxnRollback,
		}

	case *ast.SaveTransactionStatement:
		return &TransactionRequest{
			Type:          TxnSavepoint,
			SavepointName: identifierValue(s.SavepointName),
		}

	case *ast.SetTransactionIsolationStatement:
		return &TransactionRequest{
			Type:           TxnSetIsolation,
			IsolationLevel: parseIsolationLevelString(s.Level),
		}

	default:
		return nil
	}
}

// identifierValue safely extracts the value from an identifier.
func identifierValue(id *ast.Identifier) string {
	if id == nil {
		return ""
	}
	return id.Value
}

// parseIsolationLevelString converts a string to IsolationLevel.
func parseIsolationLevelString(s string) IsolationLevel {
	// Normalize the string
	switch s {
	case "READ UNCOMMITTED", "READUNCOMMITTED", "1":
		return IsolationReadUncommitted
	case "READ COMMITTED", "READCOMMITTED", "2":
		return IsolationReadCommitted
	case "REPEATABLE READ", "REPEATABLEREAD", "3":
		return IsolationRepeatableRead
	case "SERIALIZABLE", "4":
		return IsolationSerializable
	case "SNAPSHOT", "5":
		return IsolationSnapshot
	default:
		return IsolationReadCommitted
	}
}

// IsTransactionSQL returns true if the SQL appears to be a transaction statement.
// This uses the parser for accurate detection.
func IsTransactionSQL(sql string) bool {
	return ParseTransactionSQL(sql) != nil
}

// TxnSetIsolation is a transaction request to set isolation level.
const TxnSetIsolation TransactionRequestType = 10

