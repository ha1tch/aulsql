package tds

import (
	"testing"
)

func TestParseTransactionSQL(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantType TransactionRequestType
		wantName string
		wantNil  bool
	}{
		// BEGIN TRANSACTION variants
		{
			name:     "begin tran simple",
			sql:      "BEGIN TRAN",
			wantType: TxnBegin,
		},
		{
			name:     "begin transaction",
			sql:      "BEGIN TRANSACTION",
			wantType: TxnBegin,
		},
		{
			name:     "begin tran with name",
			sql:      "BEGIN TRAN MyTran",
			wantType: TxnBegin,
			wantName: "MyTran",
		},
		{
			name:     "begin transaction with bracketed name",
			sql:      "BEGIN TRANSACTION [My Transaction]",
			wantType: TxnBegin,
			wantName: "My Transaction",
		},
		{
			name:     "begin tran lowercase",
			sql:      "begin tran",
			wantType: TxnBegin,
		},
		{
			name:     "begin tran with semicolon",
			sql:      "BEGIN TRAN;",
			wantType: TxnBegin,
		},
		{
			name:     "begin tran with mark",
			sql:      "BEGIN TRAN MyTran WITH MARK 'description'",
			wantType: TxnBegin,
			wantName: "MyTran",
		},

		// COMMIT variants
		{
			name:     "commit simple",
			sql:      "COMMIT",
			wantType: TxnCommit,
		},
		{
			name:     "commit tran",
			sql:      "COMMIT TRAN",
			wantType: TxnCommit,
		},
		{
			name:     "commit transaction",
			sql:      "COMMIT TRANSACTION",
			wantType: TxnCommit,
		},
		{
			name:     "commit with name",
			sql:      "COMMIT TRAN MyTran",
			wantType: TxnCommit,
			wantName: "MyTran",
		},
		{
			name:     "commit lowercase",
			sql:      "commit",
			wantType: TxnCommit,
		},

		// ROLLBACK variants
		{
			name:     "rollback simple",
			sql:      "ROLLBACK",
			wantType: TxnRollback,
		},
		{
			name:     "rollback tran",
			sql:      "ROLLBACK TRAN",
			wantType: TxnRollback,
		},
		{
			name:     "rollback transaction",
			sql:      "ROLLBACK TRANSACTION",
			wantType: TxnRollback,
		},
		{
			name:     "rollback to savepoint",
			sql:      "ROLLBACK TRAN SavePoint1",
			wantType: TxnRollbackToSavepoint,
			wantName: "SavePoint1",
		},

		// SAVE TRANSACTION (savepoint)
		{
			name:     "save tran",
			sql:      "SAVE TRAN SavePoint1",
			wantType: TxnSavepoint,
			wantName: "SavePoint1",
		},
		{
			name:     "save transaction",
			sql:      "SAVE TRANSACTION [My Savepoint]",
			wantType: TxnSavepoint,
			wantName: "My Savepoint",
		},

		// SET TRANSACTION ISOLATION LEVEL
		{
			name:     "isolation read uncommitted",
			sql:      "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
			wantType: TxnSetIsolation,
		},
		{
			name:     "isolation read committed",
			sql:      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
			wantType: TxnSetIsolation,
		},
		{
			name:     "isolation repeatable read",
			sql:      "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
			wantType: TxnSetIsolation,
		},
		{
			name:     "isolation serializable",
			sql:      "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
			wantType: TxnSetIsolation,
		},
		{
			name:     "isolation snapshot",
			sql:      "SET TRANSACTION ISOLATION LEVEL SNAPSHOT",
			wantType: TxnSetIsolation,
		},

		// Non-transaction SQL
		{
			name:    "select statement",
			sql:     "SELECT * FROM Users",
			wantNil: true,
		},
		{
			name:    "insert statement",
			sql:     "INSERT INTO Users VALUES (1, 'test')",
			wantNil: true,
		},
		{
			name:    "begin but not tran",
			sql:     "BEGIN SELECT 1 END",
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseTransactionSQL(tt.sql)

			if tt.wantNil {
				if result != nil {
					t.Errorf("expected nil, got %+v", result)
				}
				return
			}

			if result == nil {
				t.Fatalf("expected non-nil result")
			}

			if result.Type != tt.wantType {
				t.Errorf("type = %v, want %v", result.Type, tt.wantType)
			}

			gotName := result.Name
			if result.Type == TxnRollbackToSavepoint || result.Type == TxnSavepoint {
				gotName = result.SavepointName
			}
			if gotName != tt.wantName {
				t.Errorf("name = %q, want %q", gotName, tt.wantName)
			}
		})
	}
}

func TestIsTransactionSQL(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"BEGIN TRAN", true},
		{"begin transaction", true},
		{"COMMIT", true},
		{"commit tran", true},
		{"ROLLBACK", true},
		{"rollback transaction", true},
		{"SAVE TRAN sp1", true},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", true},
		{"SELECT 1", false},
		{"INSERT INTO t VALUES (1)", false},
		{"UPDATE t SET x = 1", false},
		{"DELETE FROM t", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			if got := IsTransactionSQL(tt.sql); got != tt.want {
				t.Errorf("IsTransactionSQL(%q) = %v, want %v", tt.sql, got, tt.want)
			}
		})
	}
}

func TestParseIsolationLevel(t *testing.T) {
	tests := []struct {
		sql       string
		wantLevel IsolationLevel
	}{
		{"SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", IsolationReadUncommitted},
		{"SET TRANSACTION ISOLATION LEVEL READ COMMITTED", IsolationReadCommitted},
		{"SET TRANSACTION ISOLATION LEVEL REPEATABLE READ", IsolationRepeatableRead},
		{"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", IsolationSerializable},
		{"SET TRANSACTION ISOLATION LEVEL SNAPSHOT", IsolationSnapshot},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			result := ParseTransactionSQL(tt.sql)
			if result == nil {
				t.Fatal("expected non-nil result")
			}
			if result.IsolationLevel != tt.wantLevel {
				t.Errorf("isolation = %v, want %v", result.IsolationLevel, tt.wantLevel)
			}
		})
	}
}
