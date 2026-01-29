package tds

import (
	"context"
	"testing"

	"github.com/ha1tch/aul/tds"
)

// TestPhase3HandlersDefault verifies that default handlers return "not implemented".
func TestPhase3HandlersDefault(t *testing.T) {
	handlers := DefaultPhase3Handlers()

	ctx := context.Background()

	// Test transactions
	_, err := handlers.Transactions.BeginTransaction(ctx, "", tds.IsolationReadCommitted)
	if err == nil {
		t.Error("expected error from null transaction manager")
	}

	// Test prepared statements
	_, _, err = handlers.Prepared.Prepare(ctx, "SELECT 1", "")
	if err == nil {
		t.Error("expected error from null prepared statement store")
	}

	// Test cursors
	_, _, _, err = handlers.Cursors.Open(ctx, "SELECT 1", tds.ScrollOptForwardOnly, tds.CCOptReadOnly)
	if err == nil {
		t.Error("expected error from null cursor manager")
	}
}

// TestConnectionPhase3State tests connection state management.
func TestConnectionPhase3State(t *testing.T) {
	state := NewConnectionPhase3State()

	// Initial state
	if state.InTransaction() {
		t.Error("expected no active transaction initially")
	}

	if state.IsolationLevel != tds.IsolationReadCommitted {
		t.Errorf("expected default isolation READ COMMITTED, got %v", state.IsolationLevel)
	}

	// Simulate transaction start
	desc := tds.NewTransactionDescriptor()
	state.ActiveTransaction = &desc
	state.TransactionNestingLevel = 1

	if !state.InTransaction() {
		t.Error("expected active transaction")
	}

	// Simulate nested transaction
	state.TransactionNestingLevel = 2
	if !state.InTransaction() {
		t.Error("expected active transaction with nesting")
	}

	// Simulate commit
	state.ActiveTransaction = nil
	state.TransactionNestingLevel = 0

	if state.InTransaction() {
		t.Error("expected no active transaction after clear")
	}
}

// TestClassifyRPCRequest tests Phase 3 RPC classification.
func TestClassifyRPCRequest(t *testing.T) {
	tests := []struct {
		name     string
		procID   uint16
		expected Phase3RequestType
	}{
		{"sp_prepare", tds.ProcIDPrepare, Phase3Prepare},
		{"sp_execute", tds.ProcIDExecute, Phase3Execute},
		{"sp_unprepare", tds.ProcIDUnprepare, Phase3Unprepare},
		{"sp_cursoropen", tds.ProcIDCursorOpen, Phase3CursorOpen},
		{"sp_cursorfetch", tds.ProcIDCursorFetch, Phase3CursorFetch},
		{"sp_cursorclose", tds.ProcIDCursorClose, Phase3CursorClose},
		{"sp_cursoroption", tds.ProcIDCursorOption, Phase3CursorOption},
		{"sp_executesql", tds.ProcIDExecuteSQL, Phase3None}, // Not Phase 3
		{"unknown", 9999, Phase3None},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &tds.RPCRequest{ProcID: tt.procID}
			got := classifyRPCRequest(req)
			if got != tt.expected {
				t.Errorf("classifyRPCRequest(%d) = %v, want %v", tt.procID, got, tt.expected)
			}
		})
	}
}

// TestHandlePoolConcurrency tests handle pool under concurrent access.
func TestHandlePoolConcurrency(t *testing.T) {
	pool := tds.NewHandlePool()
	handles := make(chan int32, 100)

	// Acquire handles concurrently
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				h := pool.Acquire()
				handles <- h
			}
		}()
	}

	// Collect all handles
	seen := make(map[int32]bool)
	for i := 0; i < 100; i++ {
		h := <-handles
		if seen[h] {
			t.Errorf("duplicate handle %d", h)
		}
		seen[h] = true
	}

	// Release and reacquire
	pool.Release(5)
	pool.Release(10)

	h1 := pool.Acquire()
	h2 := pool.Acquire()

	// Should reuse released handles
	if h1 != 10 && h1 != 5 {
		t.Logf("h1 = %d (may or may not be reused)", h1)
	}
	if h2 != 10 && h2 != 5 && h2 != 101 {
		t.Logf("h2 = %d (may or may not be reused)", h2)
	}
}

// TestPreparedStatementCache tests the prepared statement cache.
func TestPreparedStatementCache(t *testing.T) {
	cache := tds.NewPreparedStatementCache(nil)
	ctx := context.Background()

	// Prepare without executor returns handle but no columns
	handle, cols, err := cache.Prepare(ctx, "SELECT @p1 + @p2", "@p1 int, @p2 int")
	if err != nil {
		t.Fatalf("Prepare failed: %v", err)
	}
	if handle != 1 {
		t.Errorf("expected handle 1, got %d", handle)
	}
	if cols != nil {
		t.Errorf("expected nil columns without executor")
	}

	// Get statement
	stmt, ok := cache.GetStatement(handle)
	if !ok {
		t.Fatal("GetStatement returned false")
	}
	if stmt.SQL != "SELECT @p1 + @p2" {
		t.Errorf("SQL = %q, want %q", stmt.SQL, "SELECT @p1 + @p2")
	}
	if stmt.ParamCount != 2 {
		t.Errorf("ParamCount = %d, want 2", stmt.ParamCount)
	}

	// Unprepare
	err = cache.Unprepare(ctx, handle)
	if err != nil {
		t.Fatalf("Unprepare failed: %v", err)
	}

	// Should not exist anymore
	_, ok = cache.GetStatement(handle)
	if ok {
		t.Error("expected statement to be removed")
	}

	// Unprepare again should error
	err = cache.Unprepare(ctx, handle)
	if err == nil {
		t.Error("expected error for double unprepare")
	}
}

// TestCursorCache tests the cursor cache.
func TestCursorCache(t *testing.T) {
	cache := tds.NewCursorCache(nil)
	ctx := context.Background()

	// Open cursor
	handle, rowCount, cols, err := cache.Open(ctx, "SELECT * FROM Users", tds.ScrollOptStatic, tds.CCOptReadOnly)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if handle != 1 {
		t.Errorf("expected handle 1, got %d", handle)
	}
	if rowCount != -1 {
		t.Errorf("expected rowCount -1 without executor, got %d", rowCount)
	}
	if cols != nil {
		t.Errorf("expected nil columns without executor")
	}

	// Get cursor
	cursor, ok := cache.GetCursor(handle)
	if !ok {
		t.Fatal("GetCursor returned false")
	}
	if cursor.SQL != "SELECT * FROM Users" {
		t.Errorf("SQL = %q, want %q", cursor.SQL, "SELECT * FROM Users")
	}
	if cursor.ScrollOpt != tds.ScrollOptStatic {
		t.Errorf("ScrollOpt = %v, want Static", cursor.ScrollOpt)
	}

	// Close cursor
	err = cache.Close(ctx, handle)
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Should not exist anymore
	_, ok = cache.GetCursor(handle)
	if ok {
		t.Error("expected cursor to be removed")
	}
}
