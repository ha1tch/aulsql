package tds

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/runtime"
	"github.com/ha1tch/aul/server"
)

// TestFullCycleWithSQLite tests a complete request/response cycle using
// go-mssqldb client -> TDS protocol -> aul server -> SQLite backend
func TestFullCycleWithSQLite(t *testing.T) {
	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Create server with SQLite backend
	logger := log.New(log.Config{
		DefaultLevel: log.LevelDebug,
		Format:       log.FormatText,
	})

	cfg := server.DefaultConfig()
	cfg.Logger = logger
	cfg.ProcedureDir = "" // No procedures needed for this test
	cfg.JITEnabled = false
	cfg.StorageConfig = runtime.StorageConfig{
		Type:    "sqlite",
		Options: map[string]string{"path": ":memory:"},
	}
	cfg.Listeners = []protocol.ListenerConfig{
		{
			Name:     "tds-test",
			Protocol: protocol.ProtocolTDS,
			Port:     port,
		},
	}

	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()

	t.Logf("Server started on port %d", port)

	// Give server a moment to fully start
	time.Sleep(100 * time.Millisecond)

	// Connect with go-mssqldb
	connStr := fmt.Sprintf("sqlserver://sa:password@127.0.0.1:%d?database=master&encrypt=disable&connection+timeout=5", port)
	
	db, err := sql.Open("sqlserver", connStr)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	// Set a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Test 1: Simple SELECT 1
	t.Run("SELECT_1", func(t *testing.T) {
		var result int
		err := db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
		if err != nil {
			t.Fatalf("SELECT 1 failed: %v", err)
		}
		if result != 1 {
			t.Errorf("Expected 1, got %d", result)
		}
		t.Logf("SELECT 1 returned: %d", result)
	})

	// Test 2: SELECT with arithmetic
	t.Run("SELECT_arithmetic", func(t *testing.T) {
		var result int
		err := db.QueryRowContext(ctx, "SELECT 2 + 3").Scan(&result)
		if err != nil {
			t.Fatalf("SELECT 2 + 3 failed: %v", err)
		}
		if result != 5 {
			t.Errorf("Expected 5, got %d", result)
		}
		t.Logf("SELECT 2 + 3 returned: %d", result)
	})

	// Test 3: SELECT multiple columns
	t.Run("SELECT_multiple_columns", func(t *testing.T) {
		var a, b, c int
		err := db.QueryRowContext(ctx, "SELECT 1, 2, 3").Scan(&a, &b, &c)
		if err != nil {
			t.Fatalf("SELECT 1, 2, 3 failed: %v", err)
		}
		if a != 1 || b != 2 || c != 3 {
			t.Errorf("Expected 1,2,3 got %d,%d,%d", a, b, c)
		}
		t.Logf("SELECT 1, 2, 3 returned: %d, %d, %d", a, b, c)
	})

	// Test 4: SELECT with string
	t.Run("SELECT_string", func(t *testing.T) {
		var result string
		err := db.QueryRowContext(ctx, "SELECT 'hello'").Scan(&result)
		if err != nil {
			t.Fatalf("SELECT 'hello' failed: %v", err)
		}
		if result != "hello" {
			t.Errorf("Expected 'hello', got '%s'", result)
		}
		t.Logf("SELECT 'hello' returned: %s", result)
	})

	// Test 5: T-SQL GETDATE() -> SQLite datetime('now')
	t.Run("GETDATE", func(t *testing.T) {
		var result string
		err := db.QueryRowContext(ctx, "SELECT GETDATE()").Scan(&result)
		if err != nil {
			t.Fatalf("SELECT GETDATE() failed: %v", err)
		}
		// Should return a datetime string
		if len(result) < 10 {
			t.Errorf("Expected datetime, got '%s'", result)
		}
		t.Logf("SELECT GETDATE() returned: %s", result)
	})

	// Test 6: T-SQL ISNULL() -> SQLite IFNULL()
	t.Run("ISNULL", func(t *testing.T) {
		var result string
		err := db.QueryRowContext(ctx, "SELECT ISNULL(NULL, 'default')").Scan(&result)
		if err != nil {
			t.Fatalf("SELECT ISNULL(NULL, 'default') failed: %v", err)
		}
		if result != "default" {
			t.Errorf("Expected 'default', got '%s'", result)
		}
		t.Logf("SELECT ISNULL(NULL, 'default') returned: %s", result)
	})

	// Test 7: T-SQL LEN() -> SQLite LENGTH()
	t.Run("LEN", func(t *testing.T) {
		var result int
		err := db.QueryRowContext(ctx, "SELECT LEN('hello')").Scan(&result)
		if err != nil {
			t.Fatalf("SELECT LEN('hello') failed: %v", err)
		}
		if result != 5 {
			t.Errorf("Expected 5, got %d", result)
		}
		t.Logf("SELECT LEN('hello') returned: %d", result)
	})
}
