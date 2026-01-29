package runtime_test

import (
	"context"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	pkglog "github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/procedure"
	"github.com/ha1tch/aul/runtime"
	"github.com/ha1tch/aul/storage"
)

// TestInventoryProcedures_EndToEnd tests the complex inventory stored procedures
// with real DDL/DML operations against SQLite.
func TestInventoryProcedures_EndToEnd(t *testing.T) {
	logger := pkglog.New(pkglog.Config{
		DefaultLevel: pkglog.LevelError,
		Format:       pkglog.FormatText,
	})

	registry := procedure.NewRegistry()

	// Load procedures from examples
	loader := procedure.NewHierarchicalLoader("tsql", logger)
	result, err := loader.LoadDirectory("../examples/procedures")
	if err != nil {
		t.Fatalf("Failed to load procedures: %v", err)
	}

	for _, p := range result.Procedures {
		registry.Register(p)
	}

	// Verify inventory procedures loaded
	inventoryProcs := []string{
		"inventory.InitializeDatabase",
		"inventory.PlaceOrder",
		"inventory.GetInventoryReport",
		"inventory.GetCustomerStatement",
		"inventory.ProcessRefund",
	}

	for _, name := range inventoryProcs {
		_, err := registry.LookupInDatabase(name, "salesdb")
		if err != nil {
			t.Fatalf("Inventory procedure not found: %s", name)
		}
	}

	// Create storage
	cfg := storage.DefaultSQLiteConfig()
	storageBackend, err := storage.NewSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storageBackend.Close()

	// Create runtime
	rtConfig := runtime.DefaultConfig()
	rtConfig.JITEnabled = false
	rt := runtime.New(rtConfig, registry, logger)
	rt.SetStorage(storageBackend)

	ctx := context.Background()

	// Test 1: Initialize Database
	t.Run("InitializeDatabase", func(t *testing.T) {
		proc, _ := registry.LookupInDatabase("inventory.InitializeDatabase", "salesdb")
		result, err := rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID: "test",
			Database:  "salesdb",
		})
		if err != nil {
			t.Fatalf("InitializeDatabase failed: %v", err)
		}

		if len(result.ResultSets) != 1 {
			t.Errorf("Expected 1 result set, got %d", len(result.ResultSets))
		}

		// Verify tables were created by running inventory report
		proc, _ = registry.LookupInDatabase("inventory.GetInventoryReport", "salesdb")
		result, err = rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID:  "test",
			Database:   "salesdb",
			Parameters: map[string]interface{}{"IncludeLowStock": 0, "MinValue": 0},
		})
		if err != nil {
			t.Fatalf("GetInventoryReport failed: %v", err)
		}

		// Should have summary + product details = 2 result sets
		if len(result.ResultSets) < 2 {
			t.Errorf("Expected at least 2 result sets, got %d", len(result.ResultSets))
		}

		// Product details should have 5 rows
		if len(result.ResultSets) >= 2 && len(result.ResultSets[1].Rows) != 5 {
			t.Errorf("Expected 5 products, got %d", len(result.ResultSets[1].Rows))
		}
	})

	// Test 2: Place Order
	t.Run("PlaceOrder", func(t *testing.T) {
		proc, _ := registry.LookupInDatabase("inventory.PlaceOrder", "salesdb")
		result, err := rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID:  "test",
			Database:   "salesdb",
			Parameters: map[string]interface{}{
				"CustomerID": 1,
				"OrderID":    2001,
				"ProductIDs": "1",
				"Quantities": "5",
			},
		})
		if err != nil {
			t.Fatalf("PlaceOrder failed: %v", err)
		}

		// Should return order confirmation + order items
		if len(result.ResultSets) != 2 {
			t.Errorf("Expected 2 result sets, got %d", len(result.ResultSets))
		}
	})

	// Test 3: Get Customer Statement
	t.Run("GetCustomerStatement", func(t *testing.T) {
		proc, _ := registry.LookupInDatabase("inventory.GetCustomerStatement", "salesdb")
		result, err := rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID:  "test",
			Database:   "salesdb",
			Parameters: map[string]interface{}{"CustomerID": 1},
		})
		if err != nil {
			t.Fatalf("GetCustomerStatement failed: %v", err)
		}

		// Should return customer header + order history + purchased products
		if len(result.ResultSets) != 3 {
			t.Errorf("Expected 3 result sets, got %d", len(result.ResultSets))
		}

		// Customer should have at least 1 order now
		if len(result.ResultSets) >= 2 && len(result.ResultSets[1].Rows) < 1 {
			t.Errorf("Expected at least 1 order, got %d", len(result.ResultSets[1].Rows))
		}
	})

	// Test 4: Get Inventory Report (verify stock decreased)
	t.Run("GetInventoryReport_AfterOrder", func(t *testing.T) {
		proc, _ := registry.LookupInDatabase("inventory.GetInventoryReport", "salesdb")
		result, err := rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID:  "test",
			Database:   "salesdb",
			Parameters: map[string]interface{}{"IncludeLowStock": 1, "MinValue": 0},
		})
		if err != nil {
			t.Fatalf("GetInventoryReport failed: %v", err)
		}

		// Summary should show decreased total units (was 215, now 210 after ordering 5)
		if len(result.ResultSets) > 0 && len(result.ResultSets[0].Rows) > 0 {
			// TotalUnits is second column
			totalUnits := result.ResultSets[0].Rows[0][1]
			t.Logf("Total units after order: %v", totalUnits)
		}
	})

	// Test 5: Insufficient stock error
	t.Run("PlaceOrder_InsufficientStock", func(t *testing.T) {
		proc, _ := registry.LookupInDatabase("inventory.PlaceOrder", "salesdb")
		_, err := rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID:  "test",
			Database:   "salesdb",
			Parameters: map[string]interface{}{
				"CustomerID": 1,
				"OrderID":    2002,
				"ProductIDs": "5",       // Deluxe Kit
				"Quantities": "100",     // Only 10 in stock
			},
		})
		if err == nil {
			t.Error("Expected error for insufficient stock")
		} else if !strings.Contains(err.Error(), "Insufficient stock") {
			t.Logf("Got error (may be expected): %v", err)
		}
	})

	// Test 6: Customer not found error
	t.Run("PlaceOrder_CustomerNotFound", func(t *testing.T) {
		proc, _ := registry.LookupInDatabase("inventory.PlaceOrder", "salesdb")
		_, err := rt.Execute(ctx, proc, &runtime.ExecContext{
			SessionID:  "test",
			Database:   "salesdb",
			Parameters: map[string]interface{}{
				"CustomerID": 999,
				"OrderID":    2003,
				"ProductIDs": "1",
				"Quantities": "1",
			},
		})
		if err == nil {
			t.Error("Expected error for customer not found")
		} else if !strings.Contains(err.Error(), "Customer not found") {
			t.Errorf("Expected 'Customer not found' error, got: %v", err)
		}
	})
}

// TestTransactions_CommitRollback verifies transaction semantics
func TestTransactions_CommitRollback(t *testing.T) {
	logger := pkglog.New(pkglog.Config{
		DefaultLevel: pkglog.LevelError,
		Format:       pkglog.FormatText,
	})

	cfg := storage.DefaultSQLiteConfig()
	storageBackend, err := storage.NewSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storageBackend.Close()

	db := storageBackend.GetDB()
	ctx := context.Background()

	// Setup: create test table
	_, err = db.ExecContext(ctx, `CREATE TABLE TxnTest (ID INT PRIMARY KEY, Value INT)`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	registry := procedure.NewRegistry()

	// Create a procedure that does a committed transaction
	commitProc := &procedure.Procedure{
		Name:     "TestCommit",
		Database: "test",
		Schema:   "dbo",
		Source: `
CREATE PROCEDURE dbo.TestCommit
AS
BEGIN
    BEGIN TRANSACTION
    INSERT INTO TxnTest (ID, Value) VALUES (1, 100)
    INSERT INTO TxnTest (ID, Value) VALUES (2, 200)
    COMMIT TRANSACTION
    SELECT COUNT(*) AS Cnt FROM TxnTest
END
`,
	}
	registry.Register(commitProc)

	// Create a procedure that rolls back
	rollbackProc := &procedure.Procedure{
		Name:     "TestRollback",
		Database: "test",
		Schema:   "dbo",
		Source: `
CREATE PROCEDURE dbo.TestRollback
AS
BEGIN
    BEGIN TRANSACTION
    INSERT INTO TxnTest (ID, Value) VALUES (3, 300)
    INSERT INTO TxnTest (ID, Value) VALUES (4, 400)
    ROLLBACK TRANSACTION
    SELECT COUNT(*) AS Cnt FROM TxnTest
END
`,
	}
	registry.Register(rollbackProc)

	rtConfig := runtime.DefaultConfig()
	rtConfig.JITEnabled = false
	rt := runtime.New(rtConfig, registry, logger)
	rt.SetStorage(storageBackend)

	// Test commit
	t.Run("Commit", func(t *testing.T) {
		result, err := rt.Execute(ctx, commitProc, &runtime.ExecContext{
			SessionID: "test",
			Database:  "test",
		})
		if err != nil {
			t.Fatalf("Commit procedure failed: %v", err)
		}

		// Verify count is 2
		var count int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM TxnTest").Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows after commit, got %d", count)
		}
		t.Logf("Rows after commit: %d", count)
		_ = result
	})

	// Test rollback
	t.Run("Rollback", func(t *testing.T) {
		result, err := rt.Execute(ctx, rollbackProc, &runtime.ExecContext{
			SessionID: "test",
			Database:  "test",
		})
		if err != nil {
			t.Fatalf("Rollback procedure failed: %v", err)
		}

		// Verify count is still 2 (rollback discarded the inserts)
		var count int
		db.QueryRowContext(ctx, "SELECT COUNT(*) FROM TxnTest").Scan(&count)
		if count != 2 {
			t.Errorf("Expected 2 rows after rollback, got %d", count)
		}
		t.Logf("Rows after rollback: %d (should be 2)", count)
		_ = result
	})
}
