package storage

import (
	"context"
	"testing"
	"time"

	"github.com/ha1tch/aul/pkg/procedure"
)

func TestSystemCatalog_IsSystemQuery(t *testing.T) {
	sc := NewSystemCatalog(nil)

	tests := []struct {
		sql      string
		expected bool
	}{
		{"SELECT * FROM sys.tables", true},
		{"SELECT * FROM sys.procedures", true},
		{"SELECT * FROM sys.schemas", true},
		{"SELECT name FROM sys.objects WHERE type = 'P'", true},
		{"SELECT * FROM sys.columns", true},
		{"SELECT * FROM sys.types", true},
		{"SELECT * FROM sys.databases", true},
		{"SELECT * FROM INFORMATION_SCHEMA.TABLES", true},
		{"SELECT * FROM Customers", false},
		{"INSERT INTO Orders VALUES (1)", false},
		{"EXEC dbo.GetCustomer @ID = 1", false},
	}

	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) {
			result := sc.IsSystemQuery(tc.sql)
			if result != tc.expected {
				t.Errorf("IsSystemQuery(%q) = %v, want %v", tc.sql, result, tc.expected)
			}
		})
	}
}

func TestSystemCatalog_QuerySchemas(t *testing.T) {
	sc := NewSystemCatalog(nil)

	storage, err := NewInMemorySQLiteStorage()
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()
	results, err := sc.ExecuteSystemQuery(ctx, storage, "SELECT * FROM sys.schemas")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(results))
	}

	rs := results[0]
	if len(rs.Columns) < 2 {
		t.Fatalf("expected at least 2 columns, got %d", len(rs.Columns))
	}

	// Should have at least dbo, guest, sys, INFORMATION_SCHEMA
	if len(rs.Rows) < 4 {
		t.Errorf("expected at least 4 schemas, got %d", len(rs.Rows))
	}

	// Check for dbo schema
	foundDbo := false
	for _, row := range rs.Rows {
		if name, ok := row[0].(string); ok && name == "dbo" {
			foundDbo = true
			break
		}
	}
	if !foundDbo {
		t.Error("expected to find 'dbo' schema")
	}
}

func TestSystemCatalog_QueryProcedures(t *testing.T) {
	// Create a registry with a test procedure
	registry := procedure.NewRegistry()
	proc := &procedure.Procedure{
		Name:     "TestProc",
		Schema:   "dbo",
		Database: "testdb",
		Source:   "CREATE PROCEDURE dbo.TestProc AS SELECT 1",
		LoadedAt: time.Now(),
	}
	if err := registry.Register(proc); err != nil {
		t.Fatalf("failed to register procedure: %v", err)
	}

	sc := NewSystemCatalog(registry)

	storage, err := NewInMemorySQLiteStorage()
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()
	results, err := sc.ExecuteSystemQuery(ctx, storage, "SELECT * FROM sys.procedures")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(results))
	}

	rs := results[0]
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 procedure, got %d", len(rs.Rows))
	}

	// Check procedure name
	if name, ok := rs.Rows[0][0].(string); !ok || name != "TestProc" {
		t.Errorf("expected procedure name 'TestProc', got %v", rs.Rows[0][0])
	}
}

func TestSystemCatalog_QueryTables(t *testing.T) {
	storage, err := NewInMemorySQLiteStorage()
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Create a test table
	_, err = storage.Exec(ctx, "CREATE TABLE Customers (ID INTEGER PRIMARY KEY, Name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	sc := NewSystemCatalog(nil)
	results, err := sc.ExecuteSystemQuery(ctx, storage, "SELECT * FROM sys.tables")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(results))
	}

	rs := results[0]
	if len(rs.Rows) != 1 {
		t.Fatalf("expected 1 table, got %d", len(rs.Rows))
	}

	// Check table name
	if name, ok := rs.Rows[0][0].(string); !ok || name != "Customers" {
		t.Errorf("expected table name 'Customers', got %v", rs.Rows[0][0])
	}
}

func TestSystemCatalog_QueryTypes(t *testing.T) {
	sc := NewSystemCatalog(nil)

	storage, err := NewInMemorySQLiteStorage()
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()
	results, err := sc.ExecuteSystemQuery(ctx, storage, "SELECT * FROM sys.types")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(results))
	}

	rs := results[0]
	// Should have many standard types
	if len(rs.Rows) < 20 {
		t.Errorf("expected at least 20 types, got %d", len(rs.Rows))
	}

	// Check for common types
	typeNames := make(map[string]bool)
	for _, row := range rs.Rows {
		if name, ok := row[0].(string); ok {
			typeNames[name] = true
		}
	}

	required := []string{"int", "varchar", "nvarchar", "datetime", "bit"}
	for _, req := range required {
		if !typeNames[req] {
			t.Errorf("expected to find type '%s'", req)
		}
	}
}

func TestSQLiteStorage_SystemCatalogIntegration(t *testing.T) {
	// Create storage
	storage, err := NewInMemorySQLiteStorage()
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	// Create a registry
	registry := procedure.NewRegistry()
	proc := &procedure.Procedure{
		Name:     "GetCustomer",
		Schema:   "dbo",
		Database: "salesdb",
		Source:   "CREATE PROCEDURE dbo.GetCustomer @ID INT AS SELECT * FROM Customers WHERE ID = @ID",
		LoadedAt: time.Now(),
	}
	registry.Register(proc)

	// Set registry on storage
	storage.SetRegistry(registry)

	ctx := context.Background()

	// Create a table
	_, err = storage.Exec(ctx, "CREATE TABLE Customers (ID INTEGER PRIMARY KEY, Name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Query sys.tables - should be intercepted
	results, err := storage.Query(ctx, "SELECT * FROM sys.tables")
	if err != nil {
		t.Fatalf("sys.tables query failed: %v", err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 {
		t.Errorf("expected 1 table in sys.tables, got %d result sets", len(results))
	}

	// Query sys.procedures - should be intercepted
	results, err = storage.Query(ctx, "SELECT * FROM sys.procedures")
	if err != nil {
		t.Fatalf("sys.procedures query failed: %v", err)
	}
	if len(results) != 1 || len(results[0].Rows) != 1 {
		t.Errorf("expected 1 procedure in sys.procedures, got %d result sets", len(results))
	}

	// Regular query - should pass through
	results, err = storage.Query(ctx, "SELECT * FROM Customers")
	if err != nil {
		t.Fatalf("regular query failed: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("expected 1 result set from regular query")
	}
}
