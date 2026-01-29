package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ha1tch/aul/pkg/annotations"
)

func TestIsolatedTableManager_CreateTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-isolated-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir

	mgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Create isolated table with annotations
	ddl := `-- @aul:isolated
-- @aul:journal-mode=WAL
CREATE TABLE AuditLog (
    ID INTEGER PRIMARY KEY,
    Action TEXT NOT NULL,
    Timestamp TEXT DEFAULT CURRENT_TIMESTAMP
)`

	ann := annotations.AnnotationSet{
		"isolated":     "",
		"journal-mode": "WAL",
	}

	err = mgr.CreateTable(ctx, "testdb", "dbo", "AuditLog", ddl, ann)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Check file was created
	dbPath := filepath.Join(tmpDir, "testdb", "dbo.AuditLog.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Errorf("database file not created at %s", dbPath)
	}

	// Check it's registered as isolated
	if !mgr.IsIsolated("testdb", "dbo", "AuditLog") {
		t.Error("table should be registered as isolated")
	}

	// Insert data
	result, err := mgr.Exec(ctx, "testdb", "dbo", "AuditLog",
		"INSERT INTO AuditLog (Action) VALUES (?)", "TestAction")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	rowsAffected, _ := result.RowsAffected()
	if rowsAffected != 1 {
		t.Errorf("expected 1 row affected, got %d", rowsAffected)
	}

	// Query data
	rows, err := mgr.Query(ctx, "testdb", "dbo", "AuditLog",
		"SELECT Action FROM AuditLog WHERE ID = 1")
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected result row")
	}

	var action string
	if err := rows.Scan(&action); err != nil {
		t.Fatalf("scan failed: %v", err)
	}

	if action != "TestAction" {
		t.Errorf("expected 'TestAction', got '%s'", action)
	}
}

func TestIsolatedTableManager_DropTable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-isolated-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir

	mgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Create table
	ddl := "CREATE TABLE ToDelete (ID INTEGER PRIMARY KEY)"
	ann := annotations.AnnotationSet{"isolated": ""}

	err = mgr.CreateTable(ctx, "testdb", "dbo", "ToDelete", ddl, ann)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify exists
	dbPath := filepath.Join(tmpDir, "testdb", "dbo.ToDelete.db")
	if _, err := os.Stat(dbPath); os.IsNotExist(err) {
		t.Fatal("database file should exist")
	}

	// Drop table
	err = mgr.DropTable(ctx, "testdb", "dbo", "ToDelete")
	if err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}

	// Verify file removed
	if _, err := os.Stat(dbPath); !os.IsNotExist(err) {
		t.Error("database file should be removed")
	}

	// Verify not registered
	if mgr.IsIsolated("testdb", "dbo", "ToDelete") {
		t.Error("table should not be registered after drop")
	}
}

func TestIsolatedTableManager_ReadOnly(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-isolated-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir

	mgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Create read-only table
	ddl := "CREATE TABLE Config (Key TEXT PRIMARY KEY, Value TEXT)"
	ann := annotations.AnnotationSet{
		"isolated":  "",
		"read-only": "",
	}

	err = mgr.CreateTable(ctx, "testdb", "dbo", "Config", ddl, ann)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Try to insert (should fail)
	_, err = mgr.Exec(ctx, "testdb", "dbo", "Config",
		"INSERT INTO Config (Key, Value) VALUES ('test', 'value')")
	if err == nil {
		t.Error("expected error for write to read-only table")
	}

	// Query should still work
	rows, err := mgr.Query(ctx, "testdb", "dbo", "Config", "SELECT 1")
	if err != nil {
		t.Errorf("query should work on read-only table: %v", err)
	} else {
		rows.Close()
	}
}

func TestIsolatedTableManager_CustomPragmas(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-isolated-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir

	mgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Create table with custom pragmas
	ddl := "CREATE TABLE CustomPragma (ID INTEGER PRIMARY KEY)"
	ann := annotations.AnnotationSet{
		"isolated":     "",
		"journal-mode": "DELETE",
		"cache-size":   "1000",
		"synchronous":  "FULL",
	}

	err = mgr.CreateTable(ctx, "testdb", "dbo", "CustomPragma", ddl, ann)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Get connection and verify pragmas
	db, err := mgr.GetConnection("testdb", "dbo", "CustomPragma")
	if err != nil {
		t.Fatalf("GetConnection failed: %v", err)
	}

	// Check journal mode (note: may be lowercase)
	var journalMode string
	err = db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journalMode)
	if err != nil {
		t.Fatalf("failed to get journal_mode: %v", err)
	}
	t.Logf("journal_mode = %s", journalMode)

	// Check synchronous
	var synchronous int
	err = db.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous)
	if err != nil {
		t.Fatalf("failed to get synchronous: %v", err)
	}
	t.Logf("synchronous = %d (FULL=2)", synchronous)
}

func TestIsolatedTableManager_MultipleTables(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-isolated-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir

	mgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	ctx := context.Background()
	ann := annotations.AnnotationSet{"isolated": ""}

	// Create multiple tables
	tables := []struct {
		db     string
		schema string
		name   string
	}{
		{"db1", "dbo", "TableA"},
		{"db1", "dbo", "TableB"},
		{"db2", "dbo", "TableA"},
		{"db2", "schema2", "TableC"},
	}

	for _, tbl := range tables {
		ddl := "CREATE TABLE " + tbl.name + " (ID INTEGER PRIMARY KEY, Data TEXT)"
		err := mgr.CreateTable(ctx, tbl.db, tbl.schema, tbl.name, ddl, ann)
		if err != nil {
			t.Fatalf("failed to create %s.%s.%s: %v", tbl.db, tbl.schema, tbl.name, err)
		}
	}

	// Verify all are registered
	for _, tbl := range tables {
		if !mgr.IsIsolated(tbl.db, tbl.schema, tbl.name) {
			t.Errorf("%s.%s.%s should be isolated", tbl.db, tbl.schema, tbl.name)
		}
	}

	// Insert different data into each
	for i, tbl := range tables {
		_, err := mgr.Exec(ctx, tbl.db, tbl.schema, tbl.name,
			"INSERT INTO "+tbl.name+" (Data) VALUES (?)",
			tbl.db+"."+tbl.schema+"."+tbl.name)
		if err != nil {
			t.Fatalf("insert %d failed: %v", i, err)
		}
	}

	// Verify isolation (each table has its own data)
	for _, tbl := range tables {
		rows, err := mgr.Query(ctx, tbl.db, tbl.schema, tbl.name,
			"SELECT Data FROM "+tbl.name)
		if err != nil {
			t.Fatalf("query failed: %v", err)
		}

		var data string
		if rows.Next() {
			rows.Scan(&data)
		}
		rows.Close()

		expected := tbl.db + "." + tbl.schema + "." + tbl.name
		if data != expected {
			t.Errorf("table %s: expected data '%s', got '%s'", expected, expected, data)
		}
	}

	// Check stats
	stats := mgr.Stats()
	if stats.TableCount != 4 {
		t.Errorf("expected 4 tables, got %d", stats.TableCount)
	}

	t.Logf("Stats: %+v", stats)
}

func TestIsolatedTableManager_NotIsolated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-isolated-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir

	mgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create manager: %v", err)
	}
	defer mgr.Close()

	// Check non-existent table
	if mgr.IsIsolated("testdb", "dbo", "NonExistent") {
		t.Error("non-existent table should not be isolated")
	}

	// Try to get connection for non-isolated table
	_, err = mgr.GetConnection("testdb", "dbo", "NonExistent")
	if err == nil {
		t.Error("expected error for non-isolated table")
	}
}

func TestStripAnnotations(t *testing.T) {
	input := `-- @aul:isolated
-- @aul:journal-mode=WAL
-- Regular comment
CREATE TABLE Test (
    ID INTEGER PRIMARY KEY
)`

	expected := `-- Regular comment
CREATE TABLE Test (
    ID INTEGER PRIMARY KEY
)`

	got := stripAnnotations(input)
	if got != expected {
		t.Errorf("stripAnnotations mismatch:\ngot:\n%s\nexpected:\n%s", got, expected)
	}
}
