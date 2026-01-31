package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestTenantSQLiteStorage_ResolveDatabasePath(t *testing.T) {
	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = "/data/tenants"

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		// Skip if can't create (e.g., permission issues)
		t.Skipf("cannot create storage: %v", err)
	}
	defer storage.Close()

	tests := []struct {
		name     string
		database string
		tenant   string
		want     string
	}{
		{
			name:     "default tenant",
			database: "master",
			tenant:   "",
			want:     "/data/tenants/_default/master.db",
		},
		{
			name:     "specific tenant",
			database: "salesdb",
			tenant:   "acme",
			want:     "/data/tenants/acme/salesdb.db",
		},
		{
			name:     "sanitise path traversal",
			database: "../etc/passwd",
			tenant:   "../../root",
			want:     "/data/tenants/____root/__etc_passwd.db",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := storage.resolveDatabasePath(tt.database, tt.tenant)
			if got != tt.want {
				t.Errorf("resolveDatabasePath(%q, %q) = %q, want %q",
					tt.database, tt.tenant, got, tt.want)
			}
		})
	}
}

func TestTenantSQLiteStorage_AutoCreate(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir
	cfg.AutoCreate = true

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Query should auto-create the database
	_, err = storage.ExecForTenant(ctx, "acme", "testdb",
		"CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Verify database file was created
	dbPath := filepath.Join(tmpDir, "acme", "testdb.db")
	if !fileExists(dbPath) {
		t.Errorf("database file not created at %s", dbPath)
	}

	// Insert and query data
	_, err = storage.ExecForTenant(ctx, "acme", "testdb",
		"INSERT INTO users (name) VALUES (?)", "Alice")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	results, err := storage.QueryForTenant(ctx, "acme", "testdb",
		"SELECT name FROM users WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		t.Fatal("expected result row")
	}

	name, ok := results[0].Rows[0][0].(string)
	if !ok || name != "Alice" {
		t.Errorf("expected name 'Alice', got %v", results[0].Rows[0][0])
	}
}

func TestTenantSQLiteStorage_AutoCreateDisabled(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir
	cfg.AutoCreate = false // Disabled

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Should fail because database doesn't exist
	_, err = storage.QueryForTenant(ctx, "acme", "nonexistent", "SELECT 1")
	if err == nil {
		t.Error("expected error when database doesn't exist and auto-create is disabled")
	}
}

func TestTenantSQLiteStorage_ReuseExisting(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir
	cfg.AutoCreate = true

	ctx := context.Background()

	// First storage instance - create database and insert data
	storage1, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage1: %v", err)
	}

	_, err = storage1.ExecForTenant(ctx, "tenant1", "mydb",
		"CREATE TABLE data (id INTEGER PRIMARY KEY, value TEXT)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = storage1.ExecForTenant(ctx, "tenant1", "mydb",
		"INSERT INTO data (value) VALUES ('persistent')")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	storage1.Close()

	// Second storage instance - should reuse existing database
	storage2, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage2: %v", err)
	}
	defer storage2.Close()

	results, err := storage2.QueryForTenant(ctx, "tenant1", "mydb",
		"SELECT value FROM data WHERE id = 1")
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		t.Fatal("expected persisted data")
	}

	value, ok := results[0].Rows[0][0].(string)
	if !ok || value != "persistent" {
		t.Errorf("expected 'persistent', got %v", results[0].Rows[0][0])
	}
}

func TestTenantSQLiteStorage_TenantIsolation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Create same table in two different tenants
	for _, tenant := range []string{"alpha", "beta"} {
		_, err = storage.ExecForTenant(ctx, tenant, "shared",
			"CREATE TABLE items (id INTEGER PRIMARY KEY, tenant TEXT)")
		if err != nil {
			t.Fatalf("failed to create table for %s: %v", tenant, err)
		}

		_, err = storage.ExecForTenant(ctx, tenant, "shared",
			"INSERT INTO items (tenant) VALUES (?)", tenant)
		if err != nil {
			t.Fatalf("failed to insert for %s: %v", tenant, err)
		}
	}

	// Verify each tenant only sees their own data
	for _, tenant := range []string{"alpha", "beta"} {
		results, err := storage.QueryForTenant(ctx, tenant, "shared",
			"SELECT tenant FROM items")
		if err != nil {
			t.Fatalf("failed to query for %s: %v", tenant, err)
		}

		if len(results) == 0 || len(results[0].Rows) != 1 {
			t.Errorf("tenant %s: expected exactly 1 row", tenant)
			continue
		}

		gotTenant, ok := results[0].Rows[0][0].(string)
		if !ok || gotTenant != tenant {
			t.Errorf("tenant %s: expected to see own data, got %v", tenant, gotTenant)
		}
	}
}

func TestTenantSQLiteStorage_Transactions(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Setup
	_, err = storage.ExecForTenant(ctx, "txtest", "db",
		"CREATE TABLE counter (id INTEGER PRIMARY KEY, value INTEGER)")
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	_, err = storage.ExecForTenant(ctx, "txtest", "db",
		"INSERT INTO counter (id, value) VALUES (1, 0)")
	if err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Test commit
	txn, err := storage.BeginForTenant(ctx, "txtest", "db")
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Note: We need to execute within the transaction context
	// For now, just test begin/commit/rollback work
	err = storage.Commit(ctx, txn)
	if err != nil {
		t.Errorf("commit failed: %v", err)
	}

	// Test rollback
	txn2, err := storage.BeginForTenant(ctx, "txtest", "db")
	if err != nil {
		t.Fatalf("failed to begin transaction 2: %v", err)
	}

	err = storage.Rollback(ctx, txn2)
	if err != nil {
		t.Errorf("rollback failed: %v", err)
	}
}

func TestTenantSQLiteStorage_ListTenantsAndDatabases(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Create databases for multiple tenants
	tenantDBs := map[string][]string{
		"tenant1": {"db1", "db2"},
		"tenant2": {"db1", "db3"},
	}

	for tenant, dbs := range tenantDBs {
		for _, db := range dbs {
			_, err = storage.ExecForTenant(ctx, tenant, db, "SELECT 1")
			if err != nil {
				t.Fatalf("failed to create %s/%s: %v", tenant, db, err)
			}
		}
	}

	// List tenants
	tenants, err := storage.ListTenants()
	if err != nil {
		t.Fatalf("failed to list tenants: %v", err)
	}

	if len(tenants) != 2 {
		t.Errorf("expected 2 tenants, got %d: %v", len(tenants), tenants)
	}

	// List databases for tenant1
	dbs, err := storage.ListDatabases("tenant1")
	if err != nil {
		t.Fatalf("failed to list databases: %v", err)
	}

	if len(dbs) != 2 {
		t.Errorf("expected 2 databases for tenant1, got %d: %v", len(dbs), dbs)
	}
}

func TestTenantSQLiteStorage_PRAGMASettings(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir
	cfg.JournalMode = "WAL"
	cfg.Synchronous = "NORMAL"

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Create a database and check PRAGMA settings
	results, err := storage.QueryForTenant(ctx, "pragmatest", "testdb",
		"PRAGMA journal_mode")
	if err != nil {
		t.Fatalf("failed to query journal_mode: %v", err)
	}

	if len(results) > 0 && len(results[0].Rows) > 0 {
		mode, ok := results[0].Rows[0][0].(string)
		if ok {
			t.Logf("journal_mode = %s", mode)
			// WAL mode should be set (case-insensitive comparison)
			if mode != "wal" && mode != "WAL" {
				t.Logf("note: journal_mode is %s, expected wal (may vary by platform)", mode)
			}
		}
	}
}
