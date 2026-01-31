package storage

import (
	"context"
	"os"
	"testing"

	"github.com/ha1tch/aul/pkg/runtime"
)

// TestGlobalProcedureWithTenantContext verifies that when using tenant-aware storage,
// the same "global" query can access different tenant databases.
func TestGlobalProcedureWithTenantContext(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-global-proc-test-*")
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

	// Set up identical table structure in two different tenants
	tenants := []string{"alpha", "beta"}
	for _, tenant := range tenants {
		_, err = storage.ExecForTenant(ctx, tenant, "appdb",
			"CREATE TABLE config (key TEXT PRIMARY KEY, value TEXT)")
		if err != nil {
			t.Fatalf("failed to create table for %s: %v", tenant, err)
		}

		// Insert tenant-specific data
		_, err = storage.ExecForTenant(ctx, tenant, "appdb",
			"INSERT INTO config (key, value) VALUES ('tenant_name', ?)", tenant)
		if err != nil {
			t.Fatalf("failed to insert for %s: %v", tenant, err)
		}
	}

	// Simulate a "global procedure" - same SQL executed for different tenants
	globalSQL := "SELECT value FROM config WHERE key = 'tenant_name'"

	// Execute for tenant alpha
	resultsAlpha, err := storage.QueryForTenant(ctx, "alpha", "appdb", globalSQL)
	if err != nil {
		t.Fatalf("failed to query for alpha: %v", err)
	}

	// Execute for tenant beta
	resultsBeta, err := storage.QueryForTenant(ctx, "beta", "appdb", globalSQL)
	if err != nil {
		t.Fatalf("failed to query for beta: %v", err)
	}

	// Verify alpha sees "alpha"
	if len(resultsAlpha) == 0 || len(resultsAlpha[0].Rows) == 0 {
		t.Fatal("no results for alpha")
	}
	alphaValue, ok := resultsAlpha[0].Rows[0][0].(string)
	if !ok || alphaValue != "alpha" {
		t.Errorf("tenant alpha expected 'alpha', got %v", resultsAlpha[0].Rows[0][0])
	}

	// Verify beta sees "beta"
	if len(resultsBeta) == 0 || len(resultsBeta[0].Rows) == 0 {
		t.Fatal("no results for beta")
	}
	betaValue, ok := resultsBeta[0].Rows[0][0].(string)
	if !ok || betaValue != "beta" {
		t.Errorf("tenant beta expected 'beta', got %v", resultsBeta[0].Rows[0][0])
	}

	t.Logf("Tenant isolation verified: alpha='%s', beta='%s'", alphaValue, betaValue)
}

// TestTenantAwareStorageBackendInterface verifies TenantSQLiteStorage implements
// the TenantAwareStorageBackend interface.
func TestTenantAwareStorageBackendInterface(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-interface-test-*")
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

	// Verify it implements runtime.StorageBackend
	var _ runtime.StorageBackend = storage

	// Verify it implements runtime.TenantAwareStorageBackend
	var _ runtime.TenantAwareStorageBackend = storage

	// Test GetDBForTenant
	ctx := context.Background()

	// Should auto-create and return a valid DB
	db, err := storage.GetDBForTenant("test_tenant", "test_db")
	if err != nil {
		t.Fatalf("GetDBForTenant failed: %v", err)
	}
	if db == nil {
		t.Fatal("GetDBForTenant returned nil")
	}

	// Verify we can use it
	err = db.PingContext(ctx)
	if err != nil {
		t.Errorf("ping failed: %v", err)
	}
}

// TestDefaultTenantFallback verifies that methods without explicit tenant
// use the configured default tenant.
func TestDefaultTenantFallback(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-default-tenant-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	cfg := DefaultTenantSQLiteConfig()
	cfg.BaseDir = tmpDir
	cfg.DefaultTenant = "_default"

	storage, err := NewTenantSQLiteStorage(cfg)
	if err != nil {
		t.Fatalf("failed to create storage: %v", err)
	}
	defer storage.Close()

	ctx := context.Background()

	// Use non-tenant-aware methods (should use default tenant)
	_, err = storage.Exec(ctx, "CREATE TABLE test (id INTEGER PRIMARY KEY)")
	if err != nil {
		t.Fatalf("Exec failed: %v", err)
	}

	_, err = storage.Exec(ctx, "INSERT INTO test (id) VALUES (1)")
	if err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	results, err := storage.Query(ctx, "SELECT id FROM test")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		t.Fatal("no results")
	}

	// Verify same data accessible via explicit default tenant
	resultsExplicit, err := storage.QueryForTenant(ctx, "_default", "master", "SELECT id FROM test")
	if err != nil {
		t.Fatalf("QueryForTenant failed: %v", err)
	}

	if len(resultsExplicit) == 0 || len(resultsExplicit[0].Rows) == 0 {
		t.Fatal("no results via explicit default tenant")
	}

	t.Log("Default tenant fallback working correctly")
}
