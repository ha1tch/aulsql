package procedure

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/ha1tch/aul/pkg/log"
)

func TestHierarchicalLoader_LoadDirectory(t *testing.T) {
	// Create temporary directory structure
	tmpDir, err := os.MkdirTemp("", "aul-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create hierarchical structure:
	// procedures/
	// ├── _global/
	// │   └── dbo/
	// │       └── GetServerInfo.sql
	// ├── master/
	// │   └── dbo/
	// │       └── sp_who.sql
	// └── salesdb/
	//     ├── dbo/
	//     │   └── GetCustomer.sql
	//     └── reporting/
	//         └── MonthlySales.sql

	dirs := []string{
		filepath.Join(tmpDir, "_global", "dbo"),
		filepath.Join(tmpDir, "master", "dbo"),
		filepath.Join(tmpDir, "salesdb", "dbo"),
		filepath.Join(tmpDir, "salesdb", "reporting"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	// Create procedure files
	files := map[string]string{
		filepath.Join(tmpDir, "_global", "dbo", "GetServerInfo.sql"): `
CREATE PROCEDURE dbo.GetServerInfo
AS
BEGIN
    SELECT @@VERSION AS ServerVersion
END
`,
		filepath.Join(tmpDir, "master", "dbo", "sp_who.sql"): `
CREATE PROCEDURE dbo.sp_who
AS
BEGIN
    SELECT 1 AS spid, 'sa' AS loginame
END
`,
		filepath.Join(tmpDir, "salesdb", "dbo", "GetCustomer.sql"): `
CREATE PROCEDURE dbo.GetCustomer
    @CustomerID INT
AS
BEGIN
    SELECT @CustomerID AS ID, 'Test Customer' AS Name
END
`,
		filepath.Join(tmpDir, "salesdb", "reporting", "MonthlySales.sql"): `
CREATE PROCEDURE reporting.MonthlySales
    @Year INT,
    @Month INT
AS
BEGIN
    SELECT @Year AS Year, @Month AS Month, 1000.00 AS Total
END
`,
	}

	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write file %s: %v", path, err)
		}
	}

	// Create loader
	logger := log.New(log.Config{DefaultLevel: log.LevelDebug})
	loader := NewHierarchicalLoader("tsql", logger)

	// Load directory
	result, err := loader.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("LoadDirectory failed: %v", err)
	}

	// Verify counts
	if result.TotalFiles != 4 {
		t.Errorf("expected 4 total files, got %d", result.TotalFiles)
	}
	if result.SuccessCount != 4 {
		t.Errorf("expected 4 successful, got %d", result.SuccessCount)
	}
	if result.FailCount != 0 {
		t.Errorf("expected 0 failures, got %d", result.FailCount)
	}
	if len(result.GlobalProcs) != 1 {
		t.Errorf("expected 1 global proc, got %d", len(result.GlobalProcs))
	}

	// Verify global procedure
	if len(result.GlobalProcs) > 0 {
		global := result.GlobalProcs[0]
		if global.Name != "GetServerInfo" {
			t.Errorf("expected global proc name 'GetServerInfo', got '%s'", global.Name)
		}
		if !global.IsGlobal {
			t.Error("expected global proc to have IsGlobal=true")
		}
		if global.Database != "" {
			t.Errorf("expected global proc database to be empty, got '%s'", global.Database)
		}
	}

	// Verify database procedures
	if len(result.ByDatabase["master"]) != 1 {
		t.Errorf("expected 1 master proc, got %d", len(result.ByDatabase["master"]))
	}
	if len(result.ByDatabase["salesdb"]) != 2 {
		t.Errorf("expected 2 salesdb procs, got %d", len(result.ByDatabase["salesdb"]))
	}

	// Verify qualified names
	for _, proc := range result.Procedures {
		t.Logf("Loaded: %s (db=%s, schema=%s, global=%v)",
			proc.QualifiedName(), proc.Database, proc.Schema, proc.IsGlobal)
	}

	// Check specific procedure
	found := false
	for _, proc := range result.ByDatabase["salesdb"] {
		if proc.Name == "MonthlySales" {
			found = true
			expectedQName := "salesdb.reporting.MonthlySales"
			if proc.QualifiedName() != expectedQName {
				t.Errorf("expected qualified name '%s', got '%s'", expectedQName, proc.QualifiedName())
			}
			if proc.Schema != "reporting" {
				t.Errorf("expected schema 'reporting', got '%s'", proc.Schema)
			}
		}
	}
	if !found {
		t.Error("MonthlySales procedure not found")
	}
}

func TestHierarchicalLoader_SchemaValidation(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create structure with mismatched schema
	dir := filepath.Join(tmpDir, "testdb", "dbo")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatalf("failed to create dir: %v", err)
	}

	// Procedure declares 'sales' schema but is in 'dbo' directory
	content := `
CREATE PROCEDURE sales.MismatchedProc
AS
BEGIN
    SELECT 1
END
`
	if err := os.WriteFile(filepath.Join(dir, "Mismatched.sql"), []byte(content), 0644); err != nil {
		t.Fatalf("failed to write file: %v", err)
	}

	logger := log.New(log.Config{DefaultLevel: log.LevelError}) // Suppress warnings
	loader := NewHierarchicalLoader("tsql", logger, WithSchemaValidation(true))

	result, err := loader.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("LoadDirectory failed: %v", err)
	}

	// Should have 1 error due to schema mismatch
	if result.FailCount != 1 {
		t.Errorf("expected 1 failure, got %d", result.FailCount)
	}
	if len(result.Errors) != 1 {
		t.Errorf("expected 1 error, got %d", len(result.Errors))
	}

	// Now test with validation disabled
	loaderNoValidation := NewHierarchicalLoader("tsql", logger, WithSchemaValidation(false))
	result2, err := loaderNoValidation.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("LoadDirectory failed: %v", err)
	}

	// Should succeed without validation
	if result2.FailCount != 0 {
		t.Errorf("expected 0 failures with validation disabled, got %d", result2.FailCount)
	}
}

func TestRegistry_LookupInDatabase(t *testing.T) {
	registry := NewRegistry()

	// Create test procedures
	globalProc := &Procedure{
		Name:     "GlobalHelper",
		Schema:   "dbo",
		Database: "",
		IsGlobal: true,
	}
	masterProc := &Procedure{
		Name:     "sp_who",
		Schema:   "dbo",
		Database: "master",
	}
	salesProc := &Procedure{
		Name:     "GetCustomer",
		Schema:   "dbo",
		Database: "salesdb",
	}
	salesReportProc := &Procedure{
		Name:     "MonthlySales",
		Schema:   "reporting",
		Database: "salesdb",
	}

	// Register all
	for _, proc := range []*Procedure{globalProc, masterProc, salesProc, salesReportProc} {
		if err := registry.Register(proc); err != nil {
			t.Fatalf("failed to register %s: %v", proc.QualifiedName(), err)
		}
	}

	tests := []struct {
		name     string
		lookup   string
		database string
		want     string
		wantErr  bool
	}{
		// Exact match tests
		{"exact full name", "salesdb.dbo.GetCustomer", "", "salesdb.dbo.GetCustomer", false},
		{"exact full name with reporting", "salesdb.reporting.MonthlySales", "", "salesdb.reporting.MonthlySales", false},

		// Database context tests
		{"schema.name in database", "dbo.GetCustomer", "salesdb", "salesdb.dbo.GetCustomer", false},
		{"name only in database", "GetCustomer", "salesdb", "salesdb.dbo.GetCustomer", false},
		{"schema.name in master", "dbo.sp_who", "master", "master.dbo.sp_who", false},

		// Global fallback tests
		{"global by schema.name", "dbo.GlobalHelper", "", "dbo.GlobalHelper", false},
		{"global by name only", "GlobalHelper", "", "dbo.GlobalHelper", false},
		{"global from database context", "GlobalHelper", "salesdb", "dbo.GlobalHelper", false},

		// Not found tests
		{"not found", "NonExistent", "", "", true},
		{"not found in database", "NonExistent", "salesdb", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc, err := registry.LookupInDatabase(tt.lookup, tt.database)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if proc.QualifiedName() != tt.want {
				t.Errorf("got %s, want %s", proc.QualifiedName(), tt.want)
			}
		})
	}
}

func TestHierarchicalLoader_TenantProcedures(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "aul-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create hierarchical structure with tenant overrides:
	// procedures/
	// ├── _global/dbo/GlobalHelper.sql
	// ├── salesdb/dbo/GetCustomer.sql
	// └── _tenant/
	//     └── acme/
	//         └── salesdb/dbo/GetCustomer.sql  (tenant override)

	dirs := []string{
		filepath.Join(tmpDir, "_global", "dbo"),
		filepath.Join(tmpDir, "salesdb", "dbo"),
		filepath.Join(tmpDir, "_tenant", "acme", "salesdb", "dbo"),
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create dir %s: %v", dir, err)
		}
	}

	// Create procedure files
	files := map[string]string{
		filepath.Join(tmpDir, "_global", "dbo", "GlobalHelper.sql"): `
CREATE PROCEDURE dbo.GlobalHelper
AS
BEGIN
    SELECT 'Global' AS Source
END
`,
		filepath.Join(tmpDir, "salesdb", "dbo", "GetCustomer.sql"): `
CREATE PROCEDURE dbo.GetCustomer
AS
BEGIN
    SELECT 'Default' AS Source
END
`,
		filepath.Join(tmpDir, "_tenant", "acme", "salesdb", "dbo", "GetCustomer.sql"): `
CREATE PROCEDURE dbo.GetCustomer
AS
BEGIN
    SELECT 'Acme Override' AS Source
END
`,
	}

	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatalf("failed to write file %s: %v", path, err)
		}
	}

	// Create loader
	logger := log.New(log.Config{DefaultLevel: log.LevelDebug})
	loader := NewHierarchicalLoader("tsql", logger)

	// Load directory
	result, err := loader.LoadDirectory(tmpDir)
	if err != nil {
		t.Fatalf("LoadDirectory failed: %v", err)
	}

	// Verify counts
	if result.TotalFiles != 3 {
		t.Errorf("expected 3 total files, got %d", result.TotalFiles)
	}
	if len(result.GlobalProcs) != 1 {
		t.Errorf("expected 1 global proc, got %d", len(result.GlobalProcs))
	}
	if len(result.ByTenant) != 1 {
		t.Errorf("expected 1 tenant, got %d", len(result.ByTenant))
	}
	if len(result.ByTenant["acme"]) != 1 {
		t.Errorf("expected 1 proc for acme tenant, got %d", len(result.ByTenant["acme"]))
	}

	// Verify tenant procedure has correct Tenant field
	acmeProcs := result.ByTenant["acme"]
	if len(acmeProcs) > 0 {
		if acmeProcs[0].Tenant != "acme" {
			t.Errorf("expected tenant 'acme', got '%s'", acmeProcs[0].Tenant)
		}
		t.Logf("Tenant procedure: %s (tenant=%s)", acmeProcs[0].QualifiedName(), acmeProcs[0].Tenant)
	}
}

func TestRegistry_LookupForTenant(t *testing.T) {
	registry := NewRegistry()

	// Create test procedures
	defaultProc := &Procedure{
		Name:     "GetCustomer",
		Schema:   "dbo",
		Database: "salesdb",
		Source:   "SELECT 'Default'",
	}
	defaultProc.SourceHash = "default123"

	tenantProc := &Procedure{
		Name:     "GetCustomer",
		Schema:   "dbo",
		Database: "salesdb",
		Tenant:   "acme",
		Source:   "SELECT 'Acme'",
	}
	tenantProc.SourceHash = "acme123"

	globalProc := &Procedure{
		Name:     "GlobalHelper",
		Schema:   "dbo",
		IsGlobal: true,
		Source:   "SELECT 'Global'",
	}
	globalProc.SourceHash = "global123"

	// Register all
	if err := registry.Register(defaultProc); err != nil {
		t.Fatalf("failed to register default proc: %v", err)
	}
	if err := registry.Register(tenantProc); err != nil {
		t.Fatalf("failed to register tenant proc: %v", err)
	}
	if err := registry.Register(globalProc); err != nil {
		t.Fatalf("failed to register global proc: %v", err)
	}

	tests := []struct {
		name     string
		lookup   string
		database string
		tenant   string
		wantSrc  string
		wantErr  bool
	}{
		// Tenant override should be used when tenant is specified
		{"tenant override", "dbo.GetCustomer", "salesdb", "acme", "SELECT 'Acme'", false},
		
		// Without tenant, should get default
		{"no tenant gets default", "dbo.GetCustomer", "salesdb", "", "SELECT 'Default'", false},
		
		// Different tenant gets default (no override for beta)
		{"other tenant gets default", "dbo.GetCustomer", "salesdb", "beta", "SELECT 'Default'", false},
		
		// Global procedure should be found regardless of tenant
		{"global proc no tenant", "dbo.GlobalHelper", "", "", "SELECT 'Global'", false},
		{"global proc with tenant", "dbo.GlobalHelper", "", "acme", "SELECT 'Global'", false},
		
		// Not found
		{"not found", "dbo.NonExistent", "", "", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc, err := registry.LookupForTenant(tt.lookup, tt.database, tt.tenant)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if proc.Source != tt.wantSrc {
				t.Errorf("got source %q, want %q", proc.Source, tt.wantSrc)
			}
		})
	}
}
