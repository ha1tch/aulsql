package storage

import (
	"context"
	"database/sql"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/ha1tch/aul/pkg/annotations"
)

func TestExtractTableNames(t *testing.T) {
	tests := []struct {
		query string
		want  []string
	}{
		{
			query: "SELECT * FROM Users",
			want:  []string{"Users"},
		},
		{
			query: "SELECT * FROM dbo.Users",
			want:  []string{"dbo.Users"},
		},
		{
			query: "SELECT * FROM mydb.dbo.Users",
			want:  []string{"mydb.dbo.Users"},
		},
		{
			query: "SELECT * FROM [dbo].[Users]",
			want:  []string{"dbo.Users"},
		},
		{
			query: "SELECT * FROM Users u JOIN Orders o ON u.ID = o.UserID",
			want:  []string{"Users", "Orders"},
		},
		{
			query: "INSERT INTO AuditLog (Action) VALUES ('test')",
			want:  []string{"AuditLog"},
		},
		{
			query: "UPDATE Users SET Name = 'test' WHERE ID = 1",
			want:  []string{"Users"},
		},
		{
			query: "DELETE FROM TempData WHERE ID = 1",
			want:  []string{"TempData"},
		},
		{
			query: "SELECT 1", // No tables
			want:  []string{},
		},
		{
			query: "SELECT u.Name, o.Total FROM Users u INNER JOIN Orders o ON u.ID = o.UserID LEFT JOIN Products p ON o.ProductID = p.ID",
			want:  []string{"Users", "Orders", "Products"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			refs := extractTableNames(tt.query)
			got := tableNames(refs)

			if len(got) != len(tt.want) {
				t.Errorf("extractTableNames(%q) = %v, want %v", tt.query, got, tt.want)
				return
			}

			// Check each table (order may vary)
			wantSet := make(map[string]bool)
			for _, w := range tt.want {
				wantSet[w] = true
			}

			for _, g := range got {
				if !wantSet[g] {
					t.Errorf("unexpected table %q in result for query %q", g, tt.query)
				}
			}
		})
	}
}

func TestParseTableRef(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantDB   string
		wantSch  string
		wantName string
	}{
		{"simple", "Users", "", "", "Users"},
		{"schema.table", "dbo.Users", "", "dbo", "Users"},
		{"full", "mydb.dbo.Users", "mydb", "dbo", "Users"},
		{"brackets", "[dbo].[Users]", "", "dbo", "Users"},
		{"full brackets", "[mydb].[dbo].[Users]", "mydb", "dbo", "Users"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ref := parseTableRef(tt.input)
			if ref.database != tt.wantDB || ref.schema != tt.wantSch || ref.name != tt.wantName {
				t.Errorf("parseTableRef(%q) = {%q, %q, %q}, want {%q, %q, %q}",
					tt.input, ref.database, ref.schema, ref.name,
					tt.wantDB, tt.wantSch, tt.wantName)
			}
		})
	}
}

func TestStorageRouter_RouteToMain(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-router-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database (in-memory)
	mainDB, err := createTestDB()
	if err != nil {
		t.Fatalf("failed to create main db: %v", err)
	}
	defer mainDB.Close()

	// Create isolated manager (empty - no isolated tables)
	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir
	isolatedMgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create isolated manager: %v", err)
	}
	defer isolatedMgr.Close()

	router := NewStorageRouter(mainDB, isolatedMgr, nil)

	// Query should route to main
	db, err := router.RouteQuery(context.Background(), "SELECT * FROM Users")
	if err != nil {
		t.Fatalf("RouteQuery failed: %v", err)
	}

	if db != mainDB {
		t.Error("expected query to route to main database")
	}
}

func TestStorageRouter_RouteToIsolated(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-router-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database
	mainDB, err := createTestDB()
	if err != nil {
		t.Fatalf("failed to create main db: %v", err)
	}
	defer mainDB.Close()

	// Create isolated manager with one isolated table
	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir
	isolatedMgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create isolated manager: %v", err)
	}
	defer isolatedMgr.Close()

	ctx := context.Background()

	// Create isolated table
	ddl := "CREATE TABLE AuditLog (ID INTEGER PRIMARY KEY, Action TEXT)"
	ann := annotations.AnnotationSet{"isolated": ""}
	err = isolatedMgr.CreateTable(ctx, "master", "dbo", "AuditLog", ddl, ann)
	if err != nil {
		t.Fatalf("failed to create isolated table: %v", err)
	}

	router := NewStorageRouter(mainDB, isolatedMgr, nil)
	router.SetDefaultDatabase("master")

	// Query to isolated table should route there
	db, err := router.RouteQuery(ctx, "SELECT * FROM dbo.AuditLog")
	if err != nil {
		t.Fatalf("RouteQuery failed: %v", err)
	}

	if db == mainDB {
		t.Error("expected query to route to isolated database, got main")
	}

	// Verify it's the correct isolated connection
	isolatedDB, _ := isolatedMgr.GetConnection("master", "dbo", "AuditLog")
	if db != isolatedDB {
		t.Error("query routed to wrong database")
	}
}

func TestStorageRouter_CrossDatabaseError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-router-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database
	mainDB, err := createTestDB()
	if err != nil {
		t.Fatalf("failed to create main db: %v", err)
	}
	defer mainDB.Close()

	// Create isolated manager
	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir
	isolatedMgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create isolated manager: %v", err)
	}
	defer isolatedMgr.Close()

	ctx := context.Background()

	// Create isolated table
	ddl := "CREATE TABLE IsolatedData (ID INTEGER PRIMARY KEY)"
	ann := annotations.AnnotationSet{"isolated": ""}
	err = isolatedMgr.CreateTable(ctx, "master", "dbo", "IsolatedData", ddl, ann)
	if err != nil {
		t.Fatalf("failed to create isolated table: %v", err)
	}

	router := NewStorageRouter(mainDB, isolatedMgr, nil)
	router.SetDefaultDatabase("master")

	// Query spanning isolated and main tables should fail
	_, err = router.RouteQuery(ctx, 
		"SELECT * FROM IsolatedData i JOIN Users u ON i.ID = u.ID")
	if err == nil {
		t.Error("expected error for cross-database query")
	}

	t.Logf("Got expected error: %v", err)
}

func TestStorageRouter_MultipleIsolatedTablesError(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-router-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database
	mainDB, err := createTestDB()
	if err != nil {
		t.Fatalf("failed to create main db: %v", err)
	}
	defer mainDB.Close()

	// Create isolated manager
	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir
	isolatedMgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create isolated manager: %v", err)
	}
	defer isolatedMgr.Close()

	ctx := context.Background()
	ann := annotations.AnnotationSet{"isolated": ""}

	// Create two different isolated tables
	err = isolatedMgr.CreateTable(ctx, "master", "dbo", "TableA", 
		"CREATE TABLE TableA (ID INTEGER PRIMARY KEY)", ann)
	if err != nil {
		t.Fatalf("failed to create TableA: %v", err)
	}

	err = isolatedMgr.CreateTable(ctx, "master", "dbo", "TableB", 
		"CREATE TABLE TableB (ID INTEGER PRIMARY KEY)", ann)
	if err != nil {
		t.Fatalf("failed to create TableB: %v", err)
	}

	router := NewStorageRouter(mainDB, isolatedMgr, nil)
	router.SetDefaultDatabase("master")

	// Query spanning two different isolated tables should fail
	_, err = router.RouteQuery(ctx, 
		"SELECT * FROM TableA a JOIN TableB b ON a.ID = b.ID")
	if err == nil {
		t.Error("expected error for query spanning multiple isolated tables")
	}

	t.Logf("Got expected error: %v", err)
}

func TestStorageRouter_SameIsolatedTableMultipleRefs(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-router-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database
	mainDB, err := createTestDB()
	if err != nil {
		t.Fatalf("failed to create main db: %v", err)
	}
	defer mainDB.Close()

	// Create isolated manager
	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir
	isolatedMgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create isolated manager: %v", err)
	}
	defer isolatedMgr.Close()

	ctx := context.Background()

	// Create isolated table
	ddl := "CREATE TABLE SelfJoin (ID INTEGER PRIMARY KEY, ParentID INTEGER)"
	ann := annotations.AnnotationSet{"isolated": ""}
	err = isolatedMgr.CreateTable(ctx, "master", "dbo", "SelfJoin", ddl, ann)
	if err != nil {
		t.Fatalf("failed to create isolated table: %v", err)
	}

	router := NewStorageRouter(mainDB, isolatedMgr, nil)
	router.SetDefaultDatabase("master")

	// Self-join on same isolated table should work
	db, err := router.RouteQuery(ctx, 
		"SELECT * FROM SelfJoin a JOIN SelfJoin b ON a.ID = b.ParentID")
	if err != nil {
		t.Fatalf("RouteQuery failed for self-join: %v", err)
	}

	// Should route to the isolated table
	if db == mainDB {
		t.Error("self-join should route to isolated database")
	}
}

func TestStorageRouter_ExecuteAndQuery(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "aul-router-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create main database with a table
	mainDB, err := createTestDB()
	if err != nil {
		t.Fatalf("failed to create main db: %v", err)
	}
	defer mainDB.Close()

	_, err = mainDB.Exec("CREATE TABLE MainTable (ID INTEGER PRIMARY KEY, Data TEXT)")
	if err != nil {
		t.Fatalf("failed to create main table: %v", err)
	}

	// Create isolated manager
	cfg := DefaultIsolatedTableConfig()
	cfg.BaseDir = tmpDir
	isolatedMgr, err := NewIsolatedTableManager(cfg)
	if err != nil {
		t.Fatalf("failed to create isolated manager: %v", err)
	}
	defer isolatedMgr.Close()

	router := NewStorageRouter(mainDB, isolatedMgr, nil)

	ctx := context.Background()

	// Execute insert on main table
	result, err := router.Execute(ctx, "INSERT INTO MainTable (Data) VALUES (?)", "test")
	if err != nil {
		t.Fatalf("Execute failed: %v", err)
	}

	rows, _ := result.RowsAffected()
	if rows != 1 {
		t.Errorf("expected 1 row affected, got %d", rows)
	}

	// Query main table
	dbRows, err := router.Query(ctx, "SELECT Data FROM MainTable WHERE ID = 1")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer dbRows.Close()

	if !dbRows.Next() {
		t.Fatal("expected result row")
	}

	var data string
	dbRows.Scan(&data)
	if data != "test" {
		t.Errorf("expected 'test', got '%s'", data)
	}
}

// Helper to create a test database
func createTestDB() (*sql.DB, error) {
	return sql.Open("sqlite3", ":memory:")
}
