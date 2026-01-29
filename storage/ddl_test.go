package storage

import (
	"testing"

	"github.com/ha1tch/aul/pkg/annotations"
)

func TestDDLParser_ParseCreateTable(t *testing.T) {
	ddl := `-- @aul:isolated
-- @aul:journal-mode=WAL
-- @aul:cache-size=5000
CREATE TABLE dbo.AuditLog (
    ID INT PRIMARY KEY IDENTITY,
    Action VARCHAR(50) NOT NULL,
    Details NVARCHAR(MAX),
    Timestamp DATETIME DEFAULT GETDATE()
)`

	parser := NewDDLParser()
	meta, err := parser.ParseCreateTable(ddl, "testdb")
	if err != nil {
		t.Fatalf("ParseCreateTable failed: %v", err)
	}

	// Check basic info
	if meta.Database != "testdb" {
		t.Errorf("expected database 'testdb', got '%s'", meta.Database)
	}
	if meta.Schema != "dbo" {
		t.Errorf("expected schema 'dbo', got '%s'", meta.Schema)
	}
	if meta.Name != "AuditLog" {
		t.Errorf("expected name 'AuditLog', got '%s'", meta.Name)
	}

	// Check annotations
	if !meta.IsIsolated() {
		t.Error("expected table to be isolated")
	}
	if meta.JournalMode("") != "WAL" {
		t.Errorf("expected journal-mode=WAL, got '%s'", meta.JournalMode(""))
	}
	if meta.CacheSize(0) != 5000 {
		t.Errorf("expected cache-size=5000, got %d", meta.CacheSize(0))
	}

	// Check columns
	if len(meta.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(meta.Columns))
	}

	// Check ID column
	idCol := meta.Columns[0]
	if idCol.Name != "ID" {
		t.Errorf("expected first column 'ID', got '%s'", idCol.Name)
	}
	if idCol.IsPrimary != true {
		t.Error("expected ID to be primary key")
	}
	if idCol.IsIdentity != true {
		t.Error("expected ID to be identity")
	}

	// Check Action column
	actionCol := meta.Columns[1]
	if actionCol.Nullable {
		t.Error("expected Action to be NOT NULL")
	}

	t.Logf("Table: %s, Annotations: %v, Columns: %d",
		meta.QualifiedName(), meta.Annotations, len(meta.Columns))
}

func TestDDLParser_NoAnnotations(t *testing.T) {
	ddl := `CREATE TABLE Users (
    ID INT PRIMARY KEY,
    Name VARCHAR(100)
)`

	parser := NewDDLParser()
	meta, err := parser.ParseCreateTable(ddl, "")
	if err != nil {
		t.Fatalf("ParseCreateTable failed: %v", err)
	}

	if meta.IsIsolated() {
		t.Error("expected table to NOT be isolated")
	}

	if len(meta.Annotations) != 0 {
		t.Errorf("expected 0 annotations, got %d", len(meta.Annotations))
	}

	if meta.Name != "Users" {
		t.Errorf("expected name 'Users', got '%s'", meta.Name)
	}
}

func TestDDLParser_ReadOnlyAnnotation(t *testing.T) {
	ddl := `-- @aul:read-only
CREATE TABLE Config (
    Key VARCHAR(100) PRIMARY KEY,
    Value VARCHAR(500)
)`

	parser := NewDDLParser()
	meta, err := parser.ParseCreateTable(ddl, "master")
	if err != nil {
		t.Fatalf("ParseCreateTable failed: %v", err)
	}

	if !meta.IsReadOnly() {
		t.Error("expected table to be read-only")
	}
}

func TestMetadataCatalogue_RegisterAndGet(t *testing.T) {
	cat := NewMetadataCatalogue()

	meta := &TableMetadata{
		Database:    "testdb",
		Schema:      "dbo",
		Name:        "Users",
		Annotations: annotations.AnnotationSet{"isolated": ""},
	}

	cat.RegisterTable(meta)

	// Retrieve
	got, ok := cat.GetTable("testdb", "dbo", "Users")
	if !ok {
		t.Fatal("table not found")
	}

	if got.QualifiedName() != "testdb.dbo.Users" {
		t.Errorf("expected 'testdb.dbo.Users', got '%s'", got.QualifiedName())
	}

	if !got.IsIsolated() {
		t.Error("expected isolated annotation")
	}
}

func TestMetadataCatalogue_IsIsolated(t *testing.T) {
	cat := NewMetadataCatalogue()

	// Register isolated table
	cat.RegisterTable(&TableMetadata{
		Database:    "db1",
		Schema:      "dbo",
		Name:        "IsolatedTable",
		Annotations: annotations.AnnotationSet{"isolated": ""},
	})

	// Register non-isolated table
	cat.RegisterTable(&TableMetadata{
		Database:    "db1",
		Schema:      "dbo",
		Name:        "NormalTable",
		Annotations: make(annotations.AnnotationSet),
	})

	if !cat.IsIsolated("db1", "dbo", "IsolatedTable") {
		t.Error("IsolatedTable should be isolated")
	}

	if cat.IsIsolated("db1", "dbo", "NormalTable") {
		t.Error("NormalTable should NOT be isolated")
	}

	if cat.IsIsolated("db1", "dbo", "NonExistent") {
		t.Error("NonExistent should NOT be isolated")
	}
}

func TestMetadataCatalogue_ListIsolatedTables(t *testing.T) {
	cat := NewMetadataCatalogue()

	// Register mix of tables
	cat.RegisterTable(&TableMetadata{
		Database:    "db1",
		Schema:      "dbo",
		Name:        "AuditLog",
		Annotations: annotations.AnnotationSet{"isolated": ""},
	})
	cat.RegisterTable(&TableMetadata{
		Database:    "db1",
		Schema:      "dbo",
		Name:        "Cache",
		Annotations: annotations.AnnotationSet{"isolated": "", "cache-size": "10000"},
	})
	cat.RegisterTable(&TableMetadata{
		Database:    "db1",
		Schema:      "dbo",
		Name:        "Users",
		Annotations: make(annotations.AnnotationSet),
	})

	isolated := cat.ListIsolatedTables()

	if len(isolated) != 2 {
		t.Errorf("expected 2 isolated tables, got %d", len(isolated))
	}
}

func TestMetadataCatalogue_UnregisterTable(t *testing.T) {
	cat := NewMetadataCatalogue()

	cat.RegisterTable(&TableMetadata{
		Database: "db1",
		Schema:   "dbo",
		Name:     "ToDelete",
	})

	// Verify registered
	_, ok := cat.GetTable("db1", "dbo", "ToDelete")
	if !ok {
		t.Fatal("table should be registered")
	}

	// Unregister
	cat.UnregisterTable("db1", "dbo", "ToDelete")

	// Verify removed
	_, ok = cat.GetTable("db1", "dbo", "ToDelete")
	if ok {
		t.Error("table should be unregistered")
	}
}

func TestParseTableName(t *testing.T) {
	tests := []struct {
		ddl        string
		wantSchema string
		wantName   string
	}{
		{"CREATE TABLE Users (ID INT)", "dbo", "Users"},
		{"CREATE TABLE dbo.Users (ID INT)", "dbo", "Users"},
		{"CREATE TABLE [schema].[TableName] (ID INT)", "schema", "TableName"},
		{"CREATE TABLE IF NOT EXISTS Config (ID INT)", "dbo", "Config"},
		{"CREATE TEMP TABLE #TempData (ID INT)", "dbo", "#TempData"},
	}

	for _, tt := range tests {
		schema, name := parseTableName(tt.ddl)
		if schema != tt.wantSchema || name != tt.wantName {
			t.Errorf("parseTableName(%q) = (%q, %q), want (%q, %q)",
				tt.ddl, schema, name, tt.wantSchema, tt.wantName)
		}
	}
}

func TestTableMetadata_QualifiedName(t *testing.T) {
	tests := []struct {
		meta *TableMetadata
		want string
	}{
		{&TableMetadata{Database: "db", Schema: "sch", Name: "tbl"}, "db.sch.tbl"},
		{&TableMetadata{Schema: "dbo", Name: "Users"}, "dbo.Users"},
		{&TableMetadata{Name: "Simple"}, "Simple"},
	}

	for _, tt := range tests {
		got := tt.meta.QualifiedName()
		if got != tt.want {
			t.Errorf("QualifiedName() = %q, want %q", got, tt.want)
		}
	}
}
