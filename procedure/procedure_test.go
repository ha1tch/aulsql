package procedure_test

import (
	"context"
	"testing"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/procedure"
	"github.com/ha1tch/aul/runtime"
	"github.com/ha1tch/aul/storage"
)

func TestSimpleProcedureExecution(t *testing.T) {
	// Create logger
	logger := log.New(log.Config{
		DefaultLevel: log.LevelDebug,
		Format:       log.FormatText,
	})

	// Create procedure registry
	registry := procedure.NewRegistry()

	// Load procedure from file
	loader := procedure.NewLoader("tsql", logger)
	proc, err := loader.LoadFile("../examples/procedures/HelloWorld.sql")
	if err != nil {
		t.Fatalf("Failed to load procedure: %v", err)
	}

	t.Logf("Loaded procedure: %s", proc.QualifiedName())
	t.Logf("  Source:\n%s", proc.Source)
	t.Logf("  Parameters: %d", len(proc.Parameters))

	// Register the procedure
	if err := registry.Register(proc); err != nil {
		t.Fatalf("Failed to register procedure: %v", err)
	}

	// Create storage backend (SQLite in-memory)
	storageBackend, err := storage.NewSQLiteStorage(storage.DefaultSQLiteConfig())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storageBackend.Close()

	// Create runtime
	rtConfig := runtime.DefaultConfig()
	rtConfig.JITEnabled = false // Disable JIT for simplicity
	rt := runtime.New(rtConfig, registry, logger)
	rt.SetStorage(storageBackend)

	// Execute the procedure
	ctx := context.Background()
	execCtx := &runtime.ExecContext{
		SessionID:  "test-session",
		Database:   "master",
		Parameters: map[string]interface{}{},
		Timeout:    0,
	}

	result, err := rt.Execute(ctx, proc, execCtx)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Check result
	t.Logf("Result: RowsAffected=%d, ResultSets=%d", result.RowsAffected, len(result.ResultSets))
	
	if len(result.ResultSets) == 0 {
		t.Fatal("Expected at least one result set")
	}

	rs := result.ResultSets[0]
	t.Logf("Columns: %v", rs.Columns)
	t.Logf("Rows: %v", rs.Rows)

	if len(rs.Rows) == 0 {
		t.Fatal("Expected at least one row")
	}

	// Check the message
	if len(rs.Rows[0]) == 0 {
		t.Fatal("Expected at least one column in first row")
	}

	msg, ok := rs.Rows[0][0].(string)
	if !ok {
		t.Fatalf("Expected string, got %T: %v", rs.Rows[0][0], rs.Rows[0][0])
	}

	if msg != "Hello, World!" {
		t.Errorf("Expected 'Hello, World!', got '%s'", msg)
	}

	t.Logf("SUCCESS: Procedure returned: %s", msg)
}

func TestProcedureWithParameters(t *testing.T) {
	// Create logger
	logger := log.New(log.Config{
		DefaultLevel: log.LevelDebug,
		Format:       log.FormatText,
	})

	// Create procedure registry
	registry := procedure.NewRegistry()

	// Load procedure from file
	loader := procedure.NewLoader("tsql", logger)
	proc, err := loader.LoadFile("../examples/procedures/Greet.sql")
	if err != nil {
		t.Fatalf("Failed to load procedure: %v", err)
	}

	t.Logf("Loaded procedure: %s", proc.QualifiedName())
	t.Logf("  Parameters: %d", len(proc.Parameters))
	for _, p := range proc.Parameters {
		t.Logf("    @%s %s (default: %v)", p.Name, p.SQLType, p.Default)
	}

	// Register the procedure
	if err := registry.Register(proc); err != nil {
		t.Fatalf("Failed to register procedure: %v", err)
	}

	// Create storage backend (SQLite in-memory)
	storageBackend, err := storage.NewSQLiteStorage(storage.DefaultSQLiteConfig())
	if err != nil {
		t.Fatalf("Failed to create storage: %v", err)
	}
	defer storageBackend.Close()

	// Create runtime
	rtConfig := runtime.DefaultConfig()
	rtConfig.JITEnabled = false
	rt := runtime.New(rtConfig, registry, logger)
	rt.SetStorage(storageBackend)

	// Execute with parameter
	ctx := context.Background()
	execCtx := &runtime.ExecContext{
		SessionID:  "test-session",
		Database:   "master",
		Parameters: map[string]interface{}{
			"Name": "Claude",
		},
		Timeout: 0,
	}

	result, err := rt.Execute(ctx, proc, execCtx)
	if err != nil {
		t.Fatalf("Execution failed: %v", err)
	}

	// Check result
	if len(result.ResultSets) == 0 || len(result.ResultSets[0].Rows) == 0 {
		t.Fatal("Expected result set with rows")
	}

	msg := result.ResultSets[0].Rows[0][0]
	t.Logf("Result: %v", msg)

	expected := "Hello, Claude!"
	if msg != expected {
		t.Errorf("Expected '%s', got '%v'", expected, msg)
	}
}

func TestProcedureAnnotationExtraction(t *testing.T) {
	source := `-- @aul:jit-threshold=50
-- @aul:timeout=5s
-- @aul:log-params
CREATE PROCEDURE dbo.usp_AnnotatedProc
    @Input VARCHAR(100)
AS
BEGIN
    SELECT @Input AS Result
END`

	parser := &procedure.TSQLParser{}
	proc, err := parser.Parse(source)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Check procedure was parsed
	if proc.Name != "usp_AnnotatedProc" {
		t.Errorf("expected name 'usp_AnnotatedProc', got '%s'", proc.Name)
	}

	// Check annotations were extracted
	if proc.Annotations == nil {
		t.Fatal("Annotations is nil")
	}

	// Check specific annotations
	if v, ok := proc.Annotations["jit-threshold"]; !ok || v != "50" {
		t.Errorf("expected jit-threshold=50, got %q", v)
	}

	if v, ok := proc.Annotations["timeout"]; !ok || v != "5s" {
		t.Errorf("expected timeout=5s, got %q", v)
	}

	if _, ok := proc.Annotations["log-params"]; !ok {
		t.Error("expected log-params annotation")
	}

	t.Logf("Annotations: %v", proc.Annotations)
}

func TestProcedureAnnotationWithFunction(t *testing.T) {
	source := `-- @aul:no-jit
-- @aul:deprecated
CREATE FUNCTION dbo.fn_OldFunction(@X INT)
RETURNS INT
AS
BEGIN
    RETURN @X * 2
END`

	parser := &procedure.TSQLParser{}
	proc, err := parser.Parse(source)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if !proc.IsFunction {
		t.Error("expected IsFunction=true")
	}

	if proc.Name != "fn_OldFunction" {
		t.Errorf("expected name 'fn_OldFunction', got '%s'", proc.Name)
	}

	// Check annotations
	if _, ok := proc.Annotations["no-jit"]; !ok {
		t.Error("expected no-jit annotation")
	}

	if _, ok := proc.Annotations["deprecated"]; !ok {
		t.Error("expected deprecated annotation")
	}
}

func TestProcedureNoAnnotations(t *testing.T) {
	source := `CREATE PROCEDURE dbo.usp_Simple
AS
BEGIN
    SELECT 1
END`

	parser := &procedure.TSQLParser{}
	proc, err := parser.Parse(source)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Annotations should be empty but not nil
	if proc.Annotations == nil {
		t.Fatal("Annotations should not be nil")
	}

	if len(proc.Annotations) != 0 {
		t.Errorf("expected 0 annotations, got %d", len(proc.Annotations))
	}
}

func TestProcedureAnnotationWithBlankLine(t *testing.T) {
	source := `-- @aul:isolated

CREATE PROCEDURE dbo.usp_Test
AS
BEGIN
    SELECT 1
END`

	parser := &procedure.TSQLParser{}
	proc, err := parser.Parse(source)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Blank line should break annotation association
	if len(proc.Annotations) != 0 {
		t.Errorf("expected 0 annotations (blank line breaks), got %d: %v", 
			len(proc.Annotations), proc.Annotations)
	}
}
