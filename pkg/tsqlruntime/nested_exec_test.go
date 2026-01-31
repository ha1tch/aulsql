package tsqlruntime

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// mockResolver implements ProcedureResolver for testing
type mockResolver struct {
	procedures map[string]struct {
		source string
		params []ProcedureParam
	}
}

func newMockResolver() *mockResolver {
	return &mockResolver{
		procedures: make(map[string]struct {
			source string
			params []ProcedureParam
		}),
	}
}

func (r *mockResolver) AddProcedure(name, source string, params []ProcedureParam) {
	r.procedures[name] = struct {
		source string
		params []ProcedureParam
	}{source: source, params: params}
}

func (r *mockResolver) Resolve(ctx context.Context, name string, database string) (string, []ProcedureParam, error) {
	// Try exact match first
	if proc, ok := r.procedures[name]; ok {
		return proc.source, proc.params, nil
	}
	// Try with dbo prefix
	if proc, ok := r.procedures["dbo."+name]; ok {
		return proc.source, proc.params, nil
	}
	return "", nil, &SQLError{Message: "procedure not found: " + name}
}

func setupTestDB(t *testing.T) *sql.DB {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to create test database: %v", err)
	}
	return db
}

func TestNestedExec_Simple(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	
	// Add a simple procedure that doesn't need DB access
	resolver.AddProcedure("dbo.GetMessage", `
		CREATE PROCEDURE dbo.GetMessage
		AS
		BEGIN
			SELECT 'Hello from nested!' AS Message
		END
	`, nil)

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	// Execute SQL that calls the procedure
	result, err := interp.Execute(context.Background(), `
		EXEC dbo.GetMessage
	`, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(result.ResultSets))
	}

	if len(result.ResultSets[0].Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.ResultSets[0].Rows))
	}

	msg := result.ResultSets[0].Rows[0][0].AsString()
	if msg != "Hello from nested!" {
		t.Errorf("expected 'Hello from nested!', got '%s'", msg)
	}
}

func TestNestedExec_WithParameters(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	
	// Add a procedure with parameters
	resolver.AddProcedure("dbo.Greet", `
		CREATE PROCEDURE dbo.Greet
			@Name VARCHAR(100)
		AS
		BEGIN
			SELECT 'Hello, ' + @Name + '!' AS Greeting
		END
	`, []ProcedureParam{
		{Name: "Name", SQLType: "VARCHAR(100)", HasDefault: false},
	})

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	// Execute with named parameter
	result, err := interp.Execute(context.Background(), `
		EXEC dbo.Greet @Name = 'Claude'
	`, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 1 {
		t.Fatalf("expected 1 result set, got %d", len(result.ResultSets))
	}

	greeting := result.ResultSets[0].Rows[0][0].AsString()
	if greeting != "Hello, Claude!" {
		t.Errorf("expected 'Hello, Claude!', got '%s'", greeting)
	}
}

func TestNestedExec_ThreeLevelNesting(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	
	// Level 3: Base procedure
	resolver.AddProcedure("dbo.Level3", `
		CREATE PROCEDURE dbo.Level3
		AS
		BEGIN
			SELECT 'Level 3' AS Level
		END
	`, nil)

	// Level 2: Calls Level3
	resolver.AddProcedure("dbo.Level2", `
		CREATE PROCEDURE dbo.Level2
		AS
		BEGIN
			SELECT 'Level 2' AS Level
			EXEC dbo.Level3
		END
	`, nil)

	// Level 1: Calls Level2
	resolver.AddProcedure("dbo.Level1", `
		CREATE PROCEDURE dbo.Level1
		AS
		BEGIN
			SELECT 'Level 1' AS Level
			EXEC dbo.Level2
		END
	`, nil)

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(), `
		EXEC dbo.Level1
	`, nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should have 3 result sets (one from each level)
	if len(result.ResultSets) != 3 {
		t.Fatalf("expected 3 result sets, got %d", len(result.ResultSets))
	}

	// Verify order
	levels := []string{"Level 1", "Level 2", "Level 3"}
	for i, expected := range levels {
		actual := result.ResultSets[i].Rows[0][0].AsString()
		if actual != expected {
			t.Errorf("result set %d: expected '%s', got '%s'", i, expected, actual)
		}
	}
}

func TestNestedExec_NestingLimitExceeded(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	
	// Create a recursive procedure that would exceed the limit
	resolver.AddProcedure("dbo.Recursive", `
		CREATE PROCEDURE dbo.Recursive
		AS
		BEGIN
			EXEC dbo.Recursive
		END
	`, nil)

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")
	interp.SetNestingLevel(MaxNestingLevel - 1) // Start near limit

	_, err := interp.Execute(context.Background(), `
		EXEC dbo.Recursive
	`, nil)

	if err == nil {
		t.Fatal("expected nesting limit error, got nil")
	}

	// The error message will be wrapped, so just check it contains the key phrase
	if !strings.Contains(err.Error(), "maximum procedure nesting level") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNestedExec_ProcedureNotFound(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	// Don't add any procedures

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	_, err := interp.Execute(context.Background(), `
		EXEC dbo.NonExistent
	`, nil)

	if err == nil {
		t.Fatal("expected error for non-existent procedure, got nil")
	}
}
