package tsqlruntime

import (
	"context"
	"testing"
)

// TestMultipleResultSets_Unconditional verifies that two consecutive SELECTs
// in a procedure body produce two distinct result sets.
func TestMultipleResultSets_Unconditional(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.TwoSets", `
CREATE PROCEDURE dbo.TwoSets
AS
BEGIN
    SELECT 1 AS ID, 'first' AS Label
    SELECT 2 AS ID, 'second' AS Label
END
`, nil)

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(), "EXEC dbo.TwoSets", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 2 {
		t.Fatalf("expected 2 result sets, got %d", len(result.ResultSets))
	}

	// Verify columns differ
	if result.ResultSets[0].Rows[0][1].AsString() != "first" {
		t.Errorf("result set 1: expected 'first', got '%s'", result.ResultSets[0].Rows[0][1].AsString())
	}
	if result.ResultSets[1].Rows[0][1].AsString() != "second" {
		t.Errorf("result set 2: expected 'second', got '%s'", result.ResultSets[1].Rows[0][1].AsString())
	}
}

// TestMultipleResultSets_ConditionalIF verifies that an IF branch controls
// whether a second result set is emitted. This is the GetCustomer pattern.
func TestMultipleResultSets_ConditionalIF(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.GetCustomer", `
CREATE PROCEDURE dbo.GetCustomer
    @CustomerID INT,
    @IncludeOrders BIT = 0
AS
BEGIN
    SELECT @CustomerID AS CustomerID, 'Customer' AS Name

    IF @IncludeOrders = 1
    BEGIN
        SELECT @CustomerID AS CustomerID, 1001 AS OrderID, 99.99 AS Total
    END
END
`, []ProcedureParam{
		{Name: "CustomerID", SQLType: "INT"},
		{Name: "IncludeOrders", SQLType: "BIT", HasDefault: true, Default: "0"},
	})

	// Case 1: IncludeOrders = 0 → one result set
	t.Run("without_orders", func(t *testing.T) {
		interp := NewInterpreter(db, DialectSQLite)
		interp.SetResolver(resolver)
		interp.SetDatabase("testdb")

		result, err := interp.Execute(context.Background(), "EXEC dbo.GetCustomer 5, 0", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result.ResultSets) != 1 {
			t.Fatalf("expected 1 result set, got %d", len(result.ResultSets))
		}
	})

	// Case 2: IncludeOrders = 1 → two result sets
	t.Run("with_orders", func(t *testing.T) {
		interp := NewInterpreter(db, DialectSQLite)
		interp.SetResolver(resolver)
		interp.SetDatabase("testdb")

		result, err := interp.Execute(context.Background(), "EXEC dbo.GetCustomer 5, 1", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result.ResultSets) != 2 {
			t.Fatalf("expected 2 result sets, got %d", len(result.ResultSets))
		}
		// Verify second result set has order data
		if len(result.ResultSets[1].Columns) != 3 {
			t.Errorf("expected 3 columns in orders result set, got %d", len(result.ResultSets[1].Columns))
		}
	})

	// Case 3: default value (IncludeOrders omitted) → one result set
	t.Run("default_param", func(t *testing.T) {
		interp := NewInterpreter(db, DialectSQLite)
		interp.SetResolver(resolver)
		interp.SetDatabase("testdb")

		result, err := interp.Execute(context.Background(), "EXEC dbo.GetCustomer 5", nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(result.ResultSets) != 1 {
			t.Fatalf("expected 1 result set (default param), got %d", len(result.ResultSets))
		}
	})
}

// TestMultipleResultSets_ThreeSets verifies that three unconditional SELECTs
// produce three result sets in order.
func TestMultipleResultSets_ThreeSets(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.ThreeSets", `
CREATE PROCEDURE dbo.ThreeSets
AS
BEGIN
    SELECT 'alpha' AS Val
    SELECT 'beta' AS Val
    SELECT 'gamma' AS Val
END
`, nil)

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(), "EXEC dbo.ThreeSets", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 3 {
		t.Fatalf("expected 3 result sets, got %d", len(result.ResultSets))
	}

	expected := []string{"alpha", "beta", "gamma"}
	for i, exp := range expected {
		actual := result.ResultSets[i].Rows[0][0].AsString()
		if actual != exp {
			t.Errorf("result set %d: expected '%s', got '%s'", i+1, exp, actual)
		}
	}
}

// TestMultipleResultSets_DifferentColumnShapes verifies that result sets
// can have different column counts and types.
func TestMultipleResultSets_DifferentColumnShapes(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.MixedShapes", `
CREATE PROCEDURE dbo.MixedShapes
AS
BEGIN
    SELECT 1 AS ID, 'Alice' AS Name, 30 AS Age
    SELECT 100 AS OrderID, 49.99 AS Amount
END
`, nil)

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(), "EXEC dbo.MixedShapes", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 2 {
		t.Fatalf("expected 2 result sets, got %d", len(result.ResultSets))
	}

	if len(result.ResultSets[0].Columns) != 3 {
		t.Errorf("result set 1: expected 3 columns, got %d", len(result.ResultSets[0].Columns))
	}
	if len(result.ResultSets[1].Columns) != 2 {
		t.Errorf("result set 2: expected 2 columns, got %d", len(result.ResultSets[1].Columns))
	}
}

// TestMultipleResultSets_NoResultSets verifies that a procedure with only
// non-SELECT statements returns zero result sets.
func TestMultipleResultSets_NoResultSets(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.NoResults", `
CREATE PROCEDURE dbo.NoResults
    @Val INT
AS
BEGIN
    DECLARE @X INT
    SET @X = @Val + 1
END
`, []ProcedureParam{
		{Name: "Val", SQLType: "INT"},
	})

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(), "EXEC dbo.NoResults 42", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 0 {
		t.Errorf("expected 0 result sets, got %d", len(result.ResultSets))
	}
}

// TestMultipleResultSets_WhileLoop verifies that result sets produced inside
// a WHILE loop accumulate correctly.
func TestMultipleResultSets_WhileLoop(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.LoopSets", `
CREATE PROCEDURE dbo.LoopSets
    @Count INT
AS
BEGIN
    DECLARE @I INT
    SET @I = 1
    WHILE @I <= @Count
    BEGIN
        SELECT @I AS Iteration
        SET @I = @I + 1
    END
END
`, []ProcedureParam{
		{Name: "Count", SQLType: "INT"},
	})

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(), "EXEC dbo.LoopSets 4", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 4 {
		t.Fatalf("expected 4 result sets, got %d", len(result.ResultSets))
	}

	for i := 0; i < 4; i++ {
		val := result.ResultSets[i].Rows[0][0].AsInt()
		if val != int64(i+1) {
			t.Errorf("result set %d: expected iteration %d, got %d", i+1, i+1, val)
		}
	}
}

// TestMultipleResultSets_NamedParams verifies that multiple result sets work
// with named parameter syntax (EXEC proc @param = value).
func TestMultipleResultSets_NamedParams(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	resolver := newMockResolver()
	resolver.AddProcedure("dbo.GetCustomer", `
CREATE PROCEDURE dbo.GetCustomer
    @CustomerID INT,
    @IncludeOrders BIT = 0
AS
BEGIN
    SELECT @CustomerID AS CustomerID

    IF @IncludeOrders = 1
    BEGIN
        SELECT @CustomerID AS CustomerID, 1001 AS OrderID
    END
END
`, []ProcedureParam{
		{Name: "CustomerID", SQLType: "INT"},
		{Name: "IncludeOrders", SQLType: "BIT", HasDefault: true, Default: "0"},
	})

	interp := NewInterpreter(db, DialectSQLite)
	interp.SetResolver(resolver)
	interp.SetDatabase("testdb")

	result, err := interp.Execute(context.Background(),
		"EXEC dbo.GetCustomer @CustomerID = 7, @IncludeOrders = 1", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(result.ResultSets) != 2 {
		t.Fatalf("expected 2 result sets with named params, got %d", len(result.ResultSets))
	}

	custID := result.ResultSets[0].Rows[0][0].AsInt()
	if custID != 7 {
		t.Errorf("expected CustomerID 7, got %d", custID)
	}
}
