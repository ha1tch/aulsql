package tsqlruntime

import (
	"strings"
	"testing"

	"github.com/ha1tch/aul/pkg/tsqlparser/ast"
	"github.com/ha1tch/aul/pkg/tsqlparser/lexer"
	"github.com/ha1tch/aul/pkg/tsqlparser/parser"
)

// parseSQL parses T-SQL and returns the first statement
func parseSQL(t *testing.T, sql string) ast.Statement {
	l := lexer.New(sql)
	p := parser.New(l)
	program := p.ParseProgram()
	if len(p.Errors()) > 0 {
		t.Fatalf("Parse errors: %v", p.Errors())
	}
	if len(program.Statements) == 0 {
		t.Fatal("No statements parsed")
	}
	return program.Statements[0]
}

func TestSQLiteRewriter_FunctionRenames(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string // What the output should contain
		excludes string // What the output should NOT contain
	}{
		{
			name:     "ISNULL to IFNULL",
			input:    "SELECT ISNULL(col, 'default') FROM t",
			contains: "IFNULL",
			excludes: "ISNULL",
		},
		{
			name:     "LEN to LENGTH",
			input:    "SELECT LEN(name) FROM t",
			contains: "LENGTH",
			excludes: "LEN(",
		},
		{
			name:     "SUBSTRING to SUBSTR",
			input:    "SELECT SUBSTRING(name, 1, 5) FROM t",
			contains: "SUBSTR",
			excludes: "SUBSTRING",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}

func TestSQLiteRewriter_ParameterlessFunctions(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "GETDATE to datetime('now')",
			input:    "SELECT GETDATE()",
			contains: "datetime('now')",
		},
		{
			name:     "NEWID to randomblob",
			input:    "SELECT NEWID()",
			contains: "randomblob",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
		})
	}
}

func TestSQLiteRewriter_CHARINDEX(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	// CHARINDEX(needle, haystack) -> INSTR(haystack, needle)
	input := "SELECT CHARINDEX('x', name) FROM t"
	stmt := parseSQL(t, input)
	rewritten := rewriter.RewriteStatement(stmt)
	output := rewritten.String()

	// Should have INSTR with swapped arguments
	if !strings.Contains(output, "INSTR") {
		t.Errorf("Expected INSTR in output, got: %s", output)
	}
	if strings.Contains(output, "CHARINDEX") {
		t.Errorf("Expected no CHARINDEX in output, got: %s", output)
	}
	// Arguments should be swapped: INSTR(name, 'x')
	if !strings.Contains(output, "INSTR(name, 'x')") {
		t.Errorf("Expected swapped arguments INSTR(name, 'x'), got: %s", output)
	}
}

func TestSQLiteRewriter_TopToLimit(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	input := "SELECT TOP 10 id, name FROM users"
	stmt := parseSQL(t, input)
	sel := stmt.(*ast.SelectStatement)

	// Verify input has TOP
	if sel.Top == nil {
		t.Fatal("Input should have TOP clause")
	}

	rewritten := rewriter.RewriteStatement(stmt)
	selOut := rewritten.(*ast.SelectStatement)

	// After rewriting, Top should be nil and Fetch should have the count
	if selOut.Top != nil {
		t.Error("TOP should be removed after rewriting")
	}
	if selOut.Fetch == nil {
		t.Error("Fetch should be set after TOP->LIMIT conversion")
	}
}

func TestSQLiteRewriter_ConvertToCast(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	input := "SELECT CONVERT(VARCHAR(50), price) FROM products"
	stmt := parseSQL(t, input)
	rewritten := rewriter.RewriteStatement(stmt)
	output := rewritten.String()

	// Should convert CONVERT to CAST
	if !strings.Contains(output, "CAST") {
		t.Errorf("Expected CAST in output, got: %s", output)
	}
	// The CONVERT should be gone (converted to CAST expression)
	// Note: depends on how CastExpression.String() formats it
}

func TestSQLiteRewriter_TypeMappings(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "DATETIME to TEXT",
			input:    "CREATE TABLE #t (d DATETIME)",
			contains: "TEXT",
		},
		{
			name:     "NVARCHAR to TEXT",
			input:    "CREATE TABLE #t (s NVARCHAR(100))",
			contains: "TEXT",
		},
		{
			name:     "BIT to INTEGER",
			input:    "CREATE TABLE #t (b BIT)",
			contains: "INTEGER",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
		})
	}
}

func TestPostgresRewriter_FunctionRenames(t *testing.T) {
	rewriter := NewPostgresRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "ISNULL to COALESCE",
			input:    "SELECT ISNULL(col, 'default') FROM t",
			contains: "COALESCE",
		},
		{
			name:     "LEN to LENGTH",
			input:    "SELECT LEN(name) FROM t",
			contains: "LENGTH",
		},
		{
			name:     "DATALENGTH to OCTET_LENGTH",
			input:    "SELECT DATALENGTH(data) FROM t",
			contains: "OCTET_LENGTH",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
		})
	}
}

func TestMySQLRewriter_FunctionRenames(t *testing.T) {
	rewriter := NewMySQLRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
	}{
		{
			name:     "ISNULL to IFNULL",
			input:    "SELECT ISNULL(col, 'default') FROM t",
			contains: "IFNULL",
		},
		{
			name:     "LEN to CHAR_LENGTH",
			input:    "SELECT LEN(name) FROM t",
			contains: "CHAR_LENGTH",
		},
		{
			name:     "CHARINDEX to LOCATE",
			input:    "SELECT CHARINDEX('x', name) FROM t",
			contains: "LOCATE",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
		})
	}
}

func TestPassthroughRewriter(t *testing.T) {
	rewriter := &PassthroughRewriter{}

	input := "SELECT ISNULL(col, 'default'), GETDATE(), LEN(name) FROM t"
	stmt := parseSQL(t, input)
	rewritten := rewriter.RewriteStatement(stmt)
	output := rewritten.String()

	// Passthrough should not change anything
	if !strings.Contains(output, "ISNULL") {
		t.Error("Passthrough should preserve ISNULL")
	}
	if !strings.Contains(output, "GETDATE") {
		t.Error("Passthrough should preserve GETDATE")
	}
	if !strings.Contains(output, "LEN") {
		t.Error("Passthrough should preserve LEN")
	}
}

func TestRewriterRecursion(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	// Test nested function calls
	input := "SELECT ISNULL(LEN(ISNULL(name, '')), 0) FROM t"
	stmt := parseSQL(t, input)
	rewritten := rewriter.RewriteStatement(stmt)
	output := rewritten.String()

	// All ISNULL should become IFNULL
	if strings.Contains(output, "ISNULL") {
		t.Errorf("All ISNULL should be converted to IFNULL, got: %s", output)
	}

	// All LEN should become LENGTH
	if strings.Contains(output, "LEN(") {
		t.Errorf("All LEN should be converted to LENGTH, got: %s", output)
	}

	// Should have IFNULL and LENGTH
	if !strings.Contains(output, "IFNULL") {
		t.Errorf("Expected IFNULL in output, got: %s", output)
	}
	if !strings.Contains(output, "LENGTH") {
		t.Errorf("Expected LENGTH in output, got: %s", output)
	}
}

func TestRewriterSubquery(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	// Test subquery with functions
	input := "SELECT * FROM t WHERE id IN (SELECT ISNULL(parent_id, 0) FROM t2)"
	stmt := parseSQL(t, input)
	rewritten := rewriter.RewriteStatement(stmt)
	output := rewritten.String()

	// ISNULL in subquery should be converted
	if strings.Contains(output, "ISNULL") {
		t.Errorf("ISNULL in subquery should be converted, got: %s", output)
	}
	if !strings.Contains(output, "IFNULL") {
		t.Errorf("Expected IFNULL in subquery, got: %s", output)
	}
}

func TestSQLiteRewriter_DateFunctions(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "YEAR to strftime",
			input:    "SELECT YEAR(created_at) FROM t",
			contains: "strftime",
			excludes: "YEAR(",
		},
		{
			name:     "MONTH to strftime",
			input:    "SELECT MONTH(created_at) FROM t",
			contains: "strftime",
			excludes: "MONTH(",
		},
		{
			name:     "DAY to strftime",
			input:    "SELECT DAY(created_at) FROM t",
			contains: "strftime",
			excludes: "DAY(",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}

func TestSQLiteRewriter_StringFunctions(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "LEFT to SUBSTR",
			input:    "SELECT LEFT(name, 5) FROM t",
			contains: "SUBSTR",
			excludes: "LEFT(",
		},
		{
			name:     "RIGHT to SUBSTR",
			input:    "SELECT RIGHT(name, 3) FROM t",
			contains: "SUBSTR",
			excludes: "RIGHT(",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}

func TestSQLiteRewriter_MathFunctions(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "CEILING to CASE expression",
			input:    "SELECT CEILING(price) FROM t",
			contains: "CAST",
			excludes: "CEILING(",
		},
		{
			name:     "FLOOR to CASE expression",
			input:    "SELECT FLOOR(price) FROM t",
			contains: "CASE",
			excludes: "FLOOR(",
		},
		{
			name:     "SIGN to CASE expression",
			input:    "SELECT SIGN(amount) FROM t",
			contains: "CASE",
			excludes: "SIGN(",
		},
		{
			name:     "RAND to random()",
			input:    "SELECT RAND()",
			contains: "random()",
			excludes: "RAND(",
		},
		{
			name:     "POWER with small int expands",
			input:    "SELECT POWER(2, 3)",
			contains: "*",
			excludes: "POWER(",
		},
		{
			name:     "PI to constant",
			input:    "SELECT PI()",
			contains: "3.14159",
			excludes: "PI(",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}

func TestSQLiteRewriter_DateArithmetic(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "DATEADD day",
			input:    "SELECT DATEADD(day, 7, '2024-01-15')",
			contains: "datetime",
			excludes: "DATEADD(",
		},
		{
			name:     "DATEDIFF day",
			input:    "SELECT DATEDIFF(day, '2024-01-01', '2024-01-15')",
			contains: "julianday",
			excludes: "DATEDIFF(",
		},
		{
			name:     "DATEPART month",
			input:    "SELECT DATEPART(month, '2024-01-15')",
			contains: "strftime",
			excludes: "DATEPART(",
		},
		{
			name:     "EOMONTH",
			input:    "SELECT EOMONTH('2024-01-15')",
			contains: "start of month",
			excludes: "EOMONTH(",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}

func TestSQLiteRewriter_AdvancedStringFunctions(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "REPLICATE",
			input:    "SELECT REPLICATE('ab', 3)",
			contains: "zeroblob",
			excludes: "REPLICATE(",
		},
		{
			name:     "SPACE",
			input:    "SELECT SPACE(5)",
			contains: "zeroblob",
			excludes: "SPACE(",
		},
		{
			name:     "STUFF",
			input:    "SELECT STUFF('abcdef', 2, 3, 'XYZ')",
			contains: "substr",
			excludes: "STUFF(",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}

func TestSQLiteRewriter_OtherFunctions(t *testing.T) {
	rewriter := NewSQLiteRewriter()

	tests := []struct {
		name     string
		input    string
		contains string
		excludes string
	}{
		{
			name:     "ISNUMERIC",
			input:    "SELECT ISNUMERIC('123')",
			contains: "GLOB",
			excludes: "ISNUMERIC(",
		},
		{
			name:     "CHOOSE",
			input:    "SELECT CHOOSE(2, 'a', 'b', 'c')",
			contains: "CASE",
			excludes: "CHOOSE(",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stmt := parseSQL(t, tc.input)
			rewritten := rewriter.RewriteStatement(stmt)
			output := rewritten.String()

			if !strings.Contains(output, tc.contains) {
				t.Errorf("Expected output to contain %q, got: %s", tc.contains, output)
			}
			if tc.excludes != "" && strings.Contains(output, tc.excludes) {
				t.Errorf("Expected output to NOT contain %q, got: %s", tc.excludes, output)
			}
		})
	}
}
