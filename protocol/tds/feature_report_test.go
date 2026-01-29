package tds

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"testing"
	"time"

	_ "github.com/microsoft/go-mssqldb"

	"github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/protocol"
	"github.com/ha1tch/aul/runtime"
	"github.com/ha1tch/aul/server"
)

// TestCase represents a single T-SQL test
type featureTestCase struct {
	Name      string
	SQL       string
	WantRows  bool // true if we expect rows back
	WantError bool // true if we expect an error (e.g., RAISERROR)
}

// TestTSQLFeatureReport runs a comprehensive test of T-SQL features
// and reports what works vs what doesn't.
func TestTSQLFeatureReport(t *testing.T) {
	// Find available port
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Create server with SQLite backend
	logger := log.New(log.Config{
		DefaultLevel: log.LevelError,
		Format:       log.FormatText,
	})

	cfg := server.DefaultConfig()
	cfg.Logger = logger
	cfg.ProcedureDir = ""
	cfg.JITEnabled = false
	cfg.StorageConfig = runtime.StorageConfig{
		Type:    "sqlite",
		Options: map[string]string{"path": ":memory:"},
	}
	cfg.Listeners = []protocol.ListenerConfig{
		{
			Name:     "feature-test",
			Protocol: protocol.ProtocolTDS,
			Port:     port,
		},
	}

	srv, err := server.New(cfg)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}

	if err := srv.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer srv.Stop()
	time.Sleep(100 * time.Millisecond)

	// Connect with go-mssqldb
	connStr := fmt.Sprintf("sqlserver://sa:password@localhost:%d?database=master&encrypt=disable", port)
	db, err := sql.Open("mssql", connStr)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Setup test tables
	setupSQL := []string{
		`CREATE TABLE Customers (ID INT PRIMARY KEY, Name VARCHAR(100), Email VARCHAR(200), Balance DECIMAL(10,2))`,
		`INSERT INTO Customers VALUES (1, 'Alice', 'alice@test.com', 100.50)`,
		`INSERT INTO Customers VALUES (2, 'Bob', 'bob@test.com', 200.75)`,
		`INSERT INTO Customers VALUES (3, 'Charlie', 'charlie@test.com', 50.00)`,
		`CREATE TABLE Orders (ID INT PRIMARY KEY, CustomerID INT, Amount DECIMAL(10,2), OrderDate VARCHAR(20))`,
		`INSERT INTO Orders VALUES (1, 1, 25.00, '2024-01-15')`,
		`INSERT INTO Orders VALUES (2, 1, 35.00, '2024-01-20')`,
		`INSERT INTO Orders VALUES (3, 2, 100.00, '2024-01-18')`,
	}
	for _, sql := range setupSQL {
		db.ExecContext(ctx, sql)
	}

	// ==================== DML ====================
	t.Run("DML", func(t *testing.T) {
		tests := []featureTestCase{
			{"SELECT_basic", "SELECT * FROM Customers", true, false},
			{"SELECT_columns", "SELECT ID, Name FROM Customers", true, false},
			{"SELECT_WHERE", "SELECT * FROM Customers WHERE Balance > 100", true, false},
			{"SELECT_WHERE_AND", "SELECT * FROM Customers WHERE Balance > 50 AND ID < 3", true, false},
			{"SELECT_WHERE_OR", "SELECT * FROM Customers WHERE ID = 1 OR ID = 3", true, false},
			{"SELECT_ORDER_BY", "SELECT * FROM Customers ORDER BY Balance DESC", true, false},
			{"SELECT_ORDER_BY_multi", "SELECT * FROM Customers ORDER BY Balance DESC, Name ASC", true, false},
			{"SELECT_TOP", "SELECT TOP 2 * FROM Customers", true, false},
			{"SELECT_DISTINCT", "SELECT DISTINCT CustomerID FROM Orders", true, false},
			{"INSERT_single", "INSERT INTO Customers VALUES (4, 'Dave', 'dave@test.com', 75.00)", false, false},
			{"UPDATE_basic", "UPDATE Customers SET Balance = 150.00 WHERE ID = 1", false, false},
			{"DELETE_basic", "DELETE FROM Customers WHERE ID = 4", false, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== JOINs ====================
	t.Run("JOINs", func(t *testing.T) {
		tests := []featureTestCase{
			{"INNER_JOIN", "SELECT c.Name, o.Amount FROM Customers c INNER JOIN Orders o ON c.ID = o.CustomerID", true, false},
			{"LEFT_JOIN", "SELECT c.Name, o.Amount FROM Customers c LEFT JOIN Orders o ON c.ID = o.CustomerID", true, false},
			{"JOIN_alias", "SELECT c.Name, o.Amount FROM Customers c JOIN Orders o ON c.ID = o.CustomerID", true, false},
			{"JOIN_WHERE", "SELECT c.Name, o.Amount FROM Customers c JOIN Orders o ON c.ID = o.CustomerID WHERE o.Amount > 30", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Aggregates ====================
	t.Run("Aggregates", func(t *testing.T) {
		tests := []featureTestCase{
			{"COUNT_star", "SELECT COUNT(*) FROM Customers", true, false},
			{"COUNT_column", "SELECT COUNT(Email) FROM Customers", true, false},
			{"SUM", "SELECT SUM(Balance) FROM Customers", true, false},
			{"AVG", "SELECT AVG(Balance) FROM Customers", true, false},
			{"MIN", "SELECT MIN(Balance) FROM Customers", true, false},
			{"MAX", "SELECT MAX(Balance) FROM Customers", true, false},
			{"GROUP_BY", "SELECT CustomerID, SUM(Amount) FROM Orders GROUP BY CustomerID", true, false},
			{"GROUP_BY_HAVING", "SELECT CustomerID, SUM(Amount) as Total FROM Orders GROUP BY CustomerID HAVING SUM(Amount) > 50", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Subqueries ====================
	t.Run("Subqueries", func(t *testing.T) {
		tests := []featureTestCase{
			{"IN_subquery", "SELECT * FROM Customers WHERE ID IN (SELECT CustomerID FROM Orders)", true, false},
			{"EXISTS_subquery", "SELECT * FROM Customers c WHERE EXISTS (SELECT 1 FROM Orders o WHERE o.CustomerID = c.ID)", true, false},
			{"Scalar_subquery", "SELECT Name, (SELECT COUNT(*) FROM Orders o WHERE o.CustomerID = c.ID) as OrderCount FROM Customers c", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== String Functions ====================
	t.Run("StringFunctions", func(t *testing.T) {
		tests := []featureTestCase{
			{"LEN", "SELECT LEN('hello')", true, false},
			{"UPPER", "SELECT UPPER('hello')", true, false},
			{"LOWER", "SELECT LOWER('HELLO')", true, false},
			{"LTRIM", "SELECT LTRIM('  hello')", true, false},
			{"RTRIM", "SELECT RTRIM('hello  ')", true, false},
			{"SUBSTRING", "SELECT SUBSTRING('hello', 1, 3)", true, false},
			{"LEFT", "SELECT LEFT('hello', 3)", true, false},
			{"RIGHT", "SELECT RIGHT('hello', 3)", true, false},
			{"REPLACE", "SELECT REPLACE('hello', 'l', 'x')", true, false},
			{"CHARINDEX", "SELECT CHARINDEX('l', 'hello')", true, false},
			{"CONCAT", "SELECT CONCAT('hello', ' ', 'world')", true, false},
			{"CONCAT_WS", "SELECT CONCAT_WS('-', 'a', 'b', 'c')", true, false},
			{"REVERSE", "SELECT REVERSE('hello')", true, false},
			{"REPLICATE", "SELECT REPLICATE('ab', 3)", true, false},
			{"SPACE", "SELECT SPACE(5)", true, false},
			{"STUFF", "SELECT STUFF('hello', 2, 3, 'XYZ')", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Date Functions ====================
	t.Run("DateFunctions", func(t *testing.T) {
		tests := []featureTestCase{
			{"GETDATE", "SELECT GETDATE()", true, false},
			{"GETUTCDATE", "SELECT GETUTCDATE()", true, false},
			{"YEAR", "SELECT YEAR('2024-01-15')", true, false},
			{"MONTH", "SELECT MONTH('2024-01-15')", true, false},
			{"DAY", "SELECT DAY('2024-01-15')", true, false},
			{"DATEADD", "SELECT DATEADD(day, 7, '2024-01-15')", true, false},
			{"DATEDIFF", "SELECT DATEDIFF(day, '2024-01-01', '2024-01-15')", true, false},
			{"DATEPART", "SELECT DATEPART(month, '2024-01-15')", true, false},
			{"EOMONTH", "SELECT EOMONTH('2024-01-15')", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Math Functions ====================
	t.Run("MathFunctions", func(t *testing.T) {
		tests := []featureTestCase{
			{"ABS", "SELECT ABS(-5)", true, false},
			{"CEILING", "SELECT CEILING(4.3)", true, false},
			{"FLOOR", "SELECT FLOOR(4.7)", true, false},
			{"ROUND", "SELECT ROUND(4.567, 2)", true, false},
			{"POWER", "SELECT POWER(2, 10)", true, false},
			{"SQRT", "SELECT SQRT(16)", true, false},
			{"SIGN", "SELECT SIGN(-5)", true, false},
			{"PI", "SELECT PI()", true, false},
			{"RAND", "SELECT RAND()", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== NULL Handling ====================
	t.Run("NullHandling", func(t *testing.T) {
		tests := []featureTestCase{
			{"ISNULL", "SELECT ISNULL(NULL, 'default')", true, false},
			{"COALESCE", "SELECT COALESCE(NULL, NULL, 'third')", true, false},
			{"NULLIF", "SELECT NULLIF(1, 1)", true, false},
			{"IS_NULL", "SELECT * FROM Customers WHERE Email IS NOT NULL", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== CASE Expression ====================
	t.Run("CaseExpression", func(t *testing.T) {
		tests := []featureTestCase{
			{"CASE_simple", "SELECT ID, CASE WHEN Balance > 100 THEN 'High' ELSE 'Low' END FROM Customers", true, false},
			{"CASE_searched", "SELECT ID, CASE Balance WHEN 100.50 THEN 'Exact' ELSE 'Other' END FROM Customers", true, false},
			{"IIF", "SELECT IIF(1 > 0, 'yes', 'no')", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Type Conversion ====================
	t.Run("TypeConversion", func(t *testing.T) {
		tests := []featureTestCase{
			{"CAST_int", "SELECT CAST(123 AS VARCHAR)", true, false},
			{"CAST_decimal", "SELECT CAST('123.45' AS DECIMAL(10,2))", true, false},
			{"CONVERT", "SELECT CONVERT(VARCHAR, 123)", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Control Flow ====================
	t.Run("ControlFlow", func(t *testing.T) {
		tests := []featureTestCase{
			{"DECLARE_SET_SELECT", `DECLARE @x INT SET @x = 42 SELECT @x`, true, false},
			{"DECLARE_init", `DECLARE @y INT = 100 SELECT @y`, true, false},
			{"IF_ELSE", `DECLARE @v INT = 10 IF @v > 5 SELECT 'big' ELSE SELECT 'small'`, true, false},
			{"WHILE_loop", `DECLARE @i INT = 0, @sum INT = 0 WHILE @i < 5 BEGIN SET @sum = @sum + @i SET @i = @i + 1 END SELECT @sum`, true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== System Variables ====================
	t.Run("SystemVariables", func(t *testing.T) {
		tests := []featureTestCase{
			{"@@VERSION", "SELECT @@VERSION", true, false},
			{"@@ROWCOUNT", "SELECT @@ROWCOUNT", true, false},
			{"@@SERVERNAME", "SELECT @@SERVERNAME", true, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== DDL ====================
	t.Run("DDL", func(t *testing.T) {
		tests := []featureTestCase{
			{"CREATE_TABLE", "CREATE TABLE TestDDL (ID INT, Name VARCHAR(50))", false, false},
			{"DROP_TABLE", "DROP TABLE TestDDL", false, false},
			{"CREATE_DROP_temp", "CREATE TABLE #TempTest (ID INT); DROP TABLE #TempTest", false, false},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Error Handling ====================
	t.Run("ErrorHandling", func(t *testing.T) {
		tests := []featureTestCase{
			{"TRY_CATCH", `BEGIN TRY SELECT 1/0 END TRY BEGIN CATCH SELECT ERROR_MESSAGE() END CATCH`, true, false},
			{"RAISERROR", `RAISERROR('Test error', 16, 1)`, false, true},
		}
		runTests(t, db, ctx, tests)
	})

	// ==================== Other Functions ====================
	t.Run("OtherFunctions", func(t *testing.T) {
		tests := []featureTestCase{
			{"NEWID", "SELECT NEWID()", true, false},
			{"ISNUMERIC", "SELECT ISNUMERIC('123')", true, false},
			{"CHOOSE", "SELECT CHOOSE(2, 'a', 'b', 'c')", true, false},
		}
		runTests(t, db, ctx, tests)
	})
}

func runTests(t *testing.T, db *sql.DB, ctx context.Context, tests []featureTestCase) {
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			if tc.WantRows {
				rows, err := db.QueryContext(ctx, tc.SQL)
				if err != nil {
					if tc.WantError {
						// Expected error - test passes
						return
					}
					t.Errorf("FAIL: %v", err)
					return
				}
				defer rows.Close()
				count := 0
				for rows.Next() {
					count++
				}
				if err := rows.Err(); err != nil {
					if tc.WantError {
						// Expected error - test passes
						return
					}
					t.Errorf("FAIL (rows): %v", err)
				}
				// Success - test passes
			} else {
				_, err := db.ExecContext(ctx, tc.SQL)
				if tc.WantError {
					// We expect an error
					if err == nil {
						t.Errorf("FAIL: expected error but got none")
					}
					// Error was expected - test passes
				} else {
					// We don't expect an error
					if err != nil {
						t.Errorf("FAIL: %v", err)
					}
				}
			}
		})
	}
}
