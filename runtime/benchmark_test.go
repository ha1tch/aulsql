package runtime_test

import (
	"context"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	pkglog "github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/procedure"
	"github.com/ha1tch/aul/runtime"
	"github.com/ha1tch/aul/storage"
)

// Benchmark harness that can be reused
type benchEnv struct {
	registry *procedure.Registry
	rt       *runtime.Runtime
	storage  *storage.SQLiteStorage
	ctx      context.Context
}

func setupBenchEnv(b *testing.B) *benchEnv {
	b.Helper()

	logger := pkglog.New(pkglog.Config{
		DefaultLevel: pkglog.LevelError,
		Format:       pkglog.FormatText,
	})

	registry := procedure.NewRegistry()

	cfg := storage.DefaultSQLiteConfig()
	storageBackend, err := storage.NewSQLiteStorage(cfg)
	if err != nil {
		b.Fatalf("Failed to create storage: %v", err)
	}

	rtConfig := runtime.DefaultConfig()
	rtConfig.JITEnabled = false
	rt := runtime.New(rtConfig, registry, logger)
	rt.SetStorage(storageBackend)

	return &benchEnv{
		registry: registry,
		rt:       rt,
		storage:  storageBackend,
		ctx:      context.Background(),
	}
}

func (e *benchEnv) close() {
	e.storage.Close()
}

func (e *benchEnv) registerProc(name, source string) *procedure.Procedure {
	proc := &procedure.Procedure{
		Name:     name,
		Database: "bench",
		Schema:   "dbo",
		Source:   source,
	}
	e.registry.Register(proc)
	return proc
}

func (e *benchEnv) exec(b *testing.B, proc *procedure.Procedure, params map[string]interface{}) {
	_, err := e.rt.Execute(e.ctx, proc, &runtime.ExecContext{
		SessionID:  "bench",
		Database:   "bench",
		Parameters: params,
	})
	if err != nil {
		b.Fatalf("Execution failed: %v", err)
	}
}

// =============================================================================
// BENCHMARK: Simple SELECT (no tables)
// =============================================================================

func BenchmarkSimpleSelect(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("SimpleSelect", `
CREATE PROCEDURE dbo.SimpleSelect
AS
BEGIN
    SELECT 1 AS Value
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, nil)
	}
}

// =============================================================================
// BENCHMARK: SELECT with parameters
// =============================================================================

func BenchmarkSelectWithParams(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("SelectWithParams", `
CREATE PROCEDURE dbo.SelectWithParams
    @A INT,
    @B INT,
    @C VARCHAR(50)
AS
BEGIN
    SELECT @A + @B AS Sum, @C AS Label
END
`)

	params := map[string]interface{}{
		"A": 100,
		"B": 200,
		"C": "test",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, params)
	}
}

// =============================================================================
// BENCHMARK: Arithmetic and string operations
// =============================================================================

func BenchmarkArithmetic(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("Arithmetic", `
CREATE PROCEDURE dbo.Arithmetic
    @X INT
AS
BEGIN
    DECLARE @Result INT
    SET @Result = (@X * 2) + (@X / 2) - (@X % 7)
    SET @Result = @Result * @Result
    SELECT @Result AS Result
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"X": i % 1000})
	}
}

// =============================================================================
// BENCHMARK: Control flow (IF/ELSE)
// =============================================================================

func BenchmarkControlFlow(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("ControlFlow", `
CREATE PROCEDURE dbo.ControlFlow
    @Value INT
AS
BEGIN
    DECLARE @Result VARCHAR(20)
    
    IF @Value < 0
        SET @Result = 'negative'
    ELSE IF @Value = 0
        SET @Result = 'zero'
    ELSE IF @Value < 10
        SET @Result = 'small'
    ELSE IF @Value < 100
        SET @Result = 'medium'
    ELSE
        SET @Result = 'large'
    
    SELECT @Result AS Category
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"Value": (i % 200) - 50})
	}
}

// =============================================================================
// BENCHMARK: WHILE loop
// =============================================================================

func BenchmarkWhileLoop_10(b *testing.B) {
	benchWhileLoop(b, 10)
}

func BenchmarkWhileLoop_100(b *testing.B) {
	benchWhileLoop(b, 100)
}

func BenchmarkWhileLoop_1000(b *testing.B) {
	benchWhileLoop(b, 1000)
}

func benchWhileLoop(b *testing.B, iterations int) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("WhileLoop", `
CREATE PROCEDURE dbo.WhileLoop
    @N INT
AS
BEGIN
    DECLARE @i INT = 0
    DECLARE @sum INT = 0
    
    WHILE @i < @N
    BEGIN
        SET @sum = @sum + @i
        SET @i = @i + 1
    END
    
    SELECT @sum AS Total
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"N": iterations})
	}
}

// =============================================================================
// BENCHMARK: Table operations (INSERT/SELECT)
// =============================================================================

func BenchmarkTableInsert(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	// Setup table
	setupProc := env.registerProc("Setup", `
CREATE PROCEDURE dbo.Setup
AS
BEGIN
    CREATE TABLE BenchData (ID INT PRIMARY KEY, Value INT, Label VARCHAR(50))
END
`)
	env.exec(b, setupProc, nil)

	proc := env.registerProc("TableInsert", `
CREATE PROCEDURE dbo.TableInsert
    @ID INT,
    @Value INT
AS
BEGIN
    INSERT INTO BenchData (ID, Value, Label) VALUES (@ID, @Value, 'test')
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"ID": i, "Value": i * 10})
	}
}

func BenchmarkTableSelect(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	// Setup table with data
	db := env.storage.GetDB()
	db.Exec("CREATE TABLE BenchSelect (ID INT PRIMARY KEY, Value INT, Label VARCHAR(50))")
	for i := 0; i < 1000; i++ {
		db.Exec("INSERT INTO BenchSelect VALUES (?, ?, ?)", i, i*10, fmt.Sprintf("label_%d", i))
	}

	proc := env.registerProc("TableSelect", `
CREATE PROCEDURE dbo.TableSelect
    @MinID INT,
    @MaxID INT
AS
BEGIN
    SELECT ID, Value, Label FROM BenchSelect WHERE ID >= @MinID AND ID < @MaxID
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := (i * 10) % 900
		env.exec(b, proc, map[string]interface{}{"MinID": start, "MaxID": start + 100})
	}
}

func BenchmarkTableSelectAggregate(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	// Setup table with data
	db := env.storage.GetDB()
	db.Exec("CREATE TABLE BenchAgg (ID INT PRIMARY KEY, Category INT, Amount DECIMAL(10,2))")
	for i := 0; i < 10000; i++ {
		db.Exec("INSERT INTO BenchAgg VALUES (?, ?, ?)", i, i%10, float64(i)*1.5)
	}

	proc := env.registerProc("TableSelectAggregate", `
CREATE PROCEDURE dbo.TableSelectAggregate
AS
BEGIN
    SELECT 
        Category,
        COUNT(*) AS Cnt,
        SUM(Amount) AS Total,
        AVG(Amount) AS Avg,
        MIN(Amount) AS Min,
        MAX(Amount) AS Max
    FROM BenchAgg
    GROUP BY Category
    ORDER BY Category
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, nil)
	}
}

// =============================================================================
// BENCHMARK: Temp tables
// =============================================================================

func BenchmarkTempTable(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("TempTable", `
CREATE PROCEDURE dbo.TempTable
    @Count INT
AS
BEGIN
    CREATE TABLE #Temp (ID INT, Value INT)
    
    DECLARE @i INT = 0
    WHILE @i < @Count
    BEGIN
        INSERT INTO #Temp VALUES (@i, @i * 2)
        SET @i = @i + 1
    END
    
    SELECT SUM(Value) AS Total FROM #Temp
    
    DROP TABLE #Temp
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"Count": 100})
	}
}

// =============================================================================
// BENCHMARK: Nested EXEC
// =============================================================================

func BenchmarkNestedExec_1Level(b *testing.B) {
	benchNestedExec(b, 1)
}

func BenchmarkNestedExec_3Levels(b *testing.B) {
	benchNestedExec(b, 3)
}

func BenchmarkNestedExec_5Levels(b *testing.B) {
	benchNestedExec(b, 5)
}

func benchNestedExec(b *testing.B, depth int) {
	env := setupBenchEnv(b)
	defer env.close()

	// Create leaf procedure
	env.registerProc("Leaf", `
CREATE PROCEDURE dbo.Leaf
    @Value INT
AS
BEGIN
    SELECT @Value * 2 AS Result
END
`)

	// Create chain of procedures
	prevName := "Leaf"
	for i := 1; i <= depth; i++ {
		name := fmt.Sprintf("Level%d", i)
		source := fmt.Sprintf(`
CREATE PROCEDURE dbo.%s
    @Value INT
AS
BEGIN
    EXEC dbo.%s @Value = @Value
    SELECT @Value AS PassedValue
END
`, name, prevName)
		env.registerProc(name, source)
		prevName = name
	}

	proc, err := env.registry.LookupInDatabase("dbo."+prevName, "bench")
	if err != nil {
		b.Fatalf("Failed to lookup %s: %v", prevName, err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"Value": i})
	}
}

// =============================================================================
// BENCHMARK: Transaction overhead
// =============================================================================

func BenchmarkTransaction(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	db := env.storage.GetDB()
	db.Exec("CREATE TABLE BenchTxn (ID INT PRIMARY KEY, Value INT)")

	proc := env.registerProc("Transaction", `
CREATE PROCEDURE dbo.Transaction
    @ID INT,
    @Value INT
AS
BEGIN
    BEGIN TRANSACTION
    INSERT INTO BenchTxn (ID, Value) VALUES (@ID, @Value)
    UPDATE BenchTxn SET Value = Value + 1 WHERE ID = @ID
    COMMIT TRANSACTION
    SELECT Value FROM BenchTxn WHERE ID = @ID
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"ID": i, "Value": i * 10})
	}
}

// =============================================================================
// BENCHMARK: Complex procedure (inventory-like)
// =============================================================================

func BenchmarkComplexProcedure(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	// Setup schema
	db := env.storage.GetDB()
	db.Exec(`CREATE TABLE Products (
		ProductID INT PRIMARY KEY, 
		Name VARCHAR(100), 
		Price DECIMAL(10,2), 
		Stock INT
	)`)
	db.Exec(`CREATE TABLE Orders (
		OrderID INT PRIMARY KEY,
		ProductID INT,
		Quantity INT,
		Total DECIMAL(10,2)
	)`)

	// Seed products
	for i := 1; i <= 100; i++ {
		db.Exec("INSERT INTO Products VALUES (?, ?, ?, ?)",
			i, fmt.Sprintf("Product %d", i), float64(i)*9.99, 1000)
	}

	proc := env.registerProc("ComplexProcedure", `
CREATE PROCEDURE dbo.ComplexProcedure
    @OrderID INT,
    @ProductID INT,
    @Quantity INT
AS
BEGIN
    DECLARE @Price DECIMAL(10,2)
    DECLARE @Stock INT
    DECLARE @Total DECIMAL(10,2)
    
    -- Get product info
    SELECT @Price = Price, @Stock = Stock
    FROM Products
    WHERE ProductID = @ProductID
    
    -- Validate
    IF @Price IS NULL
    BEGIN
        RAISERROR('Product not found', 16, 1)
        RETURN
    END
    
    IF @Stock < @Quantity
    BEGIN
        RAISERROR('Insufficient stock', 16, 1)
        RETURN
    END
    
    -- Calculate
    SET @Total = @Price * @Quantity
    
    -- Execute order
    BEGIN TRANSACTION
    
    INSERT INTO Orders (OrderID, ProductID, Quantity, Total)
    VALUES (@OrderID, @ProductID, @Quantity, @Total)
    
    UPDATE Products
    SET Stock = Stock - @Quantity
    WHERE ProductID = @ProductID
    
    COMMIT TRANSACTION
    
    -- Return confirmation
    SELECT @OrderID AS OrderID, @ProductID AS ProductID, 
           @Quantity AS Quantity, @Total AS Total
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{
			"OrderID":   i,
			"ProductID": (i % 100) + 1,
			"Quantity":  1,
		})
	}
}

// =============================================================================
// BENCHMARK: Multiple result sets
// =============================================================================

func BenchmarkMultipleResultSets(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("MultipleResultSets", `
CREATE PROCEDURE dbo.MultipleResultSets
    @ID INT
AS
BEGIN
    SELECT @ID AS ID, 'First' AS ResultSet
    SELECT @ID * 2 AS DoubleID, 'Second' AS ResultSet
    SELECT @ID * 3 AS TripleID, 'Third' AS ResultSet
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"ID": i})
	}
}

// =============================================================================
// BENCHMARK: String operations
// =============================================================================

func BenchmarkStringOperations(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("StringOperations", `
CREATE PROCEDURE dbo.StringOperations
    @Input VARCHAR(100)
AS
BEGIN
    DECLARE @Result VARCHAR(200)
    SET @Result = UPPER(@Input) + ' - ' + LOWER(@Input)
    SET @Result = LTRIM(RTRIM(@Result))
    SET @Result = LEFT(@Result, 50) + '...'
    SELECT @Result AS Result, LEN(@Result) AS Length
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"Input": "  Hello World  "})
	}
}

// =============================================================================
// BENCHMARK: CASE expression
// =============================================================================

func BenchmarkCaseExpression(b *testing.B) {
	env := setupBenchEnv(b)
	defer env.close()

	proc := env.registerProc("CaseExpression", `
CREATE PROCEDURE dbo.CaseExpression
    @Value INT
AS
BEGIN
    SELECT 
        @Value AS Input,
        CASE 
            WHEN @Value < 0 THEN 'Negative'
            WHEN @Value = 0 THEN 'Zero'
            WHEN @Value BETWEEN 1 AND 10 THEN 'Low'
            WHEN @Value BETWEEN 11 AND 100 THEN 'Medium'
            WHEN @Value BETWEEN 101 AND 1000 THEN 'High'
            ELSE 'Very High'
        END AS Category
END
`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		env.exec(b, proc, map[string]interface{}{"Value": (i * 7) % 2000 - 100})
	}
}
