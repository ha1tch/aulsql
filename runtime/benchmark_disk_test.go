package runtime_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	pkglog "github.com/ha1tch/aul/pkg/log"
	"github.com/ha1tch/aul/procedure"
	"github.com/ha1tch/aul/runtime"
	"github.com/ha1tch/aul/storage"
)

// Disk-based benchmark harness
type diskBenchEnv struct {
	registry *procedure.Registry
	rt       *runtime.Runtime
	storage  *storage.SQLiteStorage
	ctx      context.Context
	tmpDir   string
}

func setupDiskBenchEnv(b *testing.B) *diskBenchEnv {
	b.Helper()

	// Create temp directory for database
	tmpDir, err := os.MkdirTemp("", "aul-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}

	logger := pkglog.New(pkglog.Config{
		DefaultLevel: pkglog.LevelError,
		Format:       pkglog.FormatText,
	})

	registry := procedure.NewRegistry()

	// Configure SQLite with filesystem storage and WAL mode
	cfg := storage.SQLiteConfig{
		Path:         filepath.Join(tmpDir, "bench.db"),
		MaxOpenConns: 1,
		MaxIdleConns: 1,
		JournalMode:  "WAL",
		Synchronous:  "NORMAL",
		CacheSize:    -64000, // 64MB cache
		BusyTimeout:  5000,
	}

	storageBackend, err := storage.NewSQLiteStorage(cfg)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatalf("Failed to create storage: %v", err)
	}

	rtConfig := runtime.DefaultConfig()
	rtConfig.JITEnabled = false
	rt := runtime.New(rtConfig, registry, logger)
	rt.SetStorage(storageBackend)

	return &diskBenchEnv{
		registry: registry,
		rt:       rt,
		storage:  storageBackend,
		ctx:      context.Background(),
		tmpDir:   tmpDir,
	}
}

func (e *diskBenchEnv) close() {
	e.storage.Close()
	os.RemoveAll(e.tmpDir)
}

func (e *diskBenchEnv) registerProc(name, source string) *procedure.Procedure {
	proc := &procedure.Procedure{
		Name:     name,
		Database: "bench",
		Schema:   "dbo",
		Source:   source,
	}
	e.registry.Register(proc)
	return proc
}

func (e *diskBenchEnv) exec(b *testing.B, proc *procedure.Procedure, params map[string]interface{}) {
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
// DISK BENCHMARKS
// =============================================================================

func BenchmarkDisk_SimpleSelect(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_SelectWithParams(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_TableInsert(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_TableSelect(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_TableSelectAggregate(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_Transaction(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_ComplexProcedure(b *testing.B) {
	env := setupDiskBenchEnv(b)
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

func BenchmarkDisk_WhileLoop_100(b *testing.B) {
	env := setupDiskBenchEnv(b)
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
		env.exec(b, proc, map[string]interface{}{"N": 100})
	}
}

func BenchmarkDisk_TempTable(b *testing.B) {
	env := setupDiskBenchEnv(b)
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
