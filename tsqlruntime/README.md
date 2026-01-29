# tsqlruntime - T-SQL Runtime Interpreter

Runtime interpreter for T-SQL, enabling execution of dynamic SQL at runtime in Go applications.

## Overview

`tsqlruntime` solves the "dynamic SQL problem" for T-SQL to Go migrations. Instead of requiring manual rewrites for stored procedures containing `EXEC(@sql)` or `sp_executesql`, the transpiler can now generate code that interprets the dynamic SQL at runtime.

## Stage 1 Features

- **Type System**: Full T-SQL type support with NULL handling
  - Integer types: `bit`, `tinyint`, `smallint`, `int`, `bigint`
  - Decimal types: `decimal`, `numeric`, `money`, `smallmoney`
  - Floating point: `float`, `real`
  - String types: `char`, `varchar`, `nchar`, `nvarchar`
  - Date/time: `date`, `time`, `datetime`, `datetime2`, `smalldatetime`
  - Binary: `binary`, `varbinary`

- **CAST/CONVERT**: Full type conversion with SQL Server style codes

- **Expression Evaluator**: Evaluates T-SQL expressions at runtime

- **40+ Built-in Functions**: String, DateTime, Numeric, NULL handling

## Stage 2 Features

- **Temp Tables (#table)**: CREATE, DROP, TRUNCATE, SELECT, INSERT, UPDATE, DELETE
- **Global Temp Tables (##table)**: Same operations, persist across session clear
- **Table Variables (@table)**: DECLARE, INSERT, SELECT, UPDATE, DELETE
- **TRY/CATCH**: Error catching with ERROR_NUMBER(), ERROR_MESSAGE(), etc.
- **RAISERROR/THROW**: Error generation with severity and state
- **Transactions**: BEGIN/COMMIT/ROLLBACK TRANSACTION
- **ExecutionContext**: Variable scoping, system variables, nested contexts

## Stage 3 Features

- **Cursors**: Full cursor support
  - DECLARE CURSOR with options (LOCAL/GLOBAL, FORWARD_ONLY/SCROLL, STATIC/KEYSET/DYNAMIC)
  - OPEN cursor with query execution
  - FETCH (NEXT, PRIOR, FIRST, LAST, ABSOLUTE, RELATIVE)
  - CLOSE and DEALLOCATE
  - @@FETCH_STATUS tracking

- **JSON Functions** (full implementations):
  - `ISJSON(json_string)` - Validates JSON
  - `JSON_VALUE(json, '$.path')` - Extracts scalar values
  - `JSON_QUERY(json, '$.path')` - Extracts objects/arrays
  - `JSON_MODIFY(json, '$.path', value)` - Modifies JSON
  - `OPENJSON(json)` - Shreds JSON into rows
  - `FOR JSON PATH/AUTO` - Converts result set to JSON

- **XML Functions** (full implementations):
  - `.value('/xpath', 'type')` - Extracts scalar values from XML
  - `.query('/xpath')` - Extracts XML fragments
  - `.exist('/xpath')` - Checks if XPath matches
  - `.nodes('/xpath')` - Shreds XML into rows
  - `OPENXML(xml, '/xpath', flags)` - Parses XML into table
  - `FOR XML RAW/PATH` - Converts result set to XML

- **Additional Functions**:
  - Hash: HASHBYTES (MD5, SHA1, SHA256, SHA512), CHECKSUM, BINARY_CHECKSUM
  - Logical: GREATEST, LEAST
  - Conversion: TRY_CAST, TRY_CONVERT, TRY_PARSE, PARSE
  - System: HOST_NAME, APP_NAME, USER_NAME, SYSTEM_USER, etc.
  - Metadata: COL_NAME, COL_LENGTH, TYPE_ID, TYPE_NAME

## Usage

### Basic Usage

```go
import (
    "context"
    "database/sql"
    "github.com/ha1tch/tgpiler/tsqlruntime"
)

// Create interpreter with database connection
db, _ := sql.Open("postgres", connStr)
interp := tsqlruntime.NewInterpreter(db, tsqlruntime.DialectPostgres)

// Execute dynamic SQL
result, err := interp.Execute(ctx, `
    DECLARE @Status INT = 1
    SELECT * FROM Orders WHERE Status = @Status
`, nil)
```

### Cursor Example

```go
result, err := interp.Execute(ctx, `
    DECLARE @id INT, @name VARCHAR(50)
    
    DECLARE user_cursor CURSOR FOR
    SELECT id, name FROM users WHERE active = 1
    
    OPEN user_cursor
    
    FETCH NEXT FROM user_cursor INTO @id, @name
    WHILE @@FETCH_STATUS = 0
    BEGIN
        PRINT @name
        FETCH NEXT FROM user_cursor INTO @id, @name
    END
    
    CLOSE user_cursor
    DEALLOCATE user_cursor
`, nil)
```

### TRY/CATCH Example

```go
result, err := interp.Execute(ctx, `
    BEGIN TRY
        INSERT INTO orders (id, total) VALUES (1, 100)
    END TRY
    BEGIN CATCH
        DECLARE @err NVARCHAR(200)
        SET @err = ERROR_MESSAGE()
        RAISERROR(@err, 16, 1)
    END CATCH
`, nil)
```

### Temp Tables Example

```go
result, err := interp.Execute(ctx, `
    CREATE TABLE #temp (
        id INT IDENTITY(1,1),
        name VARCHAR(50),
        value DECIMAL(18,2)
    )
    
    INSERT INTO #temp (name, value) VALUES ('A', 10.5)
    INSERT INTO #temp (name, value) VALUES ('B', 20.75)
    
    SELECT * FROM #temp WHERE value > 15
    
    DROP TABLE #temp
`, nil)
```

### JSON Example

```go
result, err := interp.Execute(ctx, `
    DECLARE @json NVARCHAR(MAX) = '{"customer": {"name": "Alice", "orders": [{"id": 1}, {"id": 2}]}}'
    
    -- Extract scalar value
    SELECT JSON_VALUE(@json, '$.customer.name') AS CustomerName
    
    -- Extract object
    SELECT JSON_QUERY(@json, '$.customer.orders') AS Orders
    
    -- Modify JSON
    SET @json = JSON_MODIFY(@json, '$.customer.name', 'Bob')
    
    -- Shred JSON into rows (using OPENJSON with schema)
    SELECT id
    FROM OPENJSON(@json, '$.customer.orders')
    WITH (id INT '$.id')
`, nil)
```

### XML Example

```go
result, err := interp.Execute(ctx, `
    DECLARE @xml XML = '<root><item id="1">A</item><item id="2">B</item></root>'
    
    -- Extract scalar value
    SELECT @xml.value('(/root/item[@id="1"])[1]', 'VARCHAR(10)') AS FirstItem
    
    -- Check if path exists
    SELECT @xml.exist('/root/item[@id="3"]') AS HasItem3
    
    -- Shred XML into rows
    SELECT 
        n.value('@id', 'INT') AS ItemId,
        n.value('.', 'VARCHAR(10)') AS ItemValue
    FROM @xml.nodes('/root/item') AS t(n)
`, nil)
```

### FOR JSON/XML Example

```go
result, err := interp.Execute(ctx, `
    -- Convert query to JSON
    SELECT id, name, total
    FROM orders
    WHERE status = 'active'
    FOR JSON PATH, ROOT('orders')
    
    -- Convert query to XML
    SELECT id, name, total
    FROM orders
    WHERE status = 'active'
    FOR XML PATH('order'), ROOT('orders'), ELEMENTS
`, nil)
```

## Dialects

```go
tsqlruntime.DialectPostgres   // $1, $2, ...
tsqlruntime.DialectMySQL      // ?, ?, ...
tsqlruntime.DialectSQLite     // ?, ?, ...
tsqlruntime.DialectSQLServer  // @p0, @p1, ...
```

## Testing

```bash
go test ./tsqlruntime/... -v
```

## Coverage Summary

| Stage | Features | Coverage |
|-------|----------|----------|
| Stage 1 | Types, CAST/CONVERT, Expressions, Functions | ~70% of dynamic SQL |
| Stage 2 | Temp tables, TRY/CATCH, Transactions | +25% (total ~95%) |
| Stage 3 | Cursors, More functions | +5% (total ~100%) |
