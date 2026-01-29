# Dialect Compatibility Layer Inventory

This document catalogs ALL ad-hoc and heuristic dialect transformations in aul
so they can be systematically replaced with proper AST-level rewriting.

## Current Implementation Locations

### 1. tsqlruntime/dialect.go — String-based SQL Normalization

**Layer:** Post-AST string manipulation (WRONG LAYER)
**Method:** Regex replacement on SQL text

#### SQLite Transformations (`normalizeForSQLite`)

| T-SQL | SQLite | Line | Notes |
|-------|--------|------|-------|
| `GETDATE()` | `datetime('now')` | 39 | Parameterless function |
| `SYSDATETIME()` | `datetime('now')` | 40 | Parameterless function |
| `GETUTCDATE()` | `datetime('now', 'utc')` | 43 | Parameterless function |
| `SYSUTCDATETIME()` | `datetime('now', 'utc')` | 44 | Parameterless function |
| `ISNULL(a, b)` | `IFNULL(a, b)` | 47 | Function rename |
| `LEN(s)` | `LENGTH(s)` | 50 | Function rename |
| `DATALENGTH(s)` | `LENGTH(s)` | 51 | Function rename (semantic difference!) |
| `CHARINDEX(sub, str)` | `INSTR(str, sub)` | 54 | Argument swap + rename |
| `SUBSTRING(s, i, n)` | `SUBSTR(s, i, n)` | 57 | Function rename |
| `CONVERT(type, val)` | `CAST(val AS type)` | 60 | Complex rewrite |
| `NEWID()` | `lower(hex(randomblob(16)))` | 63 | Function replacement |
| `'a' + 'b'` | `'a' \|\| 'b'` | 67 | Operator change (heuristic!) |
| `SELECT TOP N` | `SELECT ... LIMIT N` | 70 | Clause movement |

#### PostgreSQL Transformations (`normalizeForPostgres`)

| T-SQL | PostgreSQL | Line | Notes |
|-------|------------|------|-------|
| `GETDATE()` | `NOW()` | 78 | |
| `SYSDATETIME()` | `NOW()` | 79 | |
| `GETUTCDATE()` | `(NOW() AT TIME ZONE 'UTC')` | 82 | |
| `SYSUTCDATETIME()` | `(NOW() AT TIME ZONE 'UTC')` | 83 | |
| `ISNULL(a, b)` | `COALESCE(a, b)` | 86 | |
| `LEN(s)` | `LENGTH(s)` | 89 | |
| `DATALENGTH(s)` | `OCTET_LENGTH(s)` | 90 | |
| `CHARINDEX(sub, str)` | `POSITION(sub IN str)` | 93 | |
| `NEWID()` | `gen_random_uuid()` | 96 | |
| `SELECT TOP N` | `SELECT ... LIMIT N` | 99 | |

#### MySQL Transformations (`normalizeForMySQL`)

| T-SQL | MySQL | Line | Notes |
|-------|-------|------|-------|
| `GETDATE()` | `NOW()` | 107 | |
| `SYSDATETIME()` | `NOW()` | 108 | |
| `GETUTCDATE()` | `UTC_TIMESTAMP()` | 111 | |
| `SYSUTCDATETIME()` | `UTC_TIMESTAMP()` | 112 | |
| `ISNULL(a, b)` | `IFNULL(a, b)` | 115 | |
| `LEN(s)` | `CHAR_LENGTH(s)` | 118 | |
| `DATALENGTH(s)` | `LENGTH(s)` | 119 | |
| `CHARINDEX(sub, str)` | `LOCATE(sub, str)` | 122 | |
| `NEWID()` | `UUID()` | 125 | |
| `SELECT TOP N` | `SELECT ... LIMIT N` | 128 | |

---

### 2. tsqlruntime/ddl.go — DDL Type Mapping

**Layer:** Post-AST string manipulation (WRONG LAYER)
**Method:** String replacement on DDL text

#### SQLite Type Mappings (`normalizeTypes`)

| T-SQL Type | SQLite Type | Line | Notes |
|------------|-------------|------|-------|
| `BIGINT` | `INTEGER` | 87 | |
| `SMALLINT` | `INTEGER` | 88 | |
| `TINYINT` | `INTEGER` | 89 | |
| `BIT` | `INTEGER` | 90 | |
| `MONEY` | `REAL` | 93 | |
| `SMALLMONEY` | `REAL` | 94 | |
| `FLOAT` | `REAL` | 95 | |
| `NVARCHAR(MAX)` | `TEXT` | 97 | |
| `VARCHAR(MAX)` | `TEXT` | 98 | |
| `NCHAR` | `TEXT` | 99 | |
| `NTEXT` | `TEXT` | 100 | |
| `DATETIME2` | `TEXT` | 103 | |
| `DATETIME` | `TEXT` | 104 | |
| `SMALLDATETIME` | `TEXT` | 105 | |
| `DATE` | `TEXT` | 106 | |
| `TIME` | `TEXT` | 107 | |
| `DATETIMEOFFSET` | `TEXT` | 108 | |
| `VARBINARY(MAX)` | `BLOB` | 111 | |
| `VARBINARY` | `BLOB` | 112 | |
| `BINARY` | `BLOB` | 113 | |
| `IMAGE` | `BLOB` | 114 | |
| `UNIQUEIDENTIFIER` | `TEXT` | 117 | |
| `XML` | `TEXT` | 118 | |
| `SQL_VARIANT` | `TEXT` | 119 | |

---

### 3. tsqlruntime/interpreter.go — Placeholder Generation

**Layer:** Query building (CORRECT LAYER)
**Method:** Switch on dialect enum

#### Placeholder Styles (`getPlaceholder`)

| Dialect | Style | Line |
|---------|-------|------|
| PostgreSQL | `$1, $2, $3...` | 950-951 |
| MySQL | `?` | 952-953 |
| SQLite | `?` | 952-953 |
| SQL Server | `@p0, @p1, @p2...` | 954-955 |

---

### 4. tsqlruntime/interpreter.go — System Variable Substitution

**Layer:** Query building (CORRECT LAYER - but string quoting is ad-hoc)
**Method:** Direct substitution from ExecutionContext

#### System Variables

| Variable | Source | Line |
|----------|--------|------|
| `@@ROWCOUNT` | `ctx.RowCount` | context.go:129 |
| `@@IDENTITY` | `ctx.LastInsertID` | context.go:131 |
| `@@SCOPE_IDENTITY` | `ctx.LastInsertID` | context.go:131 |
| `@@FETCH_STATUS` | `ctx.FetchStatus` | context.go:133 |
| `@@TRANCOUNT` | `ctx.TranCount` | context.go:135 |
| `@@ERROR` | `ctx.Error` | context.go:137 |
| `@@VERSION` | Hardcoded string | context.go:140 |
| `@@SERVERNAME` | Hardcoded string | context.go:142 |
| `@@SPID` | Hardcoded `1` | context.go:144 |

**Ad-hoc handling:** String values are quoted with `'...'` in `substituteVariables()` (line ~905)

---

### 5. tsqlruntime/functions.go — Built-in Function Implementations

**Layer:** Expression evaluation (CORRECT LAYER for internal evaluation)
**Note:** These are evaluated in-memory, not passed to backend DB

| Function | Line | Notes |
|----------|------|-------|
| `LEN` | 52 | Character count |
| `DATALENGTH` | 53 | Byte count |
| `SUBSTRING` | 54 | |
| `CHARINDEX` | 63 | |
| `CONCAT` | 65 | |
| `CONCAT_WS` | 66 | |
| `ISNULL` | 80 | |
| `GETDATE` | 87 | |
| `SYSDATETIME` | 89 | |
| `SYSUTCDATETIME` | 90 | |
| `NEWID` | 123 | |
| `DATETIMEFROMPARTS` | 167 | |
| `DATETIME2FROMPARTS` | 168 | |
| `TODATETIMEOFFSET` | 170 | |
| `TRY_CAST` | stage3:47 | |
| `TRY_CONVERT` | stage3:48 | |

---

### 6. storage/sqlite.go — Storage Layer Type Mapping

**Layer:** Storage abstraction (CORRECT LAYER)
**Method:** Switch on type strings

#### T-SQL to SQLite (`mapTSQLTypeToSQLite`)

| T-SQL | SQLite | Line |
|-------|--------|------|
| `VARCHAR, NVARCHAR, CHAR, NCHAR, TEXT, NTEXT` | `TEXT` | 421-422 |
| `INT, BIGINT, SMALLINT, TINYINT, BIT` | `INTEGER` | 423-424 |
| `DECIMAL, NUMERIC, MONEY, SMALLMONEY, FLOAT, REAL` | `REAL` | 425-426 |
| `DATE, DATETIME, DATETIME2, SMALLDATETIME, TIME` | `TEXT` | 427-428 |
| `BINARY, VARBINARY, IMAGE` | `BLOB` | 429-430 |
| `UNIQUEIDENTIFIER` | `TEXT` | 431-432 |

#### SQLite to T-SQL (`mapSQLiteType`)

| SQLite | T-SQL | Line |
|--------|-------|------|
| `INTEGER` | `BIGINT` | 439-440 |
| `REAL` | `FLOAT` | 441-442 |
| `TEXT` | `NVARCHAR` | 443-444 |
| `BLOB` | `VARBINARY` | 445-446 |
| `NUMERIC` | `DECIMAL` | 447-448 |

---

### 7. tsqlruntime/splogger.go — Logging Table DDL

**Layer:** Utility (ACCEPTABLE)
**Method:** Switch on dialect string

| Dialect | Primary Key Syntax | Line |
|---------|-------------------|------|
| PostgreSQL | `SERIAL PRIMARY KEY` | 261 |
| SQL Server | `INT IDENTITY(1,1) PRIMARY KEY` | 263 |
| MySQL/SQLite | `INT AUTO_INCREMENT PRIMARY KEY` | 267 |

---

### 8. protocol/tds/connection.go — TDS Type Mapping

**Layer:** Protocol (CORRECT LAYER)
**Note:** Maps between TDS wire format and Go types

| TDS Type | Go Type | Line |
|----------|---------|------|
| `VARCHAR` | `string` | 619 |
| `NVARCHAR` | `string` | 624 |
| `DATETIME` | `time.Time` | 639 |
| Default | `NVARCHAR` | 549, 653 |

---

## Summary: What Needs AST-Level Rewriting

### MUST MOVE TO AST LAYER

| Category | Items | Current Location |
|----------|-------|------------------|
| **Function Renames** | ISNULL, LEN, DATALENGTH, SUBSTRING | dialect.go |
| **Function Replacements** | GETDATE, SYSDATETIME, GETUTCDATE, NEWID | dialect.go |
| **Argument Reordering** | CHARINDEX → INSTR | dialect.go |
| **Clause Movement** | TOP → LIMIT | dialect.go |
| **Operator Change** | `+` → `\|\|` for strings | dialect.go (heuristic) |
| **DDL Types** | All type mappings | ddl.go |
| **CONVERT → CAST** | Syntax transformation | dialect.go |

### ALREADY AT CORRECT LAYER

| Category | Items | Location |
|----------|-------|----------|
| **Placeholder Style** | `$1` vs `?` vs `@p0` | interpreter.go |
| **In-Memory Functions** | All registered functions | functions.go |
| **Storage Type Mapping** | T-SQL ↔ SQLite types | storage/sqlite.go |
| **Protocol Types** | TDS wire format | tds/connection.go |

### DUPLICATED (CONSOLIDATE)

| Item | Locations | Action |
|------|-----------|--------|
| Type mappings | ddl.go, storage/sqlite.go | Single source of truth |
| Function implementations | functions.go (eval) + dialect.go (passthrough) | Clarify when each is used |

---

## Recommended AST Rewriter Interface

```go
// tsqlruntime/ast_rewriter.go

type ASTRewriter interface {
    // Statement-level
    RewriteSelect(*ast.SelectStatement) *ast.SelectStatement
    RewriteInsert(*ast.InsertStatement) *ast.InsertStatement
    RewriteUpdate(*ast.UpdateStatement) *ast.UpdateStatement
    RewriteDelete(*ast.DeleteStatement) *ast.DeleteStatement
    RewriteCreateTable(*ast.CreateTableStatement) *ast.CreateTableStatement
    
    // Expression-level
    RewriteExpression(ast.Expression) ast.Expression
    RewriteFunctionCall(*ast.FunctionCall) ast.Expression
    RewriteBinaryOp(*ast.BinaryExpression) ast.Expression
    
    // Type-level
    RewriteDataType(*ast.DataType) *ast.DataType
}

// Dialect-specific implementations
type SQLiteRewriter struct{}
type PostgresRewriter struct{}
type MySQLRewriter struct{}
type PassthroughRewriter struct{} // For SQL Server - no changes
```

## Migration Path

1. **Phase 1:** Create `ASTRewriter` interface and `PassthroughRewriter`
2. **Phase 2:** Implement `SQLiteRewriter` for functions (ISNULL, LEN, etc.)
3. **Phase 3:** Move TOP→LIMIT to AST rewriter
4. **Phase 4:** Move CONVERT→CAST to AST rewriter
5. **Phase 5:** Add type rewriting for DDL
6. **Phase 6:** Remove `dialect.go` string manipulation
7. **Phase 7:** Consolidate type mappings

## Files to Modify

| File | Changes |
|------|---------|
| `tsqlruntime/ast_rewriter.go` | NEW - Interface and implementations |
| `tsqlruntime/interpreter.go` | Call AST rewriter before `String()` |
| `tsqlruntime/dialect.go` | REMOVE after migration |
| `tsqlruntime/ddl.go` | Use AST rewriter for types |
| `tsqlparser/ast/*.go` | May need dialect param for `String()` |

---

## Additional Dialect Features NOT YET IMPLEMENTED

From tgpiler's `storage/dialects.go`, these features exist but are not in aul:

### Identifier Quoting

| Dialect | Style | Status |
|---------|-------|--------|
| PostgreSQL | `"name"` | Not implemented |
| MySQL | `` `name` `` | Not implemented |
| SQLite | `"name"` | Not implemented |
| SQL Server | `[name]` | Not implemented |

### Table Alias Syntax

| Dialect | Style | Status |
|---------|-------|--------|
| All except Oracle | `table AS alias` | Works (T-SQL default) |
| Oracle | `table alias` (no AS!) | Not implemented |

### Boolean Literals

| Dialect | TRUE | FALSE | Status |
|---------|------|-------|--------|
| PostgreSQL | `TRUE` | `FALSE` | Not implemented |
| MySQL | `1` | `0` | Not implemented |
| SQLite | `1` | `0` | Not implemented |
| SQL Server | `1` | `0` | Works (T-SQL default) |

### LIMIT/TOP Position

| Dialect | Position | Syntax | Status |
|---------|----------|--------|--------|
| PostgreSQL | End | `LIMIT n` | Partially (regex) |
| MySQL | End | `LIMIT n` | Partially (regex) |
| SQLite | End | `LIMIT n` | Partially (regex) |
| SQL Server | After SELECT | `TOP n` | Native |
| Oracle | End | `FETCH FIRST n ROWS ONLY` | Not implemented |

### OFFSET/FETCH

| Dialect | Syntax | Status |
|---------|--------|--------|
| PostgreSQL | `OFFSET n ROWS FETCH FIRST m ROWS ONLY` | Not implemented |
| MySQL | `LIMIT m OFFSET n` | Not implemented |
| SQLite | `LIMIT m OFFSET n` | Not implemented |
| SQL Server | `OFFSET n ROWS FETCH NEXT m ROWS ONLY` | Not implemented |

### Null-Safe Equality

| Dialect | Syntax | Status |
|---------|--------|--------|
| PostgreSQL | `IS NOT DISTINCT FROM` | Not implemented |
| MySQL | `<=>` | Not implemented |
| SQLite | `IS` | Not implemented |
| SQL Server | `(a = b OR (a IS NULL AND b IS NULL))` | Not implemented |

### String Concatenation

| Dialect | Operator | Status |
|---------|----------|--------|
| PostgreSQL | `\|\|` | Partial (heuristic) |
| MySQL | `CONCAT()` function | Not implemented |
| SQLite | `\|\|` | Partial (heuristic) |
| SQL Server | `+` | Native |

### UPDATE with JOIN

| Dialect | Syntax | Status |
|---------|--------|--------|
| PostgreSQL | `UPDATE t SET ... FROM o WHERE ...` | Not implemented |
| MySQL | `UPDATE t JOIN o SET ...` | Not implemented |
| SQLite | `UPDATE t SET ... FROM o WHERE ...` (3.33+) | Not implemented |
| SQL Server | `UPDATE t SET ... FROM t JOIN o` | Native |

### DELETE with JOIN

| Dialect | Syntax | Status |
|---------|--------|--------|
| PostgreSQL | `DELETE FROM t USING o WHERE ...` | Not implemented |
| MySQL | `DELETE t FROM t JOIN o` | Not implemented |
| SQLite | Must use subquery | Not implemented |
| SQL Server | `DELETE t FROM t JOIN o` | Native |

### UPSERT/MERGE

| Dialect | Syntax | Status |
|---------|--------|--------|
| PostgreSQL | `ON CONFLICT DO UPDATE` | Not implemented |
| MySQL | `ON DUPLICATE KEY UPDATE` | Not implemented |
| SQLite | `ON CONFLICT DO UPDATE` (3.24+) | Not implemented |
| SQL Server | `MERGE ... WHEN MATCHED` | Native |

### FROM DUAL

| Dialect | Required | Status |
|---------|----------|--------|
| Oracle | Yes (`SELECT 1 FROM DUAL`) | Not implemented |
| Others | No | Works |

### RETURNING / OUTPUT

| Dialect | Syntax | Status |
|---------|--------|--------|
| PostgreSQL | `INSERT ... RETURNING id` | Not implemented |
| SQLite | `INSERT ... RETURNING id` (3.35+) | Not implemented |
| SQL Server | `OUTPUT inserted.id` | Native |
| MySQL | `LAST_INSERT_ID()` | Not implemented |

### Last Insert ID

| Dialect | Method | Status |
|---------|--------|--------|
| PostgreSQL | `RETURNING id` | Not implemented |
| MySQL | `LAST_INSERT_ID()` | Not implemented |
| SQLite | `last_insert_rowid()` | Works via @@IDENTITY |
| SQL Server | `SCOPE_IDENTITY()` | Works via @@IDENTITY |

---

## Complete Function Translation Matrix

| T-SQL Function | SQLite | PostgreSQL | MySQL | Status |
|----------------|--------|------------|-------|--------|
| `GETDATE()` | `datetime('now')` | `NOW()` | `NOW()` | ✓ Implemented |
| `SYSDATETIME()` | `datetime('now')` | `NOW()` | `NOW()` | ✓ Implemented |
| `GETUTCDATE()` | `datetime('now','utc')` | `NOW() AT TIME ZONE 'UTC'` | `UTC_TIMESTAMP()` | ✓ Implemented |
| `SYSUTCDATETIME()` | `datetime('now','utc')` | `NOW() AT TIME ZONE 'UTC'` | `UTC_TIMESTAMP()` | ✓ Implemented |
| `ISNULL(a,b)` | `IFNULL(a,b)` | `COALESCE(a,b)` | `IFNULL(a,b)` | ✓ Implemented |
| `LEN(s)` | `LENGTH(s)` | `LENGTH(s)` | `CHAR_LENGTH(s)` | ✓ Implemented |
| `DATALENGTH(s)` | `LENGTH(s)` | `OCTET_LENGTH(s)` | `LENGTH(s)` | ✓ Implemented |
| `CHARINDEX(a,b)` | `INSTR(b,a)` | `POSITION(a IN b)` | `LOCATE(a,b)` | ✓ Implemented |
| `SUBSTRING(s,i,n)` | `SUBSTR(s,i,n)` | `SUBSTRING(s,i,n)` | `SUBSTRING(s,i,n)` | ✓ Implemented |
| `NEWID()` | `lower(hex(randomblob(16)))` | `gen_random_uuid()` | `UUID()` | ✓ Implemented |
| `CONVERT(t,v)` | `CAST(v AS t')` | `CAST(v AS t')` | `CAST(v AS t')` | ✓ Partial |
| `DATEADD(p,n,d)` | Complex | `d + INTERVAL 'n p'` | `DATE_ADD(d, INTERVAL n p)` | ✗ Not implemented |
| `DATEDIFF(p,a,b)` | Complex | `EXTRACT(...)` | `TIMESTAMPDIFF(p,a,b)` | ✗ Not implemented |
| `DATEPART(p,d)` | `strftime(...)` | `EXTRACT(p FROM d)` | `EXTRACT(p FROM d)` | ✗ Not implemented |
| `DATENAME(p,d)` | Complex | `TO_CHAR(d, ...)` | `DATE_FORMAT(d, ...)` | ✗ Not implemented |
| `YEAR(d)` | `strftime('%Y',d)` | `EXTRACT(YEAR FROM d)` | `YEAR(d)` | ✗ Not implemented |
| `MONTH(d)` | `strftime('%m',d)` | `EXTRACT(MONTH FROM d)` | `MONTH(d)` | ✗ Not implemented |
| `DAY(d)` | `strftime('%d',d)` | `EXTRACT(DAY FROM d)` | `DAY(d)` | ✗ Not implemented |
| `EOMONTH(d)` | Complex | `(date_trunc('month',d)+interval'1month'-interval'1day')::date` | `LAST_DAY(d)` | ✗ Not implemented |
| `FORMAT(v,f)` | Complex | `TO_CHAR(v,f)` | `DATE_FORMAT(v,f)` | ✗ Not implemented |
| `IIF(c,t,f)` | `CASE WHEN c THEN t ELSE f END` | `CASE WHEN c THEN t ELSE f END` | `IF(c,t,f)` | ✗ Not implemented |
| `CHOOSE(i,...)` | Complex | Complex | `ELT(i,...)` | ✗ Not implemented |
| `STUFF(s,i,l,r)` | Complex | `OVERLAY(s PLACING r FROM i FOR l)` | `INSERT(s,i,l,r)` | ✗ Not implemented |
| `REPLICATE(s,n)` | Complex | `REPEAT(s,n)` | `REPEAT(s,n)` | ✗ Not implemented |
| `SPACE(n)` | `substr('          ',1,n)` | `REPEAT(' ',n)` | `SPACE(n)` | ✗ Not implemented |
| `REVERSE(s)` | Not native | `REVERSE(s)` | `REVERSE(s)` | ✗ Not implemented |
| `QUOTENAME(s)` | `'"'\|\|s\|\|'"'` | `quote_ident(s)` | Complex | ✗ Not implemented |
| `PARSENAME(s,n)` | Complex | Complex | Complex | ✗ Not implemented |
| `PATINDEX(p,s)` | Not native | `POSITION(...)` | Complex | ✗ Not implemented |
| `SOUNDEX(s)` | Not native | `SOUNDEX(s)` | `SOUNDEX(s)` | ✗ Not implemented |
| `DIFFERENCE(a,b)` | Not native | Not native | Not native | ✗ Not implemented |
