# Stored Procedure Execution Architecture

**Version:** 0.4.9  
**Status:** Design document  
**Last updated:** January 2026

---

## Overview

aul executes T-SQL stored procedures against multiple backend databases (SQLite, PostgreSQL, MySQL, SQL Server). This document describes the execution architecture, dialect translation strategy, and the different code paths for each backend.

---

## Current Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         CLIENT CONNECTIONS                          │
├─────────────┬─────────────┬─────────────┬─────────────┬────────────┤
│  TDS 1433   │  PG 5432    │ MySQL 3306  │  HTTP 8080  │ gRPC 9090  │
└─────┬───────┴──────┬──────┴──────┬──────┴──────┬──────┴─────┬──────┘
      └──────────────┴─────────────┴─────────────┴────────────┘
                                   │
                           protocol.Request
                                   │
                    ┌──────────────▼──────────────┐
                    │     server/handler.go       │
                    │   ConnectionHandler.Serve   │
                    └──────────────┬──────────────┘
                                   │
              ┌────────────────────┼────────────────────┐
              │                    │                    │
      RequestQuery          RequestExec          RequestCall
     (ad-hoc SQL)       (EXEC procedure)      (RPC call)
              │                    │                    │
              └────────────────────┼────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │      runtime/runtime.go     │
                    │   Execute() / ExecuteSQL()  │
                    └──────────────┬──────────────┘
                                   │
              ┌────────────────────┴────────────────────┐
              │                                         │
        Interpreted                               JIT-compiled
    (proc.JITCompiled=false)                 (proc.JITCompiled=true)
              │                                         │
   ┌──────────▼──────────┐                 ┌───────────▼───────────┐
   │ runtime/interpreter │                 │      jit/jit.go       │
   │  tsqlruntime.       │                 │   Transpiled Go code  │
   │  NewInterpreter()   │                 │   (plugin or func)    │
   └──────────┬──────────┘                 └───────────┬───────────┘
              │                                         │
              └────────────────────┬────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │   runtime.StorageBackend    │
                    │  Query() / Exec() / GetDB() │
                    └──────────────┬──────────────┘
                                   │
         ┌─────────────────────────┼─────────────────────────┐
         │                         │                         │
   SQLiteStorage            PostgresStorage           SQLServerStorage
   (implemented)              (planned)                 (planned)
```

---

## Execution Paths by Backend

### Path 1: SQLite Backend (Current)

```
T-SQL Source
    │
    ▼
┌─────────────────┐
│  tsqlparser    │  Parse T-SQL to AST
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Interpreter    │  Walk AST, execute statements
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AST Rewriter   │  Transform functions, TOP→LIMIT, types
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQL Normalizer │  String-level fixups (+ → ||, etc.)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQLite via     │  Execute translated SQL
│  database/sql   │
└─────────────────┘
```

**Dialect translations applied:**
- `GETDATE()` → `datetime('now')`
- `ISNULL(a,b)` → `IFNULL(a,b)`
- `LEN(s)` → `LENGTH(s)`
- `TOP N` → `LIMIT N`
- `'a' + 'b'` → `'a' || 'b'` (string concatenation)
- `DATEADD/DATEDIFF/DATEPART` → SQLite date functions
- Type mappings (VARCHAR→TEXT, etc.)

### Path 2: PostgreSQL Backend (Planned)

```
T-SQL Source
    │
    ▼
┌─────────────────┐
│  tsqlparser    │  Parse T-SQL to AST
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Interpreter    │  Walk AST, execute statements
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AST Rewriter   │  Transform for PostgreSQL
└────────┬────────┘  (different from SQLite)
         │
         ▼
┌─────────────────┐
│  PostgreSQL via │  Execute translated SQL
│  database/sql   │
└─────────────────┘
```

**Dialect translations needed:**
- `GETDATE()` → `NOW()`
- `ISNULL(a,b)` → `COALESCE(a,b)`
- `TOP N` → `LIMIT N`
- `CHARINDEX(a,b)` → `POSITION(a IN b)`
- Identity columns → `SERIAL` / `GENERATED`
- String concatenation: `+` works in PG for some types, but `||` is safer

### Path 3: SQL Server Backend (Planned) — Pass-through Mode

When the backend is SQL Server, T-SQL is native. We can skip dialect translation entirely and pass SQL directly to the server.

```
T-SQL Source
    │
    ▼
┌─────────────────┐
│  tsqlparser    │  Parse T-SQL to AST (for validation only)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Syntax Check   │  Catch errors early, before sending to server
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Pass-through   │  Send original T-SQL unchanged
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQL Server via │  Execute native T-SQL
│  database/sql   │
└─────────────────┘
```

**Key insight:** We still parse the T-SQL even though we don't translate it. This provides:

1. **Early syntax validation** — Catch malformed SQL before it reaches the server
2. **Metadata extraction** — Parameter names, types, result set hints
3. **Security scanning** — Detect SQL injection patterns
4. **Logging/auditing** — Structured understanding of what's being executed

The parse step is fast (microseconds) compared to network round-trip to SQL Server (milliseconds), so it adds negligible overhead while providing significant benefits.

**Implementation:**

```go
func (i *interpreter) Execute(ctx context.Context, proc *procedure.Procedure, ...) {
    // Always parse for validation
    program := parser.ParseProgram(proc.Source)
    if len(parser.Errors()) > 0 {
        return nil, syntaxError(parser.Errors())
    }
    
    // Check backend dialect
    if storage.Dialect() == "sqlserver" {
        // Pass-through: execute original source directly
        return storage.ExecRaw(ctx, proc.Source, params)
    }
    
    // Other backends: interpret and translate
    return i.interpretAndExecute(ctx, program, params, storage)
}
```

### Path 4: Native Procedure Deployment (Future)

For production SQL Server deployments, an alternative to pass-through is deploying procedures natively to SQL Server and having aul act as a proxy.

```
┌─────────────────────────────────────────┐
│            aul (proxy mode)             │
│                                         │
│  1. Receive EXEC MyProc @p1=1          │
│  2. Look up MyProc in registry          │
│  3. Forward to SQL Server: EXEC MyProc  │
│  4. Return results to client            │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│           SQL Server                    │
│                                         │
│  CREATE PROCEDURE MyProc ...            │
│  (procedure deployed natively)          │
└─────────────────────────────────────────┘
```

This provides:
- Optimal performance (no interpretation overhead)
- Full SQL Server feature support
- Execution plans cached by SQL Server
- aul handles connection pooling, routing, monitoring

---

## Dialect Translation Layers

Translation happens at two levels:

### Level 1: AST Rewriter (`tsqlruntime/rewriter.go`)

Operates on the parsed AST before serialisation. This is the preferred layer for transformations because it's structurally aware.

**Capabilities:**
- Function rewriting (GETDATE → datetime('now'))
- Clause transformation (TOP → LIMIT)
- Type mapping (VARCHAR(MAX) → TEXT)
- Expression rewriting (complex CASE expansions)

**Example:**
```
Input AST:  SelectStatement{ Top: 10, ... }
Output AST: SelectStatement{ Top: nil, Fetch: 10, ... }
```

### Level 2: SQL Normalizer (`tsqlruntime/dialect.go`)

Operates on the serialised SQL string. This is a fallback for patterns that are difficult to handle at AST level or that slip through.

**Capabilities:**
- Regex-based replacements
- Simple function renaming
- Operator substitution (+ → || for strings)

**Example:**
```
Input:  "SELECT 'a' + @var + 'b'"
Output: "SELECT 'a' || @var || 'b'"
```

### Translation by Backend

| Feature | SQLite | PostgreSQL | MySQL | SQL Server |
|---------|--------|------------|-------|------------|
| GETDATE() | datetime('now') | NOW() | NOW() | (native) |
| ISNULL(a,b) | IFNULL(a,b) | COALESCE(a,b) | IFNULL(a,b) | (native) |
| LEN(s) | LENGTH(s) | LENGTH(s) | CHAR_LENGTH(s) | (native) |
| TOP N | LIMIT N | LIMIT N | LIMIT N | (native) |
| String + | \|\| | \|\| | CONCAT() | (native) |
| CHARINDEX | INSTR (swapped) | POSITION...IN | LOCATE | (native) |
| Identity | AUTOINCREMENT | SERIAL | AUTO_INCREMENT | (native) |
| Temp tables | Regular tables | TEMP TABLE | TEMPORARY | (native) |

---

## Nested Procedure Execution

### The Problem

When a procedure contains `EXEC OtherProcedure`, the interpreter needs to:

1. Recognise the EXEC statement
2. Look up `OtherProcedure` in the procedure registry
3. Load its source
4. Execute it recursively
5. Handle output parameters and return values

Currently, the interpreter returns "procedure execution not supported" for nested EXEC because it has no access to the procedure registry.

### Proposed Solution: ProcedureResolver Interface

```go
// ProcedureResolver looks up procedures by name
type ProcedureResolver interface {
    Resolve(name string) (*procedure.Procedure, error)
}

// Interpreter receives a resolver at construction
func NewInterpreter(db *sql.DB, dialect Dialect, resolver ProcedureResolver) *Interpreter

// In executeExec:
func (i *Interpreter) executeExec(ctx context.Context, s *ast.ExecStatement, result *ExecutionResult) error {
    if s.Procedure != nil {
        procName := s.Procedure.String()
        
        // Try to resolve from registry
        proc, err := i.resolver.Resolve(procName)
        if err != nil {
            return fmt.Errorf("procedure not found: %s", procName)
        }
        
        // Build parameters from EXEC arguments
        params := buildExecParams(s.Parameters)
        
        // Recursive execution (with nesting limit check)
        nestedResult, err := i.Execute(ctx, proc.Source, params)
        if err != nil {
            return err
        }
        
        // Merge results
        result.ResultSets = append(result.ResultSets, nestedResult.ResultSets...)
        result.RowsAffected += nestedResult.RowsAffected
        
        return nil
    }
    // ... existing dynamic SQL handling
}
```

### Nesting Limits

To prevent infinite recursion:

```go
const MaxNestingLevel = 32

func (i *Interpreter) Execute(ctx context.Context, sql string, params map[string]interface{}) (*ExecutionResult, error) {
    i.nestingLevel++
    defer func() { i.nestingLevel-- }()
    
    if i.nestingLevel > MaxNestingLevel {
        return nil, fmt.Errorf("maximum nesting level (%d) exceeded", MaxNestingLevel)
    }
    // ...
}
```

---

## JIT Compilation Path

For frequently-executed procedures, aul can transpile T-SQL to Go code for faster execution.

### When JIT is Triggered

```go
const JITThreshold = 100  // executions before JIT

func (r *Runtime) Execute(ctx context.Context, proc *Procedure, ...) {
    // Check if already JIT-compiled
    if proc.JITCompiled && proc.JITCode != nil {
        return r.executeJIT(ctx, proc, execCtx)
    }
    
    // Interpreted execution
    result, err := r.executeInterpreted(ctx, proc, execCtx)
    
    // Check if we should trigger JIT
    if proc.ExecCount >= JITThreshold && !proc.JITCompiled {
        go r.triggerJIT(proc)  // async compilation
    }
    
    return result, err
}
```

### JIT and Dialect Translation

The JIT-compiled Go code needs to work with different backends. Two approaches:

**Approach A: Generate dialect-specific code**

```go
// Generated for SQLite
func HelloWorld_SQLite(db *sql.DB) ([]ResultSet, error) {
    rows, err := db.Query("SELECT 'Hello, World!' AS Message")
    // ...
}

// Generated for PostgreSQL  
func HelloWorld_Postgres(db *sql.DB) ([]ResultSet, error) {
    rows, err := db.Query("SELECT 'Hello, World!' AS Message")
    // ... (same in this case, but types might differ)
}
```

**Approach B: Generate T-SQL, translate at runtime**

```go
// Generated (dialect-agnostic)
func HelloWorld(rt *Runtime) ([]ResultSet, error) {
    return rt.ExecuteSQL("SELECT 'Hello, World!' AS Message")
}
```

Approach B is simpler and maintains a single generated codebase, but has translation overhead at runtime. Approach A is faster but requires regeneration when switching backends.

**Recommendation:** Start with Approach B for simplicity. The translation overhead is small compared to query execution time.

---

## Output Parameters

### Current State

Output parameters are declared in procedure signatures:

```sql
CREATE PROCEDURE GetCustomerBalance
    @CustomerID INT,
    @Balance DECIMAL(18,2) OUTPUT
AS
BEGIN
    SELECT @Balance = Balance FROM Customers WHERE ID = @CustomerID
END
```

The interpreter needs to:
1. Recognise OUTPUT parameters from the AST
2. Track their values during execution
3. Return them in the execution result

### Implementation

```go
// In executeCreateProcedure:
for _, param := range s.Parameters {
    if param.Output {
        // Track this as an output parameter
        i.outputParams[param.Name] = true
    }
}

// At end of execution:
for name := range i.outputParams {
    if val, ok := i.evaluator.GetVariable(name); ok {
        result.OutputParams[name] = FromValue(val)
    }
}
```

---

## Error Handling

### T-SQL Error Format

T-SQL errors have a specific format:
```
Msg 208, Level 16, State 1, Line 5
Invalid object name 'NonExistentTable'.
```

Components:
- **Msg N** — Error number (system or user-defined, 50000+ for RAISERROR)
- **Level N** — Severity (0-10 informational, 11-16 user errors, 17-25 system)
- **State N** — Arbitrary state for debugging
- **Line N** — Line number in batch

### Implementation

```go
type TSQLError struct {
    Number   int
    Level    int
    State    int
    Line     int
    Message  string
    Procedure string  // if inside a procedure
}

func (e *TSQLError) Error() string {
    return fmt.Sprintf("Msg %d, Level %d, State %d, Line %d\n%s",
        e.Number, e.Level, e.State, e.Line, e.Message)
}
```

### Error Propagation

Errors should be translated to appropriate protocol responses:
- TDS: ERROR token with proper structure
- PostgreSQL: ErrorResponse with SQLSTATE
- HTTP: JSON error object
- gRPC: Status with details

---

## Future Considerations

### 1. Procedure Caching

Cache parsed ASTs to avoid re-parsing on every execution:

```go
type ProcedureCache struct {
    mu    sync.RWMutex
    cache map[string]*CachedProcedure
}

type CachedProcedure struct {
    AST        *ast.Program
    ParsedAt   time.Time
    SourceHash string
}
```

### 2. Execution Plans

For complex procedures, consider caching query plans:

```go
type ExecutionPlan struct {
    Procedure  string
    Backend    string
    Statements []PlannedStatement
    CreatedAt  time.Time
}
```

### 3. Parallel Execution

Some procedure patterns could execute in parallel:

```sql
-- These two queries are independent
SELECT @Count1 = COUNT(*) FROM Table1
SELECT @Count2 = COUNT(*) FROM Table2
-- Could execute concurrently
```

### 4. Streaming Results

For large result sets, stream rows instead of buffering:

```go
type ResultStream interface {
    Next() bool
    Row() []interface{}
    Err() error
    Close() error
}
```

---

## Summary

| Backend | Parse | Translate | Execute |
|---------|-------|-----------|---------|
| SQLite | ✓ | AST rewrite + normalise | database/sql |
| PostgreSQL | ✓ | AST rewrite + normalise | database/sql |
| MySQL | ✓ | AST rewrite + normalise | database/sql |
| SQL Server | ✓ (validation only) | Pass-through | database/sql |

The key architectural principle: **always parse for validation**, then choose execution strategy based on backend dialect. This provides early error detection while allowing optimal execution paths for each backend.
