# Stored Procedure Storage and Translation Options

**Version:** 0.4.9  
**Status:** Analysis report  
**Last updated:** January 2026

---

## The Core Questions

1. Where are stored procedures stored for each backend?
2. Can we translate T-SQL procedures to native PostgreSQL/MySQL procedures?
3. Should we? What are the trade-offs?

---

## Part 1: Where Are Stored Procedures Stored?

### Current State: File-Based Registry

```
/procedures/
├── HelloWorld.sql      # T-SQL source
├── GetCustomer.sql     # T-SQL source  
└── ProcessOrder.sql    # T-SQL source
```

aul loads these at startup into an in-memory registry:

```go
type Procedure struct {
    Name       string
    Source     string    // Original T-SQL
    SourceFile string    // Path to .sql file
    SourceHash string    // For change detection
    // ...
}
```

### Storage Options by Execution Strategy

| Strategy | Procedure Storage | SQL Storage | Pros | Cons |
|----------|-------------------|-------------|------|------|
| **A. Interpret at runtime** | Files → Memory | None (generated per-call) | Simple, flexible | Repeated parse/translate cost |
| **B. Go JIT compilation** | Files → Memory → .go/.so | None | Fast execution | Compilation complexity |
| **C. Native deployment** | Files → Target DB | In database catalog | Optimal performance | Requires deployment step |
| **D. Translated cache** | Files → Memory + cache | Translated SQL cached | Balance of speed/simplicity | Cache invalidation |

### Detailed Analysis

#### Strategy A: Interpret at Runtime (Current)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  .sql file  │ ──► │  Registry   │ ──► │ Interpreter │
│  (T-SQL)    │     │  (memory)   │     │ (per call)  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  Rewrite &  │
                                        │  Execute    │
                                        └─────────────┘
```

**Where is it stored?**
- Source: Filesystem (.sql files)
- Parsed: Memory (registry)
- Translated SQL: Nowhere (regenerated each time)

**Suitable for:** Development, testing, low-volume production

#### Strategy B: Go JIT Compilation

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  .sql file  │ ──► │ Transpiler  │ ──► │  .go file   │
│  (T-SQL)    │     │ (tgpiler)   │     │  (Go code)  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  go build   │
                                        │  (plugin)   │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  .so file   │
                                        │  (binary)   │
                                        └─────────────┘
```

**Where is it stored?**
- Source: Filesystem (.sql files)
- Generated Go: `jit_cache/*.go`
- Compiled plugin: `jit_cache/*.so`
- Translated SQL: Embedded in Go code (hardcoded strings or runtime calls)

**Suitable for:** High-volume production, hot procedures

#### Strategy C: Native Database Deployment

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  .sql file  │ ──► │ Translator  │ ──► │  Native     │
│  (T-SQL)    │     │ (T-SQL→PG)  │     │  procedure  │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  Database   │
                                        │  catalog    │
                                        └─────────────┘
```

**Where is it stored?**
- Source: Filesystem (.sql files)
- Translated: Database system catalog (pg_proc, mysql.proc)

**Suitable for:** Maximum performance, existing DBA workflows

#### Strategy D: Translated SQL Cache

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  .sql file  │ ──► │ Translator  │ ──► │  Cache      │
│  (T-SQL)    │     │             │     │  (memory/   │
└─────────────┘     └─────────────┘     │   disk)     │
                                        └──────┬──────┘
                                               │
                                               ▼
                                        ┌─────────────┐
                                        │  Execute    │
                                        │  cached SQL │
                                        └─────────────┘
```

**Where is it stored?**
- Source: Filesystem (.sql files)
- Translated SQL: Memory cache or disk cache (`cache/*.pgsql`, `cache/*.mysql`)

**Suitable for:** Balance between flexibility and performance

---

## Part 2: Can We Translate T-SQL to Native Procedures?

### Language Comparison

#### T-SQL (SQL Server)

```sql
CREATE PROCEDURE GetCustomerOrders
    @CustomerID INT,
    @StartDate DATE = NULL,
    @OrderCount INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TotalAmount DECIMAL(18,2);
    
    SELECT @OrderCount = COUNT(*), @TotalAmount = SUM(Amount)
    FROM Orders
    WHERE CustomerID = @CustomerID
      AND (@StartDate IS NULL OR OrderDate >= @StartDate);
    
    IF @OrderCount > 0
        SELECT * FROM Orders WHERE CustomerID = @CustomerID;
    ELSE
        RAISERROR('No orders found', 16, 1);
END
```

#### PL/pgSQL (PostgreSQL)

```sql
CREATE OR REPLACE FUNCTION get_customer_orders(
    p_customer_id INT,
    p_start_date DATE DEFAULT NULL,
    OUT p_order_count INT
) RETURNS SETOF orders AS $$
DECLARE
    v_total_amount DECIMAL(18,2);
BEGIN
    SELECT COUNT(*), SUM(amount)
    INTO p_order_count, v_total_amount
    FROM orders
    WHERE customer_id = p_customer_id
      AND (p_start_date IS NULL OR order_date >= p_start_date);
    
    IF p_order_count > 0 THEN
        RETURN QUERY SELECT * FROM orders WHERE customer_id = p_customer_id;
    ELSE
        RAISE EXCEPTION 'No orders found' USING ERRCODE = 'P0001';
    END IF;
END;
$$ LANGUAGE plpgsql;
```

#### MySQL Stored Procedure

```sql
DELIMITER //
CREATE PROCEDURE GetCustomerOrders(
    IN p_customer_id INT,
    IN p_start_date DATE,
    OUT p_order_count INT
)
BEGIN
    DECLARE v_total_amount DECIMAL(18,2);
    
    SELECT COUNT(*), SUM(Amount)
    INTO p_order_count, v_total_amount
    FROM Orders
    WHERE CustomerID = p_customer_id
      AND (p_start_date IS NULL OR OrderDate >= p_start_date);
    
    IF p_order_count > 0 THEN
        SELECT * FROM Orders WHERE CustomerID = p_customer_id;
    ELSE
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'No orders found';
    END IF;
END //
DELIMITER ;
```

### Translation Complexity Matrix

**Important:** This assessment reflects the *remaining* work, not starting from scratch. The `tsqlruntime/rewriter.go` already contains ~1500 lines of dialect translation code with 44 rewrite functions, including implementations for SQLite, PostgreSQL, and MySQL.

#### Already Implemented in tgpiler/tsqlruntime

| T-SQL Feature | SQLite | PostgreSQL | MySQL | Status |
|---------------|--------|------------|-------|--------|
| **Functions** |
| GETDATE() | datetime('now') | NOW() | NOW() | ✓ Done |
| ISNULL(a,b) | IFNULL(a,b) | COALESCE(a,b) | IFNULL(a,b) | ✓ Done |
| LEN(s) | LENGTH(s) | LENGTH(s) | CHAR_LENGTH(s) | ✓ Done |
| CHARINDEX | INSTR (swapped) | POSITION...IN | LOCATE | ✓ Done |
| LEFT/RIGHT | SUBSTR | SUBSTR | LEFT/RIGHT | ✓ Done |
| DATEADD | datetime() modifiers | interval arithmetic | DATE_ADD | ✓ Done |
| DATEDIFF | julianday calc | date subtraction | DATEDIFF | ✓ Done |
| DATEPART | strftime | EXTRACT | EXTRACT | ✓ Done |
| YEAR/MONTH/DAY | strftime | EXTRACT | YEAR/MONTH/DAY | ✓ Done |
| CEILING/FLOOR | CAST expressions | CEIL/FLOOR | CEIL/FLOOR | ✓ Done |
| POWER | multiplication | POWER | POWER | ✓ Done |
| SQRT | sqrt() | SQRT | SQRT | ✓ Done |
| **Clauses** |
| TOP N | LIMIT N | LIMIT N | LIMIT N | ✓ Done |
| CONVERT() | CAST() | CAST() | CAST() | ✓ Done |
| **Types** |
| VARCHAR(MAX) | TEXT | TEXT | LONGTEXT | ✓ Done |
| NVARCHAR | TEXT | VARCHAR | VARCHAR | ✓ Done |
| DATETIME | TEXT | TIMESTAMP | DATETIME | ✓ Done |
| BIT | INTEGER | BOOLEAN | TINYINT | ✓ Done |
| IDENTITY | AUTOINCREMENT | SERIAL | AUTO_INCREMENT | ✓ Done |
| **Operators** |
| String + | \|\| | \|\| | CONCAT() | ✓ Done |

#### Needs Work (Incremental Additions)

| T-SQL Feature | Effort | Notes |
|---------------|--------|-------|
| EOMONTH | Low | Add to specialFunctions map |
| REPLICATE/SPACE | Low | String functions, pattern exists |
| STUFF | Low | String manipulation |
| CHOOSE | Low | CASE expression expansion |
| ISNUMERIC | Low | Pattern matching |
| Additional date formats | Low | Extend existing date handlers |

#### Control Flow (Handled by Interpreter, Not Rewriter)

These don't need SQL translation because they're executed by the interpreter:

| Feature | Status | Notes |
|---------|--------|-------|
| IF...ELSE | ✓ Interpreter | Not translated, executed in Go |
| WHILE | ✓ Interpreter | Not translated, executed in Go |
| BEGIN...END | ✓ Interpreter | Block structure |
| DECLARE/SET | ✓ Interpreter | Variable management |
| TRY...CATCH | ✓ Interpreter | Error handling |
| RETURN | ✓ Interpreter | Control flow |
| GOTO | ✗ Not supported | Would require restructuring |

#### Native Procedure Translation (Already Implemented in tgpiler)

**This capability already exists.** The `tgpiler --dml` switch generates dialect-specific SQL for multiple backends:

```bash
# Generate PostgreSQL-compatible code
tgpiler --dml --sql-dialect postgres input.sql -o output.go

# Generate MySQL-compatible code  
tgpiler --dml --sql-dialect mysql input.sql -o output.go

# Generate SQLite-compatible code
tgpiler --dml --sql-dialect sqlite input.sql -o output.go
```

tgpiler already handles:

| Transformation | Status |
|----------------|--------|
| Parameter placeholders (`@p` → `$1` / `?`) | ✓ Done |
| INSERT RETURNING vs LAST_INSERT_ID() | ✓ Done |
| TOP → LIMIT | ✓ Done |
| Date/string function translation | ✓ Done |
| Type mappings | ✓ Done |

**What "native procedure translation" would add:**

If we wanted to generate actual PL/pgSQL or MySQL stored procedure files (not Go code that emits translated SQL), the additional work would be wrapping the already-translated SQL in procedural syntax:

| Feature | Effort | Notes |
|---------|--------|-------|
| Procedure wrapper syntax | Low | Template around existing output |
| Parameter declaration | Low | Already parsed, just format differently |
| Variable declaration | Low | Already handled |
| Control flow syntax | Medium | IF/WHILE → IF...THEN/WHILE...LOOP |
| Result set return | Medium | RETURN QUERY (PG) vs implicit (MySQL) |
| RAISE/SIGNAL for errors | Low | Pattern already exists |

**Estimated effort:** 300-500 lines of code to add a `--output-format=plpgsql` or `--output-format=mysql-proc` flag, building on the existing dialect translation.

### Translatability Assessment

Given that tgpiler's rewriter already handles most SQL-level translation:

| Category | Status | Notes |
|----------|--------|-------|
| Simple CRUD procedures | ✓ Ready | Rewriter handles SELECT/INSERT/UPDATE/DELETE |
| Business logic with IF/WHILE | ✓ Ready | Interpreter executes, SQL parts translated |
| Error handling | ✓ Ready | Interpreter handles TRY/CATCH |
| Functions (string, date, math) | ✓ Mostly done | 30+ functions already mapped |
| Type conversions | ✓ Done | CAST/CONVERT rewriting in place |
| Multiple result sets | ⚠ Interpreter only | Native PG would need refcursor |
| GOTO statements | ✗ Not supported | Rare in practice |
| Table variables | ⚠ Via temp tables | Converted by interpreter |

**Realistic estimate:** 90%+ of typical T-SQL procedures work today with the interpreter + rewriter. Native procedure generation would be incremental work on top of a solid foundation.

---

## Part 3: Should We Translate? Pros and Cons

### Option 1: Go JIT Only (No Native Procedure Translation)

**Architecture:**
```
T-SQL → Parse → Interpret/JIT to Go → Go calls database with simple SQL
```

**Pros:**
- Single transpilation target (Go)
- Full control over execution
- Works identically across all backends
- Easier to debug (Go code is readable)
- No dependency on target database's procedural language
- Procedures portable across backends without modification

**Cons:**
- Can't leverage database's native procedure optimisations
- All logic runs in aul process, not database
- Network round-trip for each SQL statement
- Can't use database-specific advanced features

**Best for:**
- Microservices architecture (logic in application tier)
- Multi-database deployments
- When procedure portability is paramount

### Option 2: Native SQL Translation (tgpiler --dml)

**This already exists in tgpiler 0.5.2.**

**Architecture:**
```
T-SQL → Parse → tgpiler --dml --sql-dialect=postgres → Go code with PostgreSQL SQL
```

**Pros:**
- Already implemented and tested
- Generates Go code with dialect-specific SQL
- Handles parameter placeholders, functions, types
- Part of the existing tgpiler toolchain

**Cons:**
- Generates Go code, not raw PL/pgSQL procedures
- Still executes from aul process (not database tier)
- Adding `--output-format=plpgsql` would be incremental work

**Best for:**
- Using aul with PostgreSQL/MySQL backends today
- Projects already using tgpiler for transpilation

### Option 3: Hybrid Approach

**Architecture:**
```
T-SQL → Parse → Analyse complexity
                    │
        ┌───────────┴───────────┐
        │                       │
   Simple procedure        Complex procedure
        │                       │
        ▼                       ▼
   Translate to native    JIT to Go
   (PL/pgSQL, MySQL)      (portable)
```

**Pros:**
- Best of both worlds
- Simple procedures get native performance
- Complex procedures still work (via Go)
- Graceful degradation

**Cons:**
- Most complex to implement
- Two execution paths to maintain
- Unpredictable which path will be used

---

## Part 4: Recommendation

### Clarification: Native Export vs Delegation Optimisation

There are two distinct features that involve native database procedures:

| Feature | Name | Who Controls | Purpose | Output |
|---------|------|--------------|---------|--------|
| tgpiler `--output-format=plpgsql` | **Native Export** | Developer | Migration, DBA review | PL/pgSQL files |
| aul automatic delegation | **Delegation Optimisation** | aul engine | Performance | Ephemeral procedures |

**Native Export** is a tgpiler option for developers who want to generate PL/pgSQL or MySQL procedure files for manual review, migration projects, or deployment outside aul. This is discouraged for general use.

**Delegation Optimisation** (covered in Part 9) is an internal aul runtime decision that users never see or configure directly. aul automatically pushes simple procedures to the database as an optimisation, managing their entire lifecycle.

### Primary Recommendation: Go JIT with Automatic Delegation

```
┌─────────────────────────────────────────────────────────────────┐
│                    RECOMMENDED ARCHITECTURE                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────┐                                                │
│  │  T-SQL      │                                                │
│  │  Source     │                                                │
│  └──────┬──────┘                                                │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────┐                                                │
│  │  Parse &    │                                                │
│  │  Validate   │                                                │
│  └──────┬──────┘                                                │
│         │                                                       │
│         ▼                                                       │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │              EXECUTION STRATEGY (aul decides)            │   │
│  ├──────────────┬──────────────┬──────────────────────────┤   │
│  │              │              │                          │   │
│  │  Delegated   │   Go JIT     │      Interpreted         │   │
│  │  (simple)    │   (complex)  │      (fallback)          │   │
│  │              │              │                          │   │
│  │  • Auto      │  • Auto      │  • Always available      │   │
│  │  • Invisible │  • Hot path  │  • Dev/test default      │   │
│  │  • PG/MySQL  │  • All DBs   │  • Cold path             │   │
│  │              │              │                          │   │
│  └──────────────┴──────────────┴──────────────────────────┘   │
│                                                                 │
│  Priority: Delegated > JIT > Interpreted                        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Execution Priority

aul automatically chooses the best execution path:

```go
func (r *Runtime) Execute(ctx context.Context, proc *Procedure, execCtx *ExecContext) (*ExecResult, error) {
    // 1. Try delegated (if available, hash matches, backend supports it)
    if delegated := r.delegation.Get(proc); delegated != nil && delegated.Valid() {
        return r.executeDelegated(ctx, delegated, execCtx)
    }
    
    // 2. Try JIT (if compiled and hash matches)
    if jit := r.jit.Get(proc); jit != nil && jit.Valid() {
        return r.executeJIT(ctx, jit, execCtx)
    }
    
    // 3. Interpreted (always available)
    result, err := r.executeInterpreted(ctx, proc, execCtx)
    
    // 4. Async: consider optimisation for next time
    go r.considerOptimisation(proc)
    
    return result, err
}
```

**Why this order:**
- **Delegated** procedures run entirely in database = minimal network round-trips
- **JIT** reduces parse/interpret overhead but still makes individual SQL calls
- **Interpreted** is always available as fallback

A procedure will not be both delegated and JIT-compiled — if delegation succeeds (simple procedure, supported backend), JIT is unnecessary.

### Rationale

1. **Automatic optimisation** because:
   - Users shouldn't need to configure execution strategy
   - aul knows which procedures are hot and which are simple
   - Fallback is always available if optimisation fails

2. **Delegation for simple procedures** because:
   - Reduces network round-trips dramatically
   - Database can optimise execution plan
   - No interpretation overhead

3. **JIT for complex procedures** because:
   - Eliminates repeated parsing
   - Can't delegate (too complex for native translation)
   - Still portable across backends

### Implementation Priority

| Priority | Feature | Effort | Value | Status |
|----------|---------|--------|-------|--------|
| 1 | Interpret at runtime | Done | High | ✓ Working |
| 2 | SQL dialect translation | Done | High | ✓ tgpiler --dml |
| 3 | Delegation optimisation | Medium | High | Design complete |
| 4 | Go JIT compilation | Medium | High | Scaffolded |
| 5 | SQL caching (avoid re-translation) | Low | Medium | Not started |
| 6 | Native Export (tgpiler flag) | Low | Low | For migration only |

### Configuration

Most execution decisions are automatic, but some tunables are available:

```yaml
# aul.yaml
procedures:
  directory: ./procedures
  
  execution:
    # Delegation optimisation (automatic, invisible)
    delegation:
      enabled: true
      threshold: 100           # Executions before considering
      max_complexity: basic    # simple | basic | moderate
      cleanup_on_shutdown: true
    
    # JIT compilation (automatic)
    jit:
      enabled: true
      threshold: 100
      cache_dir: ./jit_cache
```

### Native Export (tgpiler)

For migration scenarios only, tgpiler can generate native procedure files:

```bash
# Generate PL/pgSQL files for review/deployment outside aul
tgpiler --dml --output-format=plpgsql input.sql -o output.pgsql
```

This is **not** part of normal aul operation. It's a developer tool for:
- Migrating away from aul to native PostgreSQL
- DBA review of what procedures would look like
- Compliance documentation

---

## Part 5: Storage Summary

| Strategy | Source | Parsed AST | Translated SQL | Compiled Code |
|----------|--------|------------|----------------|---------------|
| Interpret | .sql files | Memory | None (per-call) | N/A |
| Go JIT | .sql files | Memory | In .go file | .so plugins |
| Native | .sql files | Memory | Database catalog | N/A |
| SQL Cache | .sql files | Memory | Memory/disk cache | N/A |

### File Locations

```
/aul/
├── procedures/           # Source T-SQL files
│   ├── HelloWorld.sql
│   └── GetCustomer.sql
│
├── jit_cache/            # Go JIT artefacts
│   ├── HelloWorld.go
│   ├── HelloWorld.so
│   └── manifest.json
│
├── sql_cache/            # Translated SQL cache (if enabled)
│   ├── sqlite/
│   │   └── GetCustomer.sql
│   ├── postgres/
│   │   └── GetCustomer.pgsql
│   └── mysql/
│       └── GetCustomer.mysql
│
└── native/               # Native deployment scripts (if enabled)
    ├── deploy_postgres.sql
    └── deploy_mysql.sql
```

---

## Part 6: Version Control Benefits

### The Case for File-Based Storage

Storing procedures as files (rather than in a database catalog) provides significant version control advantages:

| Aspect | File-Based | Database Catalog |
|--------|------------|------------------|
| Git tracking | ✓ Native | Requires export scripts |
| Diff/blame | ✓ Standard tools | Custom tooling needed |
| Code review | ✓ Pull requests | Manual review |
| Branching | ✓ Git branches | Complex (schema branches) |
| Rollback | ✓ `git revert` | Backup/restore |
| CI/CD | ✓ Standard pipelines | Database migration tools |
| Audit trail | ✓ Commit history | Database logs (if enabled) |

### Recommended Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                     DEVELOPMENT WORKFLOW                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   Developer         Git Repository          aul Server          │
│       │                   │                      │              │
│       │  1. Edit .sql     │                      │              │
│       │──────────────────►│                      │              │
│       │                   │                      │              │
│       │  2. Commit        │                      │              │
│       │──────────────────►│                      │              │
│       │                   │                      │              │
│       │  3. Push / PR     │                      │              │
│       │──────────────────►│                      │              │
│       │                   │                      │              │
│       │                   │  4. CI/CD deploy     │              │
│       │                   │─────────────────────►│              │
│       │                   │                      │              │
│       │                   │  5. Hot reload       │              │
│       │                   │─────────────────────►│              │
│       │                   │                      │              │
└─────────────────────────────────────────────────────────────────┘
```

### Procedure Metadata File

Each procedure directory should include metadata for version tracking:

```yaml
# procedures/manifest.yaml
version: "2.1.0"
last_modified: "2026-01-28T14:30:00Z"
procedures:
  - name: GetCustomer
    file: GetCustomer.sql
    version: "1.2.0"
    created: "2025-06-15"
    modified: "2026-01-20"
    author: "jsmith"
    reviewers: ["agarcia", "bwilson"]
    
  - name: ProcessOrder
    file: ProcessOrder.sql
    version: "3.0.1"
    created: "2024-11-01"
    modified: "2026-01-28"
    author: "agarcia"
    breaking_changes:
      - version: "3.0.0"
        description: "Changed @OrderID from INT to BIGINT"
```

### Hot Reload with Version Awareness

```go
type ProcedureVersion struct {
    Name        string
    Version     string
    SourceHash  string
    LoadedAt    time.Time
    Source      string
}

// Registry tracks versions
type Registry struct {
    current  map[string]*Procedure        // Current active version
    history  map[string][]ProcedureVersion // Version history (in-memory)
}

// Reload detects changes and updates
func (r *Registry) Reload(dir string) error {
    // Compare source hashes
    // Log version changes
    // Optionally keep N previous versions in memory for rollback
}
```

---

## Part 7: Hierarchical Storage

### The Problem

Real deployments need to organise procedures by:
- **Database** — Different databases have different procedures
- **Schema** — Within a database, schemas provide namespacing
- **User/Tenant** — Multi-tenant systems need isolation
- **Environment** — Dev/staging/prod may have variants

### Proposed Directory Structure

```
/procedures/
│
├── _global/                      # Shared across all databases
│   ├── utilities/
│   │   ├── FormatDate.sql
│   │   └── ValidateEmail.sql
│   └── manifest.yaml
│
├── master/                       # Database: master
│   ├── dbo/                      # Schema: dbo
│   │   ├── GetServerInfo.sql
│   │   └── manifest.yaml
│   └── admin/                    # Schema: admin
│       ├── BackupDatabase.sql
│       └── manifest.yaml
│
├── salesdb/                      # Database: salesdb
│   ├── dbo/
│   │   ├── GetCustomer.sql
│   │   ├── ProcessOrder.sql
│   │   └── manifest.yaml
│   ├── reporting/
│   │   ├── MonthlySales.sql
│   │   └── manifest.yaml
│   └── _tenant/                  # Tenant-specific overrides
│       ├── acme_corp/
│       │   ├── GetCustomer.sql   # Override for ACME
│       │   └── manifest.yaml
│       └── globex/
│           └── manifest.yaml     # Uses defaults (no overrides)
│
└── inventorydb/                  # Database: inventorydb
    └── dbo/
        ├── CheckStock.sql
        └── manifest.yaml
```

### Resolution Order

When looking up `salesdb.dbo.GetCustomer` for tenant `acme_corp`:

```
1. /procedures/salesdb/_tenant/acme_corp/dbo/GetCustomer.sql  (tenant override)
2. /procedures/salesdb/dbo/GetCustomer.sql                    (database default)
3. /procedures/_global/dbo/GetCustomer.sql                    (global fallback)
4. Not found → error
```

### Registry Keys

Internal registry uses qualified names:

```go
type QualifiedName struct {
    Tenant   string  // Optional: tenant identifier
    Database string  // Required: database name
    Schema   string  // Required: schema name (default: "dbo")
    Name     string  // Required: procedure name
}

func (q QualifiedName) String() string {
    // tenant:database.schema.name or database.schema.name
    if q.Tenant != "" {
        return fmt.Sprintf("%s:%s.%s.%s", q.Tenant, q.Database, q.Schema, q.Name)
    }
    return fmt.Sprintf("%s.%s.%s", q.Database, q.Schema, q.Name)
}

// Registry lookup
func (r *Registry) Lookup(ctx context.Context, name string) (*Procedure, error) {
    // Extract tenant from context if present
    tenant := TenantFromContext(ctx)
    
    // Parse the name
    qn := ParseQualifiedName(name)
    qn.Tenant = tenant
    
    // Try tenant-specific first, then fall back
    return r.lookupWithFallback(qn)
}
```

### Tenant Identification

Tenant ID can come from multiple sources, configured by priority:

```yaml
# aul.yaml
tenancy:
  enabled: true
  
  identification:
    # Priority order - first match wins
    sources:
      - type: header
        name: "X-Tenant-ID"
      - type: tds_property
        name: "app_name"
        pattern: "tenant:(.+)"  # Extract via regex
      - type: connection_string
        param: "tenant"
      - type: certificate
        field: "CN"
        pattern: "(.+)\\.example\\.com"
    
    # Fallback if no tenant identified
    default: null  # null = reject, or specify default tenant
```

**Implementation:**

```go
type TenantIdentifier struct {
    sources       []TenantSource
    defaultTenant string
}

func (ti *TenantIdentifier) Identify(ctx context.Context, conn *Connection) (string, error) {
    for _, source := range ti.sources {
        if tenant := source.Extract(conn); tenant != "" {
            return tenant, nil
        }
    }
    
    if ti.defaultTenant != "" {
        return ti.defaultTenant, nil
    }
    
    return "", ErrTenantNotIdentified
}

// Called early in connection handling
func (h *Handler) Serve(conn *Connection) {
    tenant, err := h.tenantIdentifier.Identify(ctx, conn)
    if err != nil {
        // Reject connection or use default based on config
    }
    ctx = WithTenant(ctx, tenant)
    // ... proceed with tenant context set
}
```

### Schema Validation

When loading procedures, aul validates that file location matches declared schema:

```go
func (l *Loader) LoadProcedure(path string) (*Procedure, error) {
    // Parse path to get expected schema
    // e.g., /procedures/salesdb/reporting/GetData.sql → salesdb, reporting
    expectedDB, expectedSchema, _ := parsePathComponents(path)
    
    // Parse SQL to get declared schema
    proc, err := ParseProcedure(source)
    if err != nil {
        return nil, err
    }
    declaredSchema := proc.Schema  // from CREATE PROCEDURE schema.name
    
    // Validate consistency
    if declaredSchema != "" && declaredSchema != expectedSchema {
        return nil, fmt.Errorf(
            "schema mismatch: file in '%s' directory but declares '%s' schema",
            expectedSchema, declaredSchema)
    }
    
    // If no schema declared, inherit from directory
    if declaredSchema == "" {
        proc.Schema = expectedSchema
    }
    
    return proc, nil
}
```

**Rules:**
1. If procedure declares schema, it must match directory location
2. If procedure doesn't declare schema, inherit from directory
3. Mismatch = load error (fail fast, don't silently use wrong schema)

### Global Procedures and Tenant Context

Procedures in `_global/` are shared code but still execute in the caller's tenant context:

```go
func (r *Registry) lookupWithFallback(qn QualifiedName) (*Procedure, error) {
    // 1. Try tenant-specific
    if qn.Tenant != "" {
        if proc := r.get(qn); proc != nil {
            return proc, nil
        }
    }
    
    // 2. Try database default (no tenant)
    qn.Tenant = ""
    if proc := r.get(qn); proc != nil {
        return proc, nil
    }
    
    // 3. Try global (still executes in caller's tenant context)
    globalQN := QualifiedName{
        Database: "_global",
        Schema:   qn.Schema,
        Name:     qn.Name,
    }
    if proc := r.get(globalQN); proc != nil {
        // Note: proc.Source is shared, but execution context has tenant
        return proc, nil
    }
    
    return nil, ErrProcedureNotFound
}
```

This means:
- A utility procedure in `_global/utilities/FormatDate.sql` can be called by any tenant
- When called, it runs with the caller's tenant context
- If it accesses tables, it accesses the tenant's tables (not shared tables)

### Configuration

```yaml
# aul.yaml
procedures:
  root_directory: ./procedures
  
  hierarchy:
    # Enable tenant isolation
    tenant_isolation: true
    tenant_directory: "_tenant"
    
    # Global procedures (shared utilities)
    global_directory: "_global"
    
    # Default schema when not specified
    default_schema: "dbo"
    
    # Resolution order
    resolution:
      - tenant    # Check tenant override first
      - database  # Then database default
      - global    # Finally global fallback
```

### Database File Storage

For file-based backends like SQLite, the database files should follow a parallel hierarchy to the procedure files:

```
/data/
├── _global/                      # Global databases (if any)
│   └── shared.db
│
├── master/                       # Database: master
│   └── master.db
│
├── salesdb/                      # Database: salesdb
│   ├── salesdb.db                # Default database file
│   └── _tenant/                  # Tenant-specific databases
│       ├── acme_corp/
│       │   └── salesdb.db        # ACME's isolated copy
│       └── globex/
│           └── salesdb.db        # Globex's isolated copy
│
└── inventorydb/                  # Database: inventorydb
    └── inventorydb.db
```

**Key points:**

| Aspect | Procedures | SQLite Databases |
|--------|------------|------------------|
| Root directory | `/procedures/` | `/data/` |
| Hierarchy | database/schema/proc.sql | database/database.db |
| Tenant isolation | `_tenant/{tenant}/` | `_tenant/{tenant}/` |
| Version control | ✓ Yes (source code) | ✗ No (binary data) |
| Backup strategy | Git | Filesystem/cloud backup |

**Configuration:**

```yaml
# aul.yaml
storage:
  type: sqlite
  
  # Root directory for database files
  data_directory: ./data
  
  # Follow same hierarchy as procedures
  hierarchy:
    enabled: true
    tenant_isolation: true
    tenant_directory: "_tenant"
  
  # Per-database settings
  databases:
    master:
      file: master.db
      # Or explicit path: /var/lib/aul/master.db
      
    salesdb:
      file: salesdb.db
      # Tenant databases created automatically under _tenant/
      
  # Default settings for all databases
  defaults:
    journal_mode: WAL
    busy_timeout: 5000
    cache_size: -64000  # 64MB
  
  # Tenant database creation policy
  tenant_databases:
    auto_create: true       # Create on first access (default)
    # auto_create: false    # Require manual provisioning
```

**Tenant database resolution and auto-creation:**

When a request comes in for tenant `acme_corp` accessing `salesdb`:

```go
func (s *SQLiteStorage) resolveDatabasePath(database, tenant string) string {
    if tenant != "" && s.config.TenantIsolation {
        // /data/salesdb/_tenant/acme_corp/salesdb.db
        return filepath.Join(s.dataDir, database, "_tenant", tenant, database+".db")
    }
    // /data/salesdb/salesdb.db
    return filepath.Join(s.dataDir, database, database+".db")
}

func (s *SQLiteStorage) GetDB(ctx context.Context, database string) (*sql.DB, error) {
    tenant := TenantFromContext(ctx)
    path := s.resolveDatabasePath(database, tenant)
    
    // Check if database exists
    if _, err := os.Stat(path); os.IsNotExist(err) {
        if !s.config.TenantDatabases.AutoCreate {
            return nil, fmt.Errorf("tenant database not found: %s (auto_create disabled)", path)
        }
        
        // Auto-create tenant database
        if err := s.createTenantDatabase(path, database); err != nil {
            return nil, fmt.Errorf("failed to create tenant database: %w", err)
        }
        
        s.logger.Info("created tenant database",
            "tenant", tenant,
            "database", database,
            "path", path)
    }
    
    return s.openDatabase(path)
}

func (s *SQLiteStorage) createTenantDatabase(path, database string) error {
    // Ensure directory exists
    dir := filepath.Dir(path)
    if err := os.MkdirAll(dir, 0755); err != nil {
        return err
    }
    
    // Create empty database with default schema
    db, err := sql.Open("sqlite3", path)
    if err != nil {
        return err
    }
    defer db.Close()
    
    // Apply default settings
    for _, pragma := range []string{
        fmt.Sprintf("PRAGMA journal_mode=%s", s.config.Defaults.JournalMode),
        fmt.Sprintf("PRAGMA busy_timeout=%d", s.config.Defaults.BusyTimeout),
        fmt.Sprintf("PRAGMA cache_size=%d", s.config.Defaults.CacheSize),
    } {
        if _, err := db.Exec(pragma); err != nil {
            return err
        }
    }
    
    return nil
}
```

**For PostgreSQL/MySQL backends:**

The hierarchy maps to database/schema names rather than files:

```yaml
storage:
  type: postgres
  
  hierarchy:
    # Tenant isolation via schema prefix
    tenant_isolation: true
    tenant_schema_prefix: "tenant_"  # tenant_acme_corp.customers
    
    # Or via separate databases
    tenant_database_prefix: ""       # acme_corp_salesdb
```

---

## Part 8: Future ACL Considerations

### Design Principles for Forward Compatibility

Even though ACLs are not implemented yet, we should design with these principles:

1. **Identity propagation** — Always know who is executing
2. **Context enrichment** — Carry permission-relevant data through the stack
3. **Hook points** — Clear places where ACL checks can be inserted
4. **Metadata support** — Store permission hints in procedure definitions

### Execution Context (ACL-Ready)

```go
// ExecContext carries identity and permission context
type ExecContext struct {
    // Current fields
    SessionID    string
    Database     string
    Parameters   map[string]interface{}
    
    // Identity (populated now, enforced later)
    Principal    *Principal  // Who is executing
    
    // Permission context (populated now, enforced later)
    Permissions  *PermissionContext
    
    // Audit context
    AuditTrail   *AuditContext
}

// Principal represents the executing identity
type Principal struct {
    Type       string   // "user", "service", "system"
    ID         string   // Unique identifier
    Name       string   // Display name
    Roles      []string // Role memberships
    Tenant     string   // Tenant identifier (multi-tenant)
    Attributes map[string]string // Extensible attributes
}

// PermissionContext holds permission-relevant data
// Actual enforcement is future work, but we capture the data now
type PermissionContext struct {
    // Effective permissions (to be computed by ACL system)
    // For now, this is nil or a stub that allows everything
    EffectivePermissions []string
    
    // Permission evaluation is deferred
    Evaluator PermissionEvaluator // Interface, nil = allow all
}

// PermissionEvaluator interface for future ACL implementation
type PermissionEvaluator interface {
    // CanExecute checks if principal can execute a procedure
    CanExecute(principal *Principal, procedure *Procedure) (bool, error)
    
    // CanAccess checks if principal can access a database/schema
    CanAccess(principal *Principal, database, schema string) (bool, error)
    
    // FilterResults filters result sets based on row-level security
    // (advanced, may not implement)
    FilterResults(principal *Principal, results []ResultSet) ([]ResultSet, error)
}
```

### Procedure Metadata (ACL Hints)

```sql
-- Procedures can declare permission hints in comments
-- These are parsed and stored but not enforced yet

/*
 * @procedure GetCustomer
 * @permission EXECUTE ON salesdb.dbo.GetCustomer
 * @permission SELECT ON salesdb.dbo.Customers
 * @role sales_reader, sales_admin
 * @deny guest
 */
CREATE PROCEDURE dbo.GetCustomer
    @CustomerID INT
AS
BEGIN
    SELECT * FROM Customers WHERE ID = @CustomerID
END
```

Or in manifest:

```yaml
# procedures/salesdb/dbo/manifest.yaml
procedures:
  - name: GetCustomer
    file: GetCustomer.sql
    
    # ACL hints (not enforced yet, but stored)
    permissions:
      required:
        - "EXECUTE ON salesdb.dbo.GetCustomer"
        - "SELECT ON salesdb.dbo.Customers"
      roles:
        allow: ["sales_reader", "sales_admin", "admin"]
        deny: ["guest"]
      
    # Row-level security hint
    row_security:
      enabled: true
      column: "TenantID"
      source: "principal.tenant"
```

### Hook Points for Future ACL

```go
// In runtime/runtime.go

func (r *Runtime) Execute(ctx context.Context, proc *Procedure, execCtx *ExecContext) (*ExecResult, error) {
    // HOOK: Pre-execution ACL check
    if err := r.checkExecutePermission(execCtx, proc); err != nil {
        return nil, err
    }
    
    // ... execution ...
    
    // HOOK: Post-execution result filtering
    result = r.filterResults(execCtx, result)
    
    return result, nil
}

// Current implementation: allow all
func (r *Runtime) checkExecutePermission(execCtx *ExecContext, proc *Procedure) error {
    if execCtx.Permissions == nil || execCtx.Permissions.Evaluator == nil {
        // No ACL configured, allow all
        return nil
    }
    
    allowed, err := execCtx.Permissions.Evaluator.CanExecute(execCtx.Principal, proc)
    if err != nil {
        return err
    }
    if !allowed {
        return &PermissionDeniedError{
            Principal: execCtx.Principal,
            Procedure: proc.QualifiedName(),
        }
    }
    return nil
}
```

### Audit Trail (Useful Even Without ACL)

Even before ACL enforcement, audit logging is valuable:

```go
type AuditEntry struct {
    Timestamp   time.Time
    SessionID   string
    Principal   *Principal  // Who
    Action      string      // "EXECUTE", "QUERY", etc.
    Target      string      // Procedure or SQL
    Database    string
    Parameters  map[string]interface{} // Sanitised
    Success     bool
    Error       string
    Duration    time.Duration
    RowsAffected int64
}

// Audit logger interface
type AuditLogger interface {
    Log(entry AuditEntry) error
}

// Usage in runtime
func (r *Runtime) Execute(...) {
    entry := AuditEntry{
        Timestamp: time.Now(),
        Principal: execCtx.Principal,
        Action:    "EXECUTE",
        Target:    proc.QualifiedName(),
        // ...
    }
    defer func() {
        entry.Duration = time.Since(entry.Timestamp)
        r.auditLogger.Log(entry)
    }()
    
    // ... execution ...
}
```

### Summary: ACL-Ready Design

| Component | Current State | Future ACL State |
|-----------|---------------|------------------|
| Principal | Captured in ExecContext | Used for permission checks |
| Permissions | Nil (allow all) | Populated by ACL system |
| Hooks | Present but pass-through | Call ACL evaluator |
| Metadata | Stored in manifest | Loaded into ACL rules |
| Audit | Can be enabled now | Required for compliance |

The key is that **we capture the data now** (principal, permissions context, audit trail) even though **we don't enforce anything yet**. This means:

1. No breaking changes when ACL is added
2. Audit logging works immediately
3. Permission metadata is stored and ready
4. Integration points are clearly defined

---

## Part 9: Delegated Procedure Optimisation

### Principle: aul Owns the Procedures

Native database procedures are **never** the source of truth. They are:
- Generated automatically by aul as an optimisation
- Considered throwaway/ephemeral
- Managed entirely by aul (create, update, delete)
- Invisible to users and DBAs (implementation detail)

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCE OF TRUTH                              │
│                                                                 │
│   /procedures/salesdb/dbo/GetCustomer.sql  (T-SQL)              │
│                                                                 │
│   • Version controlled in Git                                   │
│   • Loaded by aul at startup                                    │
│   • Never modified by optimisation                              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ aul decides to optimise
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                   DELEGATED (EPHEMERAL)                          │
│                                                                 │
│   PostgreSQL: _aul_proc_GetCustomer_a1b2c3()                    │
│                                                                 │
│   • Generated by aul                                            │
│   • Hash suffix for versioning                                  │
│   • Dropped and recreated on source change                      │
│   • Dropped on aul shutdown (optional)                          │
│   • Never edited by humans                                      │
└─────────────────────────────────────────────────────────────────┘
```

### When to Delegate

aul decides to delegate based on analysis, not user configuration:

```go
type DelegationDecision struct {
    ShouldDelegate bool
    Reason         string
    Complexity     int      // Estimated cost of interpretation
    Frequency      int      // Execution count
    Backend        string   // Target database
}

func (r *Runtime) shouldDelegate(proc *Procedure, stats *ProcStats) DelegationDecision {
    // Never delegate if backend doesn't support procedures
    if !r.storage.SupportsProcedures() {
        return DelegationDecision{ShouldDelegate: false, Reason: "backend lacks procedure support"}
    }
    
    // Never delegate complex procedures (GOTO, dynamic SQL, etc.)
    if proc.HasUnsupportedFeatures() {
        return DelegationDecision{ShouldDelegate: false, Reason: "unsupported features"}
    }
    
    // Only delegate simple, frequently-executed procedures
    if proc.Complexity <= ComplexitySimple && stats.ExecutionCount >= DelegationThreshold {
        return DelegationDecision{
            ShouldDelegate: true,
            Reason:         "simple procedure with high execution frequency",
            Complexity:     proc.Complexity,
            Frequency:      stats.ExecutionCount,
        }
    }
    
    return DelegationDecision{ShouldDelegate: false, Reason: "does not meet criteria"}
}
```

### Complexity Classification

| Complexity | Description | Delegate? |
|------------|-------------|-----------|
| **Simple** | Single SELECT/INSERT/UPDATE/DELETE, no control flow | ✓ Yes |
| **Basic** | Simple IF/ELSE, single result set | ✓ Yes |
| **Moderate** | WHILE loops, multiple statements, temp tables | Maybe |
| **Complex** | Cursors, dynamic SQL, TRY/CATCH, multiple result sets | ✗ No |
| **Unsupported** | GOTO, CLR, extended procedures | ✗ Never |

### Backend Support

| Backend | Delegation Support | Notes |
|---------|-------------------|-------|
| PostgreSQL | ✓ Yes | CREATE FUNCTION with PL/pgSQL |
| MySQL | ✓ Yes | CREATE PROCEDURE |
| SQLite | ✗ No | SQLite has no stored procedure support |
| SQL Server | ✗ N/A | Use pass-through mode instead |

For SQLite backends, delegation is automatically disabled. The JIT path remains available for optimisation.

### Naming Convention

Delegated procedures use a naming scheme that:
- Identifies them as aul-managed
- Includes instance ID for multi-instance deployments
- Includes a version hash for change detection
- Avoids collision with user procedures

```
_aul_proc_{instance}_{schema}_{name}_{hash}

Examples:
  _aul_proc_i1_dbo_GetCustomer_a1b2c3d4
  _aul_proc_i2_dbo_GetCustomer_a1b2c3d4  (different instance)
  _aul_proc_i1_dbo_ProcessOrder_e5f6g7h8
```

Each aul instance manages only its own procedures (`_aul_proc_{its_instance_id}_*`). This avoids coordination complexity when multiple instances share a database.

### Lifecycle Management

```go
type DelegatedProcedure struct {
    SourceName     string    // Original procedure name
    SourceHash     string    // Hash of source T-SQL
    DelegatedName  string    // Name in target database
    Backend        string    // postgres, mysql
    InstanceID     string    // aul instance that created this
    CreatedAt      time.Time
    LastUsed       time.Time
    ExecutionCount int64
}

type DelegationManager struct {
    instanceID     string
    delegated      map[string]*DelegatedProcedure
    inProgress     sync.Map  // Prevents race conditions
    recentFailures map[string]*DelegationAttempt
    storage        StorageBackend
    registry       *Registry
    mu             sync.RWMutex
}

type DelegationAttempt struct {
    Procedure   string
    AttemptedAt time.Time
    Error       string
    RetryCount  int
}
```

**Startup cleanup:**

On startup, aul scans for orphaned delegated procedures:

```go
func (dm *DelegationManager) StartupCleanup() error {
    // Find all _aul_proc_{our_instance}* in database
    pattern := fmt.Sprintf("_aul_proc_%s_%%", dm.instanceID)
    existing, err := dm.storage.ListProcedures(pattern)
    if err != nil {
        return err
    }
    
    for _, dbProc := range existing {
        // Parse name to extract original procedure name and hash
        sourceName, hash := parseAulProcName(dbProc.Name)
        
        // Check if we have this procedure with matching hash
        proc := dm.registry.Get(sourceName)
        if proc == nil || proc.SourceHash != hash {
            // Orphaned or stale - drop it
            dm.drop(dbProc)
            dm.logger.Info("cleaned up stale delegated procedure",
                "name", dbProc.Name,
                "reason", ternary(proc == nil, "source removed", "source changed"))
        }
    }
    return nil
}
```

**Race condition prevention:**

```go
func (dm *DelegationManager) Create(proc *Procedure) {
    key := proc.QualifiedName()
    
    // Check if creation already in progress
    done := make(chan struct{})
    if existing, loaded := dm.inProgress.LoadOrStore(key, done); loaded {
        // Another goroutine is creating this - wait for it
        <-existing.(chan struct{})
        return
    }
    
    // We're responsible for creating
    defer func() {
        close(done)
        dm.inProgress.Delete(key)
    }()
    
    // Check recent failures (exponential backoff)
    if attempt := dm.recentFailures[key]; attempt != nil {
        if time.Since(attempt.AttemptedAt) < dm.retryBackoff(attempt.RetryCount) {
            return  // Too soon to retry
        }
    }
    
    // Double-check it wasn't created while we were setting up
    dm.mu.RLock()
    if _, exists := dm.delegated[key]; exists {
        dm.mu.RUnlock()
        return
    }
    dm.mu.RUnlock()
    
    // Actually create
    if err := dm.createInDatabase(proc); err != nil {
        dm.handleCreationError(proc, err)
        return
    }
    
    // Record success
    dm.mu.Lock()
    dm.delegated[key] = &DelegatedProcedure{
        SourceName:    proc.QualifiedName(),
        SourceHash:    proc.SourceHash,
        DelegatedName: dm.delegatedName(proc),
        Backend:       dm.storage.Dialect(),
        InstanceID:    dm.instanceID,
        CreatedAt:     time.Now(),
    }
    delete(dm.recentFailures, key)
    dm.mu.Unlock()
}

func (dm *DelegationManager) handleCreationError(proc *Procedure, err error) {
    key := proc.QualifiedName()
    
    dm.logger.Warn("delegation failed, will use interpreted path",
        "procedure", key,
        "error", err)
    
    // Record failure for backoff
    dm.mu.Lock()
    attempt := dm.recentFailures[key]
    if attempt == nil {
        attempt = &DelegationAttempt{Procedure: key}
    }
    attempt.AttemptedAt = time.Now()
    attempt.Error = err.Error()
    attempt.RetryCount++
    dm.recentFailures[key] = attempt
    dm.mu.Unlock()
    
    // Expose via metrics
    dm.metrics.DelegationFailures.Inc()
}

func (dm *DelegationManager) retryBackoff(retryCount int) time.Duration {
    // Exponential backoff: 1m, 5m, 30m, 2h, 24h max
    backoffs := []time.Duration{
        1 * time.Minute, 5 * time.Minute, 30 * time.Minute,
        2 * time.Hour, 24 * time.Hour,
    }
    if retryCount >= len(backoffs) {
        return backoffs[len(backoffs)-1]
    }
    return backoffs[retryCount]
}
```

**Sync and cleanup:**

```go
// Sync ensures delegated procedures match source
func (dm *DelegationManager) Sync(registry *Registry) error {
    for _, proc := range registry.All() {
        existing := dm.delegated[proc.QualifiedName()]
        
        if existing == nil {
            continue  // Not delegated yet
        }
        
        if existing.SourceHash != proc.SourceHash {
            // Source changed - drop old, let next execution recreate
            dm.drop(existing)
        }
    }
    
    // Drop delegated procedures whose source no longer exists
    for name, delegated := range dm.delegated {
        if !registry.Exists(name) {
            dm.drop(delegated)
        }
    }
    
    return nil
}

// Cleanup removes all delegated procedures (e.g., on shutdown)
func (dm *DelegationManager) Cleanup() error {
    if !dm.config.CleanupOnShutdown {
        return nil
    }
    for _, delegated := range dm.delegated {
        dm.drop(delegated)
    }
    return nil
}
```

**Hot reload behaviour:**

When source changes during hot reload:
1. Old delegated procedure is dropped
2. In-flight executions using old procedure complete normally (database doesn't drop procedures with active sessions)
3. New executions use interpreted path until delegation recreated
4. Next execution triggers async delegation creation

### Execution Path

```go
func (r *Runtime) Execute(ctx context.Context, proc *Procedure, execCtx *ExecContext) (*ExecResult, error) {
    // Check if this procedure is delegated
    if delegated := r.delegation.Get(proc.QualifiedName()); delegated != nil {
        // Verify hash still matches (source hasn't changed)
        if delegated.SourceHash == proc.SourceHash {
            // Execute via delegated procedure
            return r.executeDelegated(ctx, delegated, execCtx)
        }
        // Hash mismatch - fall through to interpreted, trigger async resync
        go r.delegation.Resync(proc)
    }
    
    // Check if we should delegate (async, don't block this execution)
    if decision := r.shouldDelegate(proc, r.stats.Get(proc.Name)); decision.ShouldDelegate {
        go r.delegation.Create(proc)
    }
    
    // Execute interpreted
    return r.executeInterpreted(ctx, proc, execCtx)
}

func (r *Runtime) executeDelegated(ctx context.Context, dp *DelegatedProcedure, execCtx *ExecContext) (*ExecResult, error) {
    // Build CALL/EXEC statement for target database
    var callSQL string
    switch dp.Backend {
    case "postgres":
        callSQL = fmt.Sprintf("SELECT * FROM %s($1, $2, ...)", dp.DelegatedName)
    case "mysql":
        callSQL = fmt.Sprintf("CALL %s(?, ?, ...)", dp.DelegatedName)
    }
    
    // Execute and return results
    return r.storage.Query(ctx, callSQL, execCtx.Parameters)
}
```

### Configuration

```yaml
# aul.yaml
procedures:
  delegation:
    # Master switch for delegation optimisation
    enabled: true
    
    # Minimum executions before considering delegation
    threshold: 100
    
    # Maximum complexity level to delegate
    max_complexity: basic  # simple | basic | moderate
    
    # Cleanup delegated procedures on shutdown
    cleanup_on_shutdown: true
    
    # Prefix for delegated procedure names
    name_prefix: "_aul_proc_"
    
    # Schema for delegated procedures (isolate from user objects)
    schema: "_aul_internal"
```

### Observability

```go
type ExecutionMetrics struct {
    // Counters
    InterpretedCount  int64
    JITCount          int64
    DelegatedCount    int64
    DelegationFailures int64
    
    // Latency histograms (per procedure, per path)
    Latencies map[string]*PathLatencies
}

type PathLatencies struct {
    Interpreted *LatencyHistogram
    JIT         *LatencyHistogram
    Delegated   *LatencyHistogram
}

type LatencyHistogram struct {
    Count   int64
    Sum     time.Duration
    Buckets []int64  // <1ms, <5ms, <10ms, <50ms, <100ms, <500ms, >500ms
}

// Record latency after each execution
func (r *Runtime) recordLatency(proc string, path string, duration time.Duration) {
    r.metrics.Latencies[proc].Record(path, duration)
}
```

**Exposed via metrics endpoint:**

```
# GET /metrics

# Counters
aul_executions_total{path="interpreted"} 3400
aul_executions_total{path="jit"} 45000
aul_executions_total{path="delegated"} 125000
aul_delegation_failures_total 3

# Delegation status
aul_delegation_active{backend="postgres"} 15

# Latency histograms (per procedure)
aul_execution_duration_seconds_bucket{procedure="GetCustomer",path="delegated",le="0.001"} 120000
aul_execution_duration_seconds_bucket{procedure="GetCustomer",path="delegated",le="0.005"} 124500
aul_execution_duration_seconds_bucket{procedure="GetCustomer",path="interpreted",le="0.001"} 2800
aul_execution_duration_seconds_bucket{procedure="GetCustomer",path="interpreted",le="0.005"} 3200
```

**Optional auto-revocation:**

If delegation is consistently slower than interpreted (unusual, but possible with network issues), aul can auto-revoke:

```go
func (dm *DelegationManager) maybeAutoRevoke(proc string) {
    if !dm.config.AutoRevoke {
        return
    }
    
    latencies := dm.metrics.Latencies[proc]
    if latencies.Delegated.Count < 100 {
        return  // Not enough data
    }
    
    interpP50 := latencies.Interpreted.Percentile(50)
    delegP50 := latencies.Delegated.Percentile(50)
    
    // If delegation is 20%+ slower, revoke
    if delegP50 > interpP50*1.2 {
        dm.logger.Warn("revoking delegation - interpreted path faster",
            "procedure", proc,
            "interpreted_p50", interpP50,
            "delegated_p50", delegP50)
        dm.Revoke(proc)
    }
}
```

### Important Guarantees

1. **Source is always authoritative** — If source and delegated differ, source wins
2. **Delegation is invisible** — Users never interact with delegated procedures
3. **Fallback is always available** — Interpreted path works regardless of delegation
4. **No manual intervention** — aul creates, updates, and drops automatically
5. **Clean shutdown** — Option to remove all delegated procedures on exit
6. **No data loss** — Delegation is pure optimisation, never affects correctness

### What This Is NOT

| This is NOT | Because |
|-------------|---------|
| A migration tool | Source stays in aul, not migrated to database |
| A deployment mechanism | Users don't deploy procedures to database |
| A way to edit procedures in the database | Database copy is read-only, auto-generated |
| Permanent | Delegated procedures can be dropped at any time |
| Required | System works fine without delegation |

---

## Conclusion

1. **Stored procedures are stored in files**, loaded into memory at startup
2. **Translation to native SQL dialects already exists** via tgpiler --dml
3. **Go JIT is recommended** as the primary execution strategy
4. **Native PL/pgSQL/MySQL file output** would be incremental work on tgpiler
5. **SQL caching** provides a good middle ground for interpreted execution
6. **File-based storage enables version control** — Git tracking, code review, CI/CD
7. **Hierarchical storage** supports multi-database, multi-tenant, multi-schema deployments
8. **ACL-ready design** captures identity and permission context now, enforces later
9. **Delegated optimisation** allows aul to push simple procedures to the database as an automatic, invisible optimisation while retaining full ownership

The key architectural principles:

- **aul is the source of truth** — Procedures live in version-controlled files, not database catalogs
- **Optimisation is automatic** — Delegation to native procedures is an engine decision, not user configuration
- **Delegated code is ephemeral** — aul creates, updates, and drops native procedures as needed
- **Fallback is always available** — Interpreted execution works regardless of delegation status

This provides the performance benefits of native database procedures when appropriate, while maintaining the portability, debuggability, and version control benefits of file-based procedure management.
