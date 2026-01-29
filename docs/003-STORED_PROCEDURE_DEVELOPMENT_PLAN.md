# Stored Procedure Development Plan

**Version:** 0.5.0  
**Status:** Development roadmap  
**Last updated:** January 2026

---

## Overview

This document outlines the phased implementation plan for stored procedure support in aul, following the architecture defined in `PROCEDURE_STORAGE_AND_TRANSLATION.md`.

### Current State (v0.5.0)

| Component | Status |
|-----------|--------|
| T-SQL parsing (tsqlparser) | ✓ Complete |
| Basic interpreter (tsqlruntime) | ✓ Complete |
| SQL dialect rewriting | ✓ Complete |
| Procedure registry | ✓ Complete (hierarchical) |
| Procedure execution | ✓ Complete (nested EXEC, output params) |
| SQLite backend | ✓ Complete |
| TDS protocol | ✓ Complete |
| Hot reload | ✓ Complete |

### Target State

| Component | Target Phase |
|-----------|--------------|
| Hierarchical procedure storage | Phase 1 |
| Schema validation on load | Phase 1 |
| Nested EXEC support | Phase 1 |
| Output parameters | Phase 1 |
| Tenant identification | Phase 2 |
| Tenant-isolated SQLite databases | Phase 2 |
| Global procedures | Phase 2 |
| Delegation optimisation | Phase 3 |
| Go JIT compilation | Phase 4 |
| Performance monitoring | Phase 4 |
| ACL hooks (no enforcement) | Phase 5 |

---

## Phase 1: Core Procedure Infrastructure

**Goal:** Robust procedure loading, validation, and execution for single-tenant deployments.

**Duration:** 2-3 weeks

### 1.1 Hierarchical Procedure Storage

**Files:** `procedure/loader.go`, `procedure/registry.go`

```
/procedures/
├── master/
│   └── dbo/
│       └── GetServerInfo.sql
├── salesdb/
│   ├── dbo/
│   │   ├── GetCustomer.sql
│   │   └── manifest.yaml
│   └── reporting/
│       └── MonthlySales.sql
```

**Tasks:**
- [x] Implement directory walker that respects `database/schema/` structure
- [ ] Parse `manifest.yaml` for procedure metadata (version, author, etc.) — deferred
- [x] Build qualified names: `database.schema.name`
- [x] Update registry to use `QualifiedName` keys
- [x] Add `_global/` directory support (shared procedures)

**Tests:**
- [x] Load procedures from nested directories
- [x] Verify qualified name construction
- [ ] Test manifest parsing — deferred
- [x] Test global procedure fallback

### 1.2 Schema Validation

**Files:** `procedure/loader.go`

**Tasks:**
- [x] Extract declared schema from `CREATE PROCEDURE schema.name`
- [x] Compare declared schema vs directory location
- [x] Fail fast on mismatch with clear error message
- [x] If no schema declared, inherit from directory

**Tests:**
- [x] Matching schema passes
- [x] Mismatched schema fails with descriptive error
- [x] Missing schema inherits from directory

### 1.3 Nested EXEC Support

**Files:** `tsqlruntime/interpreter.go`

**Tasks:**
- [x] Define `ProcedureResolver` interface
- [x] Pass resolver to interpreter at construction
- [x] In `executeExec`, check if procedure name (not dynamic SQL)
- [x] Look up procedure via resolver
- [x] Execute recursively with nesting level check
- [x] Handle output parameters from nested calls

```go
type ProcedureResolver interface {
    Resolve(ctx context.Context, name string) (*Procedure, error)
}
```

**Tests:**
- [x] Procedure A calls procedure B
- [x] Three-level nesting
- [x] Nesting limit exceeded (max 32)
- [x] Nested procedure not found error

### 1.4 Output Parameters

**Files:** `tsqlruntime/interpreter.go`, `runtime/interpreter.go`

**Tasks:**
- [x] Track OUTPUT parameters in `CreateProcedureStatement`
- [x] After execution, extract final values from evaluator
- [x] Return in `ExecResult.OutputParams`
- [x] Wire through to TDS response

**Tests:**
- [x] Simple OUTPUT parameter
- [ ] Multiple OUTPUT parameters — needs dedicated test
- [ ] OUTPUT with default value — needs dedicated test
- [x] Nested procedure with OUTPUT

### 1.5 Hot Reload

**Files:** `procedure/registry.go`, `server/server.go`

**Tasks:**
- [x] File watcher on procedures directory
- [x] Detect changes via source hash comparison
- [x] Reload changed procedures without restart
- [x] Log reload events
- [x] Signal to invalidate any cached state (JIT cleared on reload)

**Tests:**
- [x] Modify procedure file, verify reload
- [x] Add new procedure file, verify discovery
- [x] Delete procedure file, verify removal from registry

### Phase 1 Deliverables

- [x] All tests passing
- [ ] Example: multi-database procedure setup — pending
- [x] Documentation updates
- [x] Version bump to 0.5.0
- [ ] Documentation updates
- [ ] Version bump to 0.5.0

---

## Phase 2: Multi-Tenancy

**Goal:** Support tenant-isolated procedure resolution and database access.

**Duration:** 2-3 weeks

**Prerequisite:** Phase 1 complete

### 2.1 Tenant Identification

**Files:** `server/tenant.go` (new), `server/handler.go`

**Tasks:**
- [ ] Define `TenantIdentifier` with configurable sources
- [ ] Implement extractors: HTTP header, TDS property, connection string
- [ ] Add `WithTenant(ctx, tenant)` context helper
- [ ] Integrate into connection handler early in request lifecycle
- [ ] Configuration in `aul.yaml`

```yaml
tenancy:
  enabled: true
  identification:
    sources:
      - type: header
        name: "X-Tenant-ID"
      - type: tds_property
        name: "app_name"
        pattern: "tenant:(.+)"
    default: null
```

**Tests:**
- [ ] Extract tenant from HTTP header
- [ ] Extract tenant from TDS app_name with regex
- [ ] Fallback to default tenant
- [ ] Reject when no tenant and no default

### 2.2 Tenant Procedure Resolution

**Files:** `procedure/registry.go`

**Tasks:**
- [x] Support `_tenant/{tenant}/` override directories
- [x] Implement resolution order: tenant → database → global
- [x] Extract tenant from context in `Lookup()`

**Tests:**
- [x] Tenant override found, use it
- [x] No tenant override, fall back to database default
- [x] No database default, fall back to global
- [x] Nothing found, error

### 2.3 Tenant SQLite Databases

**Files:** `storage/sqlite.go`

**Tasks:**
- [x] Implement `resolveDatabasePath(database, tenant)`
- [x] Auto-create tenant database on first access (configurable)
- [x] Apply default PRAGMA settings to new databases
- [x] Connection pooling per tenant database

**Tests:**
- [x] Tenant database auto-created
- [x] Tenant database already exists, reuse
- [x] Auto-create disabled, missing database errors
- [x] Correct PRAGMA settings applied

### 2.4 Global Procedures with Tenant Context

**Files:** `procedure/registry.go`, `runtime/runtime.go`

**Tasks:**
- [x] Global procedures (`_global/`) resolve for any tenant
- [x] Execution context still carries tenant
- [x] Database access uses tenant's database

**Tests:**
- [x] Global utility called by tenant A accesses tenant A's data
- [x] Global utility called by tenant B accesses tenant B's data

### Phase 2 Deliverables

- [ ] All tests passing
- [ ] Example: multi-tenant setup with overrides
- [ ] Documentation updates
- [ ] Version bump to 0.6.0

---

## Phase 2.5: Annotations & Isolated Tables

**Goal:** Provide a general-purpose annotation system for aul-specific directives, and use it to enable per-table SQLite storage for isolated tables.

**Duration:** 1-2 weeks

**Prerequisite:** Phase 2 complete

### Overview

aul annotations are SQL comments with a special prefix that configure aul-specific behaviour without breaking SQL Server compatibility. Annotations are placed on contiguous lines immediately preceding the statement they apply to:

```sql
-- @aul:isolated
-- @aul:journal-mode=WAL
-- @aul:cache-size=5000
CREATE TABLE AuditLog (
    ID INT PRIMARY KEY,
    Action VARCHAR(50),
    Timestamp DATETIME
)

-- @aul:jit-threshold=50
-- @aul:timeout=5s
CREATE PROCEDURE usp_QuickLookup
    @Key VARCHAR(100)
AS
BEGIN
    SELECT Value FROM LookupCache WHERE Key = @Key
END
```

**Syntax:**
- `-- @aul:<key>` — Boolean flag (presence means true)
- `-- @aul:<key>=<value>` — Key-value setting
- Contiguous `-- @aul:` lines apply to the immediately following statement
- A blank line or non-aul comment breaks the association

### 2.5.1 Annotation Parser

**Files:** `pkg/annotations/annotations.go` (new)

**Tasks:**
- [x] Define `Annotation` struct with Key, Value, Line fields
- [x] Define `AnnotationSet` as `map[string]string` with helper methods
- [x] Implement `Extract(source string) []StatementAnnotations` to parse source text
- [x] Associate annotation blocks with statement positions (line numbers)
- [x] Handle edge cases: multiple statements, mixed comments, empty lines

```go
type StatementAnnotations struct {
    Annotations AnnotationSet
    StartLine   int  // First line of annotation block
    EndLine     int  // Last line before statement
    StmtLine    int  // Line where statement begins
}

type AnnotationSet map[string]string

func (a AnnotationSet) Has(key string) bool
func (a AnnotationSet) Get(key string) (string, bool)
func (a AnnotationSet) GetInt(key string, defaultVal int) int
func (a AnnotationSet) GetDuration(key string, defaultVal time.Duration) time.Duration
func (a AnnotationSet) GetBool(key string) bool  // true if key present
```

**Tests:**
- [x] Single annotation parsed correctly
- [x] Multiple annotations grouped correctly
- [x] Key-value parsing with various value types
- [x] Boolean flags (key without value)
- [x] Blank line breaks annotation block
- [x] Non-aul comments don't break block but aren't included
- [x] Multiple statements each get their own annotations

### 2.5.2 Annotation Storage

**Files:** `procedure/procedure.go`, `procedure/loader.go`

**Tasks:**
- [x] Add `Annotations AnnotationSet` field to `Procedure` struct
- [x] Extract annotations during procedure loading
- [x] Match annotations to statements by line number
- [x] Store annotations in registry alongside procedure metadata

**New annotation keys for procedures:**
| Key | Type | Description |
|-----|------|-------------|
| `jit-threshold` | int | Override default JIT threshold |
| `no-jit` | bool | Disable JIT for this procedure |
| `timeout` | duration | Execution timeout override |
| `log-params` | bool | Log parameter values (default true) |
| `deprecated` | bool | Log warning when called |

**Tests:**
- [x] Procedure loaded with annotations accessible
- [x] Annotations survive registry round-trip
- [x] Missing annotations return empty set (not nil)

### 2.5.3 DDL Annotation Extraction

**Files:** `storage/ddl.go` (new)

**Tasks:**
- [x] Hook into DDL execution path (CREATE TABLE, CREATE INDEX, etc.)
- [x] Extract annotations from DDL source before execution
- [x] Store table annotations in metadata catalogue
- [x] Define `TableMetadata` struct with schema info and annotations

```go
type TableMetadata struct {
    Database    string
    Schema      string
    Name        string
    Annotations AnnotationSet
    Columns     []ColumnMetadata
    CreatedAt   time.Time
}
```

**New annotation keys for tables:**
| Key | Type | Description |
|-----|------|-------------|
| `isolated` | bool | Store in separate SQLite file |
| `journal-mode` | string | SQLite journal mode (WAL, DELETE, etc.) |
| `cache-size` | int | SQLite cache size in pages |
| `synchronous` | string | SQLite synchronous setting |
| `read-only` | bool | Reject writes to this table |

**Tests:**
- [x] CREATE TABLE with annotations stores metadata
- [x] Annotations retrievable by table name
- [x] DROP TABLE removes metadata

### 2.5.4 Isolated Table Storage

**Files:** `storage/sqlite.go`, `storage/isolated.go` (new)

**Tasks:**
- [x] Implement `IsolatedTableManager` to manage per-table SQLite files
- [x] Directory structure: `{data_dir}/{database}/{schema}.{table}.db`
- [x] Route queries to isolated files based on table metadata
- [x] Apply per-table SQLite settings from annotations
- [x] Handle table creation: create isolated file with correct PRAGMAs
- [x] Handle table deletion: remove isolated file
- [x] Connection pooling per isolated table

```go
type IsolatedTableManager struct {
    baseDir     string
    connections map[string]*sql.DB  // keyed by qualified table name
    metadata    map[string]AnnotationSet
    mu          sync.RWMutex
}

func (m *IsolatedTableManager) GetConnection(database, schema, table string) (*sql.DB, error)
func (m *IsolatedTableManager) CreateTable(database, schema, table string, ddl string, annotations AnnotationSet) error
func (m *IsolatedTableManager) DropTable(database, schema, table string) error
func (m *IsolatedTableManager) IsIsolated(database, schema, table string) bool
```

**Tests:**
- [x] Isolated table created in separate file
- [x] Correct directory structure
- [x] PRAGMA settings applied from annotations
- [x] Queries route to correct file
- [x] Non-isolated tables unaffected
- [x] DROP TABLE removes isolated file
- [x] Concurrent access to different isolated tables

### 2.5.5 Query Routing

**Files:** `storage/router.go` (new)

**Tasks:**
- [ ] Implement query analysis to identify target tables
- [ ] Single-table queries to isolated tables route directly
- [ ] Multi-table queries involving isolated tables: error with clear message
- [ ] Queries to non-isolated tables route to main database

```go
type StorageRouter struct {
    mainDB      *sql.DB
    isolatedMgr *IsolatedTableManager
    metadata    *MetadataCatalogue
}

func (r *StorageRouter) Route(ctx context.Context, query string) (*sql.DB, error)
func (r *StorageRouter) Execute(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
func (r *StorageRouter) Query(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
```

**Tests:**
- [ ] Single-table SELECT routes correctly
- [ ] INSERT/UPDATE/DELETE route correctly
- [ ] JOIN across isolated and main tables returns error
- [ ] JOIN between two non-isolated tables works normally
- [ ] Subquery against isolated table detected

### Phase 2.5 Deliverables

- [ ] All tests passing
- [ ] Annotation system documented in new `docs/009-ANNOTATIONS.md`
- [ ] Example: isolated audit log table
- [ ] Example: procedure with custom JIT threshold
- [ ] Version bump to 0.6.5

---

## Phase 3: Delegation Optimisation

**Goal:** Automatically push simple procedures to PostgreSQL/MySQL for performance.

**Duration:** 3-4 weeks

**Prerequisite:** Phase 2 complete, PostgreSQL backend implemented

### 3.1 PostgreSQL Backend

**Files:** `storage/postgres.go` (new)

**Tasks:**
- [ ] Implement `StorageBackend` interface for PostgreSQL
- [ ] Connection pooling with pgx or database/sql
- [ ] `SupportsProcedures()` returns true
- [ ] `Dialect()` returns "postgres"
- [ ] Transaction support

**Tests:**
- [ ] Basic CRUD operations
- [ ] Transaction commit/rollback
- [ ] Connection pool behaviour

### 3.2 Delegation Manager

**Files:** `runtime/delegation.go` (new)

**Tasks:**
- [ ] `DelegationManager` struct with instance ID
- [ ] `shouldDelegate()` complexity analysis
- [ ] `Create()` with race condition prevention (sync.Map)
- [ ] `Drop()` for cleanup
- [ ] Error handling with exponential backoff

**Tests:**
- [ ] Simple procedure gets delegated
- [ ] Complex procedure does not get delegated
- [ ] Concurrent requests don't duplicate creation
- [ ] Failed creation backs off appropriately

### 3.3 Procedure Generation

**Files:** `runtime/delegation.go`, `tsqlruntime/generator.go` (new)

**Tasks:**
- [ ] Generate PL/pgSQL CREATE FUNCTION from T-SQL AST
- [ ] Use existing rewriter for SQL translation
- [ ] Naming: `_aul_proc_{instance}_{schema}_{name}_{hash}`
- [ ] Handle parameters (IN, OUT, INOUT)

**Tests:**
- [ ] Simple SELECT procedure generates valid PL/pgSQL
- [ ] Procedure with parameters
- [ ] Procedure with OUTPUT parameters

### 3.4 Startup and Shutdown

**Files:** `runtime/delegation.go`, `server/server.go`

**Tasks:**
- [ ] `StartupCleanup()` scans for orphaned `_aul_proc_{instance}_*`
- [ ] Compare against registry, drop stale
- [ ] `Cleanup()` on shutdown (configurable)
- [ ] Log all cleanup actions

**Tests:**
- [ ] Orphaned procedure cleaned up on startup
- [ ] Stale procedure (hash mismatch) cleaned up
- [ ] Current procedure not touched
- [ ] Shutdown cleanup works

### 3.5 Execution Path Integration

**Files:** `runtime/runtime.go`

**Tasks:**
- [ ] Check delegation before interpreted execution
- [ ] Verify hash matches before using delegated
- [ ] Async trigger delegation creation
- [ ] Async trigger resync on hash mismatch

**Tests:**
- [ ] Delegated procedure used when available
- [ ] Hash mismatch falls back to interpreted
- [ ] New procedure eventually gets delegated

### 3.6 Hot Reload with Delegation

**Files:** `runtime/delegation.go`, `procedure/registry.go`

**Tasks:**
- [ ] On source change, drop old delegated procedure
- [ ] In-flight executions complete normally
- [ ] Next execution triggers re-delegation

**Tests:**
- [ ] Source change drops delegation
- [ ] Re-delegation happens after source change

### Phase 3 Deliverables

- [ ] All tests passing
- [ ] PostgreSQL backend working
- [ ] Delegation working for simple procedures
- [ ] Documentation updates
- [ ] Version bump to 0.7.0

---

## Phase 4: JIT Compilation and Monitoring

**Goal:** Fix JIT architecture issues, integrate with tgpiler, add comprehensive monitoring.

**Duration:** 3-4 weeks

**Prerequisite:** Phase 3 complete

**Reference:** `JIT_ARCHITECTURE_FIXES.md` (reviewer feedback)

### 4.0 JIT Architecture Fixes (Blockers) — ✓ COMPLETED

**Files:** `jit/abi/abi.go`, `jit/state.go`, `jit/naming.go`, `jit/jit.go`

These fixes address critical issues identified in code review. **All implemented.**

**4.0.1 Create ABI Package** ✓

```go
// jit/abi/abi.go - shared types between host and plugins
type StorageBackend interface { ... }
type ExecResult struct { ... }
type CompiledFunc func(ctx, params, storage) (*ExecResult, error)
```

- [x] Create `jit/abi/abi.go` with shared types
- [x] Move type definitions from `jit/jit.go`
- [x] Ensure types are exported and documented

**4.0.2 Safe Go Identifiers** ✓

```go
// jit/naming.go
func SafeGoName(qualifiedName string) string
// "salesdb.dbo.GetCustomer" -> "Proc_salesdb_dbo_GetCustomer_a1b2c3d4"
```

- [x] Create `jit/naming.go` with identifier sanitisation
- [x] Add short hash for uniqueness
- [x] Handle all SQL name edge cases
- [x] Tests passing

**4.0.3 Compilation State Machine** ✓

```go
// jit/state.go
type CompileState int // None, Queued, Compiling, Ready, Failed
type CompileStatus struct { State, SourceHash, Error, RetryCount, ... }
```

- [x] Create `jit/state.go` with state types
- [x] Add `status map[string]*CompileStatus` to Manager
- [x] Implement `MaybeEnqueue()` with state checks (prevents duplicate compiles)
- [x] Implement exponential backoff for failed compiles

**4.0.4 Fix Runtime Gating** ✓

```go
// jit/jit.go
func (m *Manager) IsReady(name string, sourceHash string) bool
```

- [x] Add `IsReady(name, sourceHash)` to JIT manager
- [x] Runtime should use this instead of `proc.JITCompiled`
- [x] Verify source hash matches before using cached compilation

**4.0.5 Build Workspace with Module Context** ✓

```go
// jit_cache/<safe_name>_<hash>/
//   go.mod (with replace directive)
//   proc.go
//   proc.so
```

- [x] Create temp module per compile with `go.mod`
- [x] Add `replace github.com/ha1tch/aul => <path>` directive
- [x] Run `go build` in workspace directory
- [x] Persist compile errors for debugging

**4.0.6 Plugin Loading with ABI Types** ✓

- [x] Use `plugin.Open()` and `plugin.Lookup("Execute")`
- [x] Type assert to `*abi.CompiledFunc`
- [x] Generated code exports `var Execute abi.CompiledFunc = ...`

**4.0.7 Remove Redundant Semaphore** ✓

- [x] Workers limit concurrency directly
- [x] Removed separate `compileSem` channel

**Tests for 4.0:** ✓
- [x] `TestSafeGoName` — various SQL names produce valid Go identifiers
- [x] `TestSafeGoNameUniqueness` — different inputs produce different outputs
- [x] `TestSafePackageName` — lowercase package names
- [x] `TestWorkspaceDirName` — filesystem-safe directory names

### 4.1 tgpiler Integration

**Files:** `jit/generator.go` (new)

**Tasks:**
- [ ] Create `generator.go` with tgpiler integration
- [ ] Generate Go code targeting `jit/abi` types
- [ ] Handle all DML config options (dialect, store var, etc.)
- [ ] Generate proper import statements
- [ ] Export `var Execute abi.CompiledFunc = <fn>`

**Generated code structure:**
```go
package main

import (
    "context"
    "github.com/ha1tch/aul/jit/abi"
)

func execute(ctx context.Context, params map[string]interface{}, storage abi.StorageBackend) (*abi.ExecResult, error) {
    // tgpiler-generated procedure logic
}

var Execute abi.CompiledFunc = execute
```

**Tests:**
- [ ] Generated Go code compiles
- [ ] Generated code produces correct results
- [ ] Results match interpreted execution

### 4.2 Execution Priority

**Files:** `runtime/runtime.go`

**Tasks:**
- [ ] Implement priority: Delegated > JIT > Interpreted
- [ ] Simple procedures prefer delegation (Phase 3, PG/MySQL only)
- [ ] Complex procedures prefer JIT (all backends)
- [ ] Everything falls back to interpreted

```go
func (r *Runtime) Execute(...) {
    // 1. Try delegated (simple procs, PG/MySQL only)
    if r.delegation != nil && r.delegation.IsReady(proc) {
        return r.executeDelegated(...)
    }
    
    // 2. Try JIT (complex procs, all backends)
    if r.jitManager != nil && r.jitManager.IsReady(name, hash) {
        return r.executeJIT(...)
    }
    
    // 3. Interpreted (always available)
    return r.executeInterpreted(...)
}
```

**Tests:**
- [ ] Simple hot procedure on PostgreSQL: delegated
- [ ] Complex hot procedure: JIT
- [ ] Cold procedure: interpreted
- [ ] SQLite hot procedure: JIT (no delegation available)

### 4.3 Latency Monitoring

**Files:** `runtime/metrics.go` (new)

**Tasks:**
- [ ] `ExecutionMetrics` with latency histograms
- [ ] Per-procedure, per-path tracking
- [ ] Prometheus-compatible metrics endpoint
- [ ] P50, P95, P99 calculations

**Tests:**
- [ ] Latencies recorded correctly
- [ ] Metrics endpoint returns valid Prometheus format

### 4.4 Auto-Revocation

**Files:** `runtime/delegation.go`

**Tasks:**
- [ ] Compare delegation vs interpreted latency
- [ ] Revoke delegation if consistently slower (20%+ threshold)
- [ ] Configurable, disabled by default
- [ ] Log revocation decisions

**Tests:**
- [ ] Slow delegation gets revoked
- [ ] Normal delegation not revoked
- [ ] Disabled when configured off

### Phase 4 Deliverables

- [x] JIT architecture fixes complete (4.0)
- [x] Tests for naming utilities passing
- [ ] tgpiler integration (4.1)
- [ ] Execution priority implementation (4.2)
- [ ] Metrics endpoint with latency data (4.3)
- [ ] Auto-revocation (4.4)
- [ ] Documentation updates
- [ ] Version bump to 0.8.0

**Note:** Phase 4.0 (JIT architecture fixes) is complete. Remaining work is tgpiler integration and metrics.

---

## Phase 5: ACL Hooks and Production Hardening

**Goal:** Prepare for future ACL enforcement, production-ready error handling.

**Duration:** 2 weeks

**Prerequisite:** Phase 4 complete

### 5.1 Principal Capture

**Files:** `runtime/runtime.go`, `server/handler.go`

**Tasks:**
- [ ] Define `Principal` struct (type, ID, name, roles, tenant)
- [ ] Populate from connection context
- [ ] Carry through `ExecContext`
- [ ] No enforcement yet, just capture

**Tests:**
- [ ] Principal populated from TDS login
- [ ] Principal available in execution context

### 5.2 ACL Hook Points

**Files:** `runtime/runtime.go`

**Tasks:**
- [ ] `checkExecutePermission()` hook (returns nil for now)
- [ ] `filterResults()` hook (pass-through for now)
- [ ] Define `PermissionEvaluator` interface
- [ ] Configuration to enable/disable hooks

**Tests:**
- [ ] Hooks called but don't block
- [ ] Interface defined for future implementation

### 5.3 Audit Logging

**Files:** `runtime/audit.go` (new)

**Tasks:**
- [ ] Define `AuditEntry` struct
- [ ] `AuditLogger` interface
- [ ] Log all executions (procedure, parameters, duration, success)
- [ ] Configurable destinations (file, stdout, external)

**Tests:**
- [ ] Audit entries logged
- [ ] Sensitive parameters sanitised
- [ ] Log rotation (if file-based)

### 5.4 Error Handling Hardening

**Files:** Throughout

**Tasks:**
- [ ] Review all error paths
- [ ] Ensure T-SQL error format preserved (Msg, Level, State, Line)
- [ ] Map errors to appropriate protocol responses
- [ ] No panics escape to client

**Tests:**
- [ ] T-SQL errors have correct format
- [ ] TDS error tokens correct
- [ ] No panics under error conditions

### 5.5 Documentation and Examples

**Tasks:**
- [ ] Complete API documentation
- [ ] Multi-tenant setup guide
- [ ] Performance tuning guide
- [ ] Troubleshooting guide
- [ ] Example applications

### Phase 5 Deliverables

- [ ] All tests passing
- [ ] Audit logging working
- [ ] ACL hooks in place (not enforced)
- [ ] Production documentation
- [ ] Version bump to 1.0.0-rc1

---

## Summary Timeline

| Phase | Duration | Cumulative | Version |
|-------|----------|------------|---------|
| Phase 1: Core Infrastructure | 2-3 weeks | 2-3 weeks | 0.5.0 |
| Phase 2: Multi-Tenancy | 2-3 weeks | 4-6 weeks | 0.6.0 |
| Phase 2.5: Annotations & Isolated Tables | 1-2 weeks | 5-8 weeks | 0.6.5 |
| Phase 3: Delegation | 3-4 weeks | 8-12 weeks | 0.7.0 |
| Phase 4: JIT & Monitoring | 3-4 weeks | 11-16 weeks | 0.8.0 |
| Phase 5: ACL & Hardening | 2 weeks | 13-18 weeks | 1.0.0-rc1 |

**Total estimated time:** 13-18 weeks

---

## Dependencies

### External

| Dependency | Phase | Purpose |
|------------|-------|---------|
| PostgreSQL test instance | Phase 3 | Delegation testing |
| tgpiler 0.5.2+ | Phase 4 | JIT compilation |
| Prometheus (optional) | Phase 4 | Metrics collection |

### Internal

| Dependency | Blocks |
|------------|--------|
| Phase 1 complete | Phase 2 |
| Phase 2 complete | Phase 2.5 |
| Phase 2.5 complete | Phase 3 |
| PostgreSQL backend | Phase 3 delegation |
| Phase 3 complete | Phase 4 |
| Phase 4 complete | Phase 5 |

---

## Risk Mitigation

| Risk | Impact | Mitigation |
|------|--------|------------|
| PostgreSQL backend delays | Blocks Phase 3 | Can test delegation logic with mocks |
| JIT plugin compilation issues | Blocks Phase 4 | Fall back to interpreted, JIT is optimisation |
| Performance regression | User impact | Comprehensive benchmarks, auto-revocation |
| Multi-tenant data leakage | Security critical | Thorough isolation tests, code review |

---

## Success Criteria

### Phase 1
- 100+ procedure test suite passing
- Hot reload works without restart
- Nested EXEC to 3 levels

### Phase 2
- 3+ tenants isolated correctly
- Tenant override resolution works
- Auto-created tenant databases

### Phase 3
- Simple procedures delegated automatically
- Delegation 50%+ faster than interpreted for simple queries
- Startup cleanup removes orphans

### Phase 4
- Complex procedures JIT compiled
- Metrics available in Prometheus format
- Auto-revocation prevents performance regression

### Phase 5
- Audit log captures all executions
- No breaking changes from ACL hooks
- Documentation complete for 1.0 release
