# Annotations System

**Version:** 0.6.0  
**Status:** Implemented  
**Last updated:** January 2026

---

## Overview

aul supports SQL comment-based annotations for configuring procedure and table behaviour. Annotations use the `-- @aul:` prefix and are parsed at load time.

## Syntax

```sql
-- @aul:key              -- Boolean flag (presence = true)
-- @aul:key=value        -- Key-value setting
```

Annotations must be contiguous (no blank lines) and immediately precede the statement they configure.

## Procedure Annotations

| Key | Type | Description |
|-----|------|-------------|
| `jit-threshold` | int | Invocation count before JIT compilation triggers |
| `no-jit` | bool | Disable JIT compilation for this procedure |
| `timeout` | duration | Maximum execution time (e.g., `30s`, `5m`) |
| `log-params` | bool | Log parameter values on each invocation |
| `deprecated` | bool | Log deprecation warning when called |

### Example

```sql
-- @aul:jit-threshold=100
-- @aul:timeout=30s
-- @aul:log-params
CREATE PROCEDURE dbo.ProcessOrder
    @OrderID INT
AS
BEGIN
    -- procedure body
END
```

## Table Annotations

| Key | Type | Description |
|-----|------|-------------|
| `isolated` | bool | Store table in separate SQLite file |
| `journal-mode` | string | SQLite journal mode (WAL, DELETE, etc.) |
| `cache-size` | int | SQLite cache size in pages |
| `synchronous` | string | SQLite synchronous setting |
| `read-only` | bool | Reject writes to this table |

### Example

```sql
-- @aul:isolated
-- @aul:journal-mode=WAL
-- @aul:cache-size=2000
CREATE TABLE AuditLog (
    ID INT PRIMARY KEY,
    Action VARCHAR(100),
    Timestamp DATETIME
)
```

## Isolated Table Storage

Tables marked with `-- @aul:isolated` are stored in separate SQLite database files:

```
{data_dir}/
├── {database}/
│   ├── {schema}.{table}.db      -- isolated table
│   └── {schema}.{table2}.db     -- another isolated table
└── main.db                       -- non-isolated tables
```

### Benefits

- Independent PRAGMA settings per table
- Separate WAL files for high-write tables
- Can be backed up independently
- Isolation for audit/compliance data

### Limitations

- JOINs across isolated and non-isolated tables are not supported
- JOINs between different isolated tables are not supported
- Self-joins on the same isolated table work normally

## Implementation

### Files

| File | Purpose |
|------|---------|
| `pkg/annotations/annotations.go` | Parser and AnnotationSet type |
| `storage/ddl.go` | TableMetadata and MetadataCatalogue |
| `storage/isolated.go` | IsolatedTableManager |
| `storage/router.go` | StorageRouter for query routing |
| `procedure/procedure.go` | Procedure.Annotations field |

### API

```go
// Parse annotations from SQL source
parser := annotations.NewParser()
stmtAnnotations := parser.Extract(source)

// Get annotations for a specific line
annSet := parser.ExtractForLine(source, lineNumber)

// Check annotation values
if annSet.Has("isolated") {
    journalMode := annSet.GetString("journal-mode", "WAL")
    cacheSize := annSet.GetInt("cache-size", 2000)
}

// Validate annotations
errors := annotations.ValidateTableAnnotations(annSet)
```

### Query Routing

The `StorageRouter` automatically routes queries to the correct database:

```go
router := storage.NewStorageRouter(mainDB, isolatedMgr, catalogue)

// Automatically routes to isolated table's database
rows, err := router.Query(ctx, "SELECT * FROM AuditLog")

// Returns error for cross-database queries
_, err := router.Query(ctx, 
    "SELECT * FROM AuditLog a JOIN Users u ON a.UserID = u.ID")
// Error: query spans isolated and non-isolated tables
```

## Testing

Tests are in:
- `pkg/annotations/annotations_test.go` (19 tests)
- `storage/ddl_test.go` (9 tests)
- `storage/isolated_test.go` (7 tests)
- `storage/router_test.go` (10 tests)

Run with:
```bash
make test
```
