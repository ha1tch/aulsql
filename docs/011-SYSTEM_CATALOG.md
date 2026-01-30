# System Catalog Views

**Version:** 0.6.1  
**Status:** Implemented

## Overview

aul provides SQL Server-compatible system catalog views that allow client applications to query database metadata using familiar T-SQL patterns. This enables tools like SQL Server Management Studio, Azure Data Studio, and custom applications to introspect the aul server.

## Supported Views

### sys.tables

Returns information about user tables.

| Column | Type | Description |
|--------|------|-------------|
| name | NVARCHAR | Table name |
| object_id | INT | Synthetic object identifier |
| schema_id | INT | Schema identifier (1 = dbo) |
| type | CHAR(2) | Object type ('U ' = user table) |
| type_desc | NVARCHAR | Type description |
| create_date | DATETIME | Creation date |
| modify_date | DATETIME | Modification date |
| is_ms_shipped | BIT | Always 0 (user tables only) |

**Example:**
```sql
SELECT name, schema_id FROM sys.tables WHERE is_ms_shipped = 0
```

### sys.procedures

Returns information about stored procedures loaded in the registry.

| Column | Type | Description |
|--------|------|-------------|
| name | NVARCHAR | Procedure name |
| object_id | INT | Synthetic object identifier |
| schema_id | INT | Schema identifier |
| type | CHAR(2) | Object type ('P ' = procedure) |
| type_desc | NVARCHAR | 'SQL_STORED_PROCEDURE' |
| create_date | DATETIME | When procedure was loaded |
| modify_date | DATETIME | Same as create_date |
| is_ms_shipped | BIT | Always 0 |

**Example:**
```sql
SELECT name FROM sys.procedures WHERE is_ms_shipped = 0
```

### sys.schemas

Returns schema information.

| Column | Type | Description |
|--------|------|-------------|
| name | NVARCHAR | Schema name |
| schema_id | INT | Schema identifier |
| principal_id | INT | Owner principal (always 1) |

Default schemas:
- 1: dbo
- 2: guest  
- 3: INFORMATION_SCHEMA
- 4: sys

**Example:**
```sql
SELECT name, schema_id FROM sys.schemas
```

### sys.objects

Returns combined tables and procedures.

Same columns as sys.tables. Includes all user tables (type 'U ') and stored procedures (type 'P ').

**Example:**
```sql
SELECT name, type_desc FROM sys.objects WHERE type IN ('U', 'P')
```

### sys.columns

Returns column information for tables.

| Column | Type | Description |
|--------|------|-------------|
| object_id | INT | Parent table's object_id |
| name | NVARCHAR | Column name |
| column_id | INT | Column ordinal (1-based) |
| system_type_id | INT | Data type identifier |
| max_length | SMALLINT | Maximum length |
| is_nullable | BIT | Nullability |

**Example:**
```sql
SELECT name, column_id FROM sys.columns WHERE object_id = 1
```

### sys.types

Returns SQL Server data type information.

| Column | Type | Description |
|--------|------|-------------|
| name | NVARCHAR | Type name |
| system_type_id | INT | Type identifier |
| user_type_id | INT | Same as system_type_id |
| max_length | SMALLINT | Maximum length |
| is_nullable | BIT | Always 1 |

Includes standard SQL Server types: int, bigint, varchar, nvarchar, datetime, bit, etc.

**Example:**
```sql
SELECT name, system_type_id FROM sys.types WHERE name LIKE '%int%'
```

### sys.databases

Returns database information.

| Column | Type | Description |
|--------|------|-------------|
| name | NVARCHAR | Database name |
| database_id | INT | Database identifier |
| create_date | DATETIME | Creation date |
| compatibility_level | TINYINT | 160 (SQL Server 2022) |
| state | TINYINT | 0 (ONLINE) |
| state_desc | NVARCHAR | 'ONLINE' |

Returns standard system databases: master, tempdb, model, msdb.

## Implementation Notes

### Query Interception

System catalog queries are intercepted at the storage layer. When a query contains references to `sys.*` views, it is routed to the SystemCatalog handler instead of the underlying SQLite database.

### Limitations

1. **No JOINs between sys views**: Complex queries joining sys.tables with sys.schemas are not fully supported. Use simple single-table queries.

2. **Synthetic object_id**: Object identifiers are generated at query time and may change between server restarts.

3. **Limited filtering**: WHERE clauses are evaluated after data generation, not optimised.

4. **No sys.indexes, sys.foreign_keys**: Only the views listed above are implemented.

### Compatibility

The system catalog is designed to support common introspection patterns used by:

- SQL Server Management Studio (SSMS)
- Azure Data Studio
- go-mssqldb driver
- Entity Framework
- Dapper

## Usage with go-mssqldb

```go
// List tables
rows, err := db.Query("SELECT name FROM sys.tables WHERE is_ms_shipped = 0")

// List procedures
rows, err := db.Query("SELECT name FROM sys.procedures WHERE is_ms_shipped = 0")
```

## Future Enhancements

Planned additions:
- sys.indexes
- sys.foreign_keys
- sys.parameters (procedure parameters)
- INFORMATION_SCHEMA views
- sys.dm_* dynamic management views
