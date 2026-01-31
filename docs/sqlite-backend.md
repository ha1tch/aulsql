# SQLite Backend

aul uses SQLite as its default storage backend. This document describes the type mappings, limitations, and considerations when using the SQLite backend.

## Type Mappings

T-SQL types are mapped to SQLite types as follows:

| T-SQL Type | SQLite Type | Notes |
|------------|-------------|-------|
| `INT`, `INTEGER` | `INTEGER` | Native SQLite integer |
| `BIGINT` | `INTEGER` | SQLite INTEGER is 64-bit |
| `SMALLINT`, `TINYINT` | `INTEGER` | Stored as INTEGER |
| `BIT` | `INTEGER` | 0 or 1 |
| `FLOAT`, `REAL`, `DOUBLE` | `REAL` | IEEE 754 float64 |
| `DECIMAL(p,s)`, `NUMERIC(p,s)` | `TEXT` | Exact string representation |
| `MONEY`, `SMALLMONEY` | `TEXT` | Exact string representation |
| `VARCHAR(n)`, `NVARCHAR(n)` | `TEXT` | Length not enforced |
| `CHAR(n)`, `NCHAR(n)` | `TEXT` | Length not enforced |
| `TEXT`, `NTEXT` | `TEXT` | |
| `DATE`, `DATETIME`, `DATETIME2` | `TEXT` | ISO 8601 format |
| `TIME`, `DATETIMEOFFSET` | `TEXT` | ISO 8601 format |
| `VARBINARY`, `BINARY`, `IMAGE` | `BLOB` | |
| `UNIQUEIDENTIFIER` | `TEXT` | UUID string format |
| `XML` | `TEXT` | |

## Decimal and Money Handling

### Storage

`DECIMAL`, `NUMERIC`, `MONEY`, and `SMALLMONEY` values are stored as `TEXT` in SQLite to preserve exact decimal representation. This ensures that values like `19.99` are stored exactly as `19.99`, not as `19.989999999999998`.

### Limitations

**Arithmetic Operations**

When performing arithmetic in SQL (e.g., `price * quantity`), SQLite converts TEXT values to REAL (float64) for computation. This means:

```sql
-- Given: price = '19.99' (TEXT), quantity = 100 (INTEGER)
SELECT price * quantity FROM Products;
-- Result: 1999.0 (may show floating-point artifacts like 1998.9999999999998)
```

**Aggregate Functions**

Aggregate functions (`SUM`, `AVG`, etc.) also convert TEXT to REAL:

```sql
SELECT SUM(price) FROM Products;
-- Result is REAL, may have floating-point precision limits
```

**Comparisons**

Direct comparisons work correctly because SQLite performs numeric coercion:

```sql
SELECT * FROM Products WHERE price > 10.00;  -- Works correctly
SELECT * FROM Products WHERE price = 19.99;  -- Works correctly
```

**Sorting**

`ORDER BY` on decimal columns works correctly due to SQLite's type affinity rules:

```sql
SELECT * FROM Products ORDER BY price;  -- Sorts numerically
```

### Recommendations

1. **For exact arithmetic**: Perform calculations in application code using a decimal library (e.g., shopspring/decimal in Go)

2. **For financial applications**: Consider using scaled integers (e.g., store cents as INTEGER) if you need exact arithmetic within SQL

3. **For reporting/display**: The TEXT storage ensures values display exactly as entered

## String Length Constraints

SQLite does not enforce `VARCHAR(n)` length constraints. A `VARCHAR(50)` column can store strings of any length. Length validation, if required, should be performed at the application level.

## Date/Time Handling

Dates and times are stored as TEXT in ISO 8601 format. SQLite's date/time functions work with this format:

```sql
SELECT * FROM Orders WHERE OrderDate > '2025-01-01';
SELECT DATE(OrderDate) FROM Orders;
```

## Concurrency

SQLite uses file-level locking. For high-concurrency scenarios:

- Use WAL mode (enabled by default in aul)
- Consider connection pooling limits
- For heavy write workloads, consider PostgreSQL backend instead

## Memory vs. Persistent Storage

```bash
# In-memory (default) - data lost on restart
./aul --tds-port 1433

# Persistent storage
./aul --tds-port 1433 --storage-path ./data/aul.db
```

## Comparison with SQL Server

| Feature | SQL Server | aul (SQLite) |
|---------|-----------|--------------|
| DECIMAL precision | Exact up to 38 digits | Stored exact, arithmetic as float64 |
| VARCHAR length | Enforced | Not enforced |
| Transactions | Full ACID | Full ACID (single-writer) |
| Concurrency | Row-level locking | File-level locking |
| Stored procedures | Native | Interpreted T-SQL |
| Triggers | Native | Not supported |
| Views | Materializable | Not supported |

## Best Practices

1. **Use INTEGER for IDs and counts** - Native SQLite type, most efficient

2. **Use TEXT for decimals when precision matters** - Exact storage, accept float arithmetic

3. **Use REAL only for truly approximate values** - Scientific data, coordinates

4. **Test arithmetic queries** - Verify results meet precision requirements

5. **Consider application-level validation** - For string lengths, decimal ranges, etc.
