# Multi-Database Procedure Example

This directory demonstrates the hierarchical procedure storage structure introduced in aul v0.6.0.

## Directory Structure

```
procedures/
├── _global/                    # Shared across all databases
│   └── dbo/
│       └── GetServerInfo.sql   # Available to any database context
├── master/                     # System database
│   └── dbo/
│       └── sp_who.sql          # master.dbo.sp_who
└── salesdb/                    # Application database
    ├── dbo/
    │   └── GetCustomer.sql     # salesdb.dbo.GetCustomer
    ├── reporting/
    │   └── MonthlySales.sql    # salesdb.reporting.MonthlySales
    └── inventory/              # Complex stored procedures with DDL/DML
        ├── InitializeDatabase.sql   # Creates tables and seed data
        ├── PlaceOrder.sql           # Order processing with validation
        ├── GetInventoryReport.sql   # Aggregations and CASE expressions
        ├── GetCustomerStatement.sql # JOINs and subqueries
        └── ProcessRefund.sql        # TRY/CATCH and transactions
```

## Procedure Resolution

When a procedure is called, aul resolves it in this order:

1. **Exact match** — `database.schema.name`
2. **Database context** — If connected to `salesdb`, calling `dbo.GetCustomer` resolves to `salesdb.dbo.GetCustomer`
3. **Global fallback** — If not found in current database, check `_global`

## Examples

### Calling from salesdb context

```sql
-- Resolves to salesdb.dbo.GetCustomer
EXEC dbo.GetCustomer @CustomerID = 123

-- Resolves to salesdb.reporting.MonthlySales
EXEC reporting.MonthlySales @Year = 2026, @Month = 1

-- Resolves to _global/dbo/GetServerInfo (global fallback)
EXEC dbo.GetServerInfo
```

### Nested procedure calls

The `MonthlySales` procedure demonstrates nested EXEC:

```sql
-- When @ShowServerInfo = 1, this procedure calls dbo.GetServerInfo
EXEC reporting.MonthlySales @Year = 2026, @Month = 1, @ShowServerInfo = 1
```

### Complex inventory procedures

The `inventory` schema demonstrates real-world stored procedures with DDL/DML:

```sql
-- Initialize database schema and seed data
EXEC inventory.InitializeDatabase

-- Place an order (validates stock, credit, updates multiple tables)
EXEC inventory.PlaceOrder @CustomerID = 1, @OrderID = 1001, 
    @ProductIDs = '1', @Quantities = '2'

-- Get inventory report with aggregations
EXEC inventory.GetInventoryReport @IncludeLowStock = 1, @MinValue = 100

-- Get customer statement with order history
EXEC inventory.GetCustomerStatement @CustomerID = 1
```

## Hot Reload

With the `-w` flag, aul watches this directory for changes:

```bash
aul -d ./examples/procedures -w --http-port 8080
```

- **New file** — Automatically registered
- **Modified file** — Reloaded if source hash changed
- **Deleted file** — Removed from registry

## Schema Validation

By default, aul validates that the schema declared in `CREATE PROCEDURE` matches the directory location:

```sql
-- In salesdb/reporting/MonthlySales.sql
CREATE PROCEDURE reporting.MonthlySales  -- ✓ Matches directory
```

If there's a mismatch, the procedure fails to load with a clear error message.
