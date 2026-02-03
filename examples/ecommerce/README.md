# E-Commerce Example

A complete e-commerce schema and stored procedures demonstrating aul's T-SQL capabilities.

## Quick Start

```bash
cd examples/ecommerce

# First-time setup (creates database, schema, sample data)
make setup

# Start the server
make run
```

Then connect with DBeaver, SSMS, or any SQL Server client:
- **Host:** localhost
- **Port:** 1433
- **User:** (anything)
- **Password:** (anything)

## Makefile Commands

| Command | Description |
|---------|-------------|
| `make setup` | Build, create database, load schema and sample data |
| `make run` | Start the aul server |
| `make shell` | Interactive SQL shell (iaul) |
| `make reset` | Wipe and reinitialize everything |
| `make clean` | Remove database file |

## Schema

The schema includes:

- **customers** - Customer accounts
- **addresses** - Shipping/billing addresses
- **categories** - Product categories (hierarchical)
- **products** - Product catalog
- **product_categories** - Product-category relationships
- **inventory** - Stock tracking with reservation system
- **orders** - Order headers
- **order_items** - Order line items
- **payments** - Payment transactions
- **cart_items** - Shopping cart
- **reviews** - Product reviews

## Stored Procedures

| Procedure | Description |
|-----------|-------------|
| `AddToCart` | Add product to cart with inventory check |
| `GetCart` | Retrieve cart contents with totals |
| `CreateOrder` | Convert cart to order, reserve inventory |
| `ProcessPayment` | Record payment, update order status |
| `GetOrder` | Retrieve complete order details |
| `CancelOrder` | Cancel order, release inventory |
| `GetCustomerOrders` | Customer order history with pagination |
| `SearchProducts` | Search/filter catalog with pagination |
| `GetInventoryReport` | Stock levels with reorder alerts |
| `UpdateInventory` | Adjust stock levels |

## Usage

### Quick Test

After `make run`, open another terminal:

```bash
# Interactive shell
make shell

# Or connect with any SQL Server client
```

### Example Workflow

```sql
-- Add items to cart
EXEC AddToCart @CustomerID = 1, @ProductID = 1, @Quantity = 2
EXEC AddToCart @CustomerID = 1, @ProductID = 3, @Quantity = 1

-- View cart
EXEC GetCart @CustomerID = 1

-- Create order
EXEC CreateOrder @CustomerID = 1, @ShippingAddressID = 1

-- Process payment
EXEC ProcessPayment @OrderID = 1, @PaymentMethod = 'credit_card',
     @TransactionID = 'txn_abc123', @Amount = 334.96

-- View order
EXEC GetOrder @OrderNumber = 'ORD-20260131-1234'
```

### Inventory Management

```sql
-- Check low stock items
EXEC GetInventoryReport @LowStockOnly = 1

-- Receive new stock
EXEC UpdateInventory @ProductID = 1, @QuantityChange = 100,
     @Reason = 'PO-2026-001 received'
```

### Product Search

```sql
-- Search with filters
EXEC SearchProducts @SearchTerm = 'wireless',
     @MinPrice = 50, @MaxPrice = 200, @InStockOnly = 1

-- Browse category
EXEC SearchProducts @CategoryID = 1, @SortBy = 'price_asc'
```

## Features Demonstrated

- **Inventory reservation** - Stock reserved when order created, deducted when paid
- **Transaction safety** - Order creation validates availability before committing
- **Multi-result sets** - Procedures return multiple related result sets
- **Pagination** - OFFSET/FETCH for large result sets
- **Conditional logic** - Complex business rules in T-SQL
- **Error handling** - RAISERROR for validation failures

## Version Control

All procedures are plain `.sql` files - version them with git alongside your application code.
