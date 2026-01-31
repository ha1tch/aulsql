-- InitializeDatabase.sql
-- Creates the schema for the inventory system
-- Location: salesdb/inventory/

CREATE PROCEDURE inventory.InitializeDatabase
AS
BEGIN
    -- Products table
    CREATE TABLE Products (
        ProductID INT PRIMARY KEY,
        Name VARCHAR(100) NOT NULL,
        SKU VARCHAR(50) NOT NULL,
        Price DECIMAL(10,2) NOT NULL,
        StockQty INT NOT NULL,
        ReorderLevel INT NOT NULL
    )

    -- Customers table
    CREATE TABLE Customers (
        CustomerID INT PRIMARY KEY,
        Name VARCHAR(100) NOT NULL,
        Email VARCHAR(100),
        CreditLimit DECIMAL(10,2),
        Balance DECIMAL(10,2)
    )

    -- Orders table
    CREATE TABLE Orders (
        OrderID INT PRIMARY KEY,
        CustomerID INT NOT NULL,
        OrderDate DATETIME,
        Status VARCHAR(20),
        TotalAmount DECIMAL(10,2)
    )

    -- Order items table
    CREATE TABLE OrderItems (
        ItemID INT PRIMARY KEY,
        OrderID INT NOT NULL,
        ProductID INT NOT NULL,
        Quantity INT NOT NULL,
        UnitPrice DECIMAL(10,2) NOT NULL,
        LineTotal DECIMAL(10,2) NOT NULL
    )

    -- Seed products
    INSERT INTO Products (ProductID, Name, SKU, Price, StockQty, ReorderLevel) VALUES (1, 'Widget A', 'WGT-001', 29.99, 100, 20)
    INSERT INTO Products (ProductID, Name, SKU, Price, StockQty, ReorderLevel) VALUES (2, 'Widget B', 'WGT-002', 49.99, 50, 15)
    INSERT INTO Products (ProductID, Name, SKU, Price, StockQty, ReorderLevel) VALUES (3, 'Gadget X', 'GDG-001', 99.99, 25, 10)
    INSERT INTO Products (ProductID, Name, SKU, Price, StockQty, ReorderLevel) VALUES (4, 'Gadget Y', 'GDG-002', 149.99, 30, 10)
    INSERT INTO Products (ProductID, Name, SKU, Price, StockQty, ReorderLevel) VALUES (5, 'Deluxe Kit', 'KIT-001', 299.99, 10, 5)

    -- Seed customers
    INSERT INTO Customers (CustomerID, Name, Email, CreditLimit, Balance) VALUES (1, 'Acme Corp', 'orders@acme.com', 5000.00, 0)
    INSERT INTO Customers (CustomerID, Name, Email, CreditLimit, Balance) VALUES (2, 'TechStart Inc', 'purchasing@techstart.io', 2500.00, 0)
    INSERT INTO Customers (CustomerID, Name, Email, CreditLimit, Balance) VALUES (3, 'Jane Smith', 'jane.smith@email.com', 1000.00, 0)

    SELECT 'Database initialized' AS Status, 5 AS ProductsCreated, 3 AS CustomersCreated
END
