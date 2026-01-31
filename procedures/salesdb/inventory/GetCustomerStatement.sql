-- GetCustomerStatement.sql
-- Generates a customer account statement
-- Location: salesdb/inventory/

CREATE PROCEDURE inventory.GetCustomerStatement
    @CustomerID INT
AS
BEGIN
    DECLARE @Name VARCHAR(100)
    DECLARE @Email VARCHAR(100)
    DECLARE @CreditLimit DECIMAL(10,2)
    DECLARE @Balance DECIMAL(10,2)
    DECLARE @TotalOrders INT
    DECLARE @TotalSpent DECIMAL(10,2)

    -- Get customer info
    SELECT @Name = Name,
           @Email = Email,
           @CreditLimit = CreditLimit,
           @Balance = Balance
    FROM Customers
    WHERE CustomerID = @CustomerID

    IF @Name IS NULL
    BEGIN
        RAISERROR('Customer not found: %d', 16, 1, @CustomerID)
        RETURN
    END

    -- Calculate totals
    SELECT @TotalOrders = COUNT(*),
           @TotalSpent = ISNULL(SUM(TotalAmount), 0)
    FROM Orders
    WHERE CustomerID = @CustomerID
      AND Status = 'Confirmed'

    -- Customer header
    SELECT 
        @CustomerID AS CustomerID,
        @Name AS CustomerName,
        @Email AS Email,
        @CreditLimit AS CreditLimit,
        @Balance AS CurrentBalance,
        (@CreditLimit - @Balance) AS AvailableCredit,
        @TotalOrders AS TotalOrders,
        @TotalSpent AS LifetimeSpend

    -- Order history
    SELECT 
        o.OrderID,
        o.OrderDate,
        o.Status,
        o.TotalAmount,
        (SELECT COUNT(*) FROM OrderItems WHERE OrderID = o.OrderID) AS ItemCount
    FROM Orders o
    WHERE o.CustomerID = @CustomerID
    ORDER BY o.OrderDate DESC

    -- Most purchased products
    SELECT 
        p.ProductID,
        p.Name,
        SUM(oi.Quantity) AS TotalQuantity,
        SUM(oi.LineTotal) AS TotalSpent
    FROM OrderItems oi
    JOIN Orders o ON oi.OrderID = o.OrderID
    JOIN Products p ON oi.ProductID = p.ProductID
    WHERE o.CustomerID = @CustomerID
      AND o.Status = 'Confirmed'
    GROUP BY p.ProductID, p.Name
    ORDER BY TotalQuantity DESC
END
