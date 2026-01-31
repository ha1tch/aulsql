-- GetCustomer.sql
-- Retrieve customer by ID
-- Location: salesdb/dbo/

CREATE PROCEDURE dbo.GetCustomer
    @CustomerID INT,
    @IncludeOrders BIT = 0
AS
BEGIN
    -- Return customer info
    SELECT 
        @CustomerID AS CustomerID,
        'Sample Customer ' + CAST(@CustomerID AS VARCHAR(10)) AS CustomerName,
        'customer' + CAST(@CustomerID AS VARCHAR(10)) + '@example.com' AS Email,
        GETDATE() AS CreatedDate
    
    -- Optionally return orders
    IF @IncludeOrders = 1
    BEGIN
        SELECT
            @CustomerID AS CustomerID,
            1001 AS OrderID,
            GETDATE() AS OrderDate,
            99.99 AS Total
    END
END
