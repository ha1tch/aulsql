-- ProcessRefund.sql
-- Processes a refund with inventory restoration
-- Location: salesdb/inventory/

CREATE PROCEDURE inventory.ProcessRefund
    @OrderID INT,
    @Reason VARCHAR(200) = 'Customer request'
AS
BEGIN
    DECLARE @CustomerID INT
    DECLARE @TotalAmount DECIMAL(10,2)
    DECLARE @Status VARCHAR(20)
    DECLARE @ErrorMsg VARCHAR(200)

    -- Get order info
    SELECT @CustomerID = CustomerID, 
           @TotalAmount = TotalAmount,
           @Status = Status
    FROM Orders
    WHERE OrderID = @OrderID

    IF @CustomerID IS NULL
    BEGIN
        RAISERROR('Order not found: %d', 16, 1, @OrderID)
        RETURN
    END

    IF @Status = 'Refunded'
    BEGIN
        RAISERROR('Order %d has already been refunded', 16, 1, @OrderID)
        RETURN
    END

    IF @Status <> 'Confirmed'
    BEGIN
        SET @ErrorMsg = 'Cannot refund order with status: ' + @Status
        RAISERROR(@ErrorMsg, 16, 1)
        RETURN
    END

    BEGIN TRY
        -- Restore inventory for each item
        UPDATE Products
        SET StockQty = StockQty + oi.Quantity
        FROM Products p
        JOIN OrderItems oi ON p.ProductID = oi.ProductID
        WHERE oi.OrderID = @OrderID

        -- Credit customer account
        UPDATE Customers
        SET Balance = Balance - @TotalAmount
        WHERE CustomerID = @CustomerID

        -- Update order status
        UPDATE Orders
        SET Status = 'Refunded'
        WHERE OrderID = @OrderID

        -- Return refund confirmation
        SELECT 
            @OrderID AS OrderID,
            @CustomerID AS CustomerID,
            @TotalAmount AS RefundAmount,
            @Reason AS Reason,
            'Refunded' AS NewStatus,
            GETDATE() AS ProcessedAt

        -- Return restored items
        SELECT 
            p.ProductID,
            p.Name,
            oi.Quantity AS RestoredQty,
            p.StockQty AS NewStockLevel
        FROM OrderItems oi
        JOIN Products p ON oi.ProductID = p.ProductID
        WHERE oi.OrderID = @OrderID

    END TRY
    BEGIN CATCH
        RAISERROR('Refund processing failed', 16, 1)
    END CATCH
END
