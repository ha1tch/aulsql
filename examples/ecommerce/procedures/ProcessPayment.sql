-- ProcessPayment: Record payment and update order status
CREATE PROCEDURE ProcessPayment
    @OrderID INT,
    @PaymentMethod NVARCHAR(50),
    @TransactionID NVARCHAR(100),
    @Amount DECIMAL(10,2)
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @OrderTotal DECIMAL(10,2)
    DECLARE @OrderStatus NVARCHAR(20)
    DECLARE @PaidAmount DECIMAL(10,2)
    
    -- Get order details
    SELECT @OrderTotal = total, @OrderStatus = status
    FROM orders
    WHERE id = @OrderID
    
    IF @OrderTotal IS NULL
    BEGIN
        RAISERROR('Order not found', 16, 1)
        RETURN
    END
    
    IF @OrderStatus NOT IN ('pending', 'partially_paid')
    BEGIN
        RAISERROR('Order cannot accept payment in current status', 16, 1)
        RETURN
    END
    
    -- Get existing payments
    SELECT @PaidAmount = ISNULL(SUM(amount), 0)
    FROM payments
    WHERE order_id = @OrderID AND status = 'completed'
    
    -- Record payment
    INSERT INTO payments (order_id, payment_method, transaction_id, amount, status, processed_at)
    VALUES (@OrderID, @PaymentMethod, @TransactionID, @Amount, 'completed', GETDATE())
    
    -- Update total paid
    SET @PaidAmount = @PaidAmount + @Amount
    
    -- Update order status
    IF @PaidAmount >= @OrderTotal
    BEGIN
        UPDATE orders
        SET status = 'paid', updated_at = GETDATE()
        WHERE id = @OrderID
        
        -- Convert reserved inventory to sold (reduce quantity)
        UPDATE i
        SET i.quantity = i.quantity - oi.quantity,
            i.reserved = i.reserved - oi.quantity,
            i.updated_at = GETDATE()
        FROM inventory i
        JOIN order_items oi ON i.product_id = oi.product_id
        WHERE oi.order_id = @OrderID
    END
    ELSE
    BEGIN
        UPDATE orders
        SET status = 'partially_paid', updated_at = GETDATE()
        WHERE id = @OrderID
    END
    
    -- Return payment confirmation
    SELECT 
        o.id AS order_id,
        o.order_number,
        o.status,
        o.total AS order_total,
        @PaidAmount AS amount_paid,
        (o.total - @PaidAmount) AS balance_due
    FROM orders o
    WHERE o.id = @OrderID
END
