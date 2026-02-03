-- CancelOrder: Cancel order and release inventory
CREATE PROCEDURE CancelOrder
    @OrderID INT,
    @Reason NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @Status NVARCHAR(20)
    
    -- Get current status
    SELECT @Status = status FROM orders WHERE id = @OrderID
    
    IF @Status IS NULL
    BEGIN
        RAISERROR('Order not found', 16, 1)
        RETURN
    END
    
    IF @Status IN ('shipped', 'delivered', 'cancelled', 'refunded')
    BEGIN
        RAISERROR('Order cannot be cancelled in current status', 16, 1)
        RETURN
    END
    
    -- Release reserved inventory (only if not yet paid/shipped)
    IF @Status IN ('pending', 'partially_paid')
    BEGIN
        UPDATE i
        SET i.reserved = i.reserved - oi.quantity,
            i.updated_at = GETDATE()
        FROM inventory i
        JOIN order_items oi ON i.product_id = oi.product_id
        WHERE oi.order_id = @OrderID
    END
    
    -- If paid, restore inventory quantity
    IF @Status = 'paid'
    BEGIN
        UPDATE i
        SET i.quantity = i.quantity + oi.quantity,
            i.updated_at = GETDATE()
        FROM inventory i
        JOIN order_items oi ON i.product_id = oi.product_id
        WHERE oi.order_id = @OrderID
    END
    
    -- Update order status
    UPDATE orders
    SET status = 'cancelled',
        notes = ISNULL(notes + ' | ', '') + 'Cancelled: ' + ISNULL(@Reason, 'No reason provided'),
        updated_at = GETDATE()
    WHERE id = @OrderID
    
    -- Return confirmation
    SELECT 
        id AS order_id,
        order_number,
        status,
        total,
        notes,
        updated_at
    FROM orders
    WHERE id = @OrderID
END
