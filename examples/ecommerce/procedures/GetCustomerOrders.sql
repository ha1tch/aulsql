-- GetCustomerOrders: Order history for a customer
CREATE PROCEDURE GetCustomerOrders
    @CustomerID INT,
    @Status NVARCHAR(20) = NULL,
    @PageSize INT = 10,
    @PageNumber INT = 1
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @Offset INT
    SET @Offset = (@PageNumber - 1) * @PageSize
    
    -- Verify customer exists
    IF NOT EXISTS (SELECT 1 FROM customers WHERE id = @CustomerID)
    BEGIN
        RAISERROR('Customer not found', 16, 1)
        RETURN
    END
    
    -- Get total count
    SELECT COUNT(*) AS total_orders
    FROM orders
    WHERE customer_id = @CustomerID
        AND (@Status IS NULL OR status = @Status)
    
    -- Return orders
    SELECT 
        o.id AS order_id,
        o.order_number,
        o.status,
        o.subtotal,
        o.tax,
        o.shipping,
        o.total,
        o.created_at,
        (SELECT COUNT(*) FROM order_items WHERE order_id = o.id) AS item_count,
        (SELECT SUM(quantity) FROM order_items WHERE order_id = o.id) AS total_units
    FROM orders o
    WHERE o.customer_id = @CustomerID
        AND (@Status IS NULL OR o.status = @Status)
    ORDER BY o.created_at DESC
    OFFSET @Offset ROWS
    FETCH NEXT @PageSize ROWS ONLY
    
    -- Return customer summary
    SELECT 
        COUNT(*) AS lifetime_orders,
        SUM(total) AS lifetime_value,
        AVG(total) AS average_order_value,
        MAX(created_at) AS last_order_date
    FROM orders
    WHERE customer_id = @CustomerID
        AND status NOT IN ('cancelled', 'refunded')
END
