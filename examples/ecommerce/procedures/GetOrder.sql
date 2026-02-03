-- GetOrder: Retrieve complete order details
CREATE PROCEDURE GetOrder
    @OrderID INT = NULL,
    @OrderNumber NVARCHAR(20) = NULL
AS
BEGIN
    SET NOCOUNT ON
    
    -- Allow lookup by ID or order number
    IF @OrderID IS NULL AND @OrderNumber IS NULL
    BEGIN
        RAISERROR('Must provide OrderID or OrderNumber', 16, 1)
        RETURN
    END
    
    IF @OrderID IS NULL
    BEGIN
        SELECT @OrderID = id FROM orders WHERE order_number = @OrderNumber
        
        IF @OrderID IS NULL
        BEGIN
            RAISERROR('Order not found', 16, 1)
            RETURN
        END
    END
    
    -- Return order header
    SELECT 
        o.id AS order_id,
        o.order_number,
        o.status,
        o.subtotal,
        o.tax,
        o.shipping,
        o.total,
        o.notes,
        o.created_at,
        o.updated_at,
        c.id AS customer_id,
        c.email AS customer_email,
        c.first_name,
        c.last_name
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    WHERE o.id = @OrderID
    
    -- Return order items
    SELECT 
        oi.id AS item_id,
        oi.product_id,
        p.sku,
        p.name AS product_name,
        oi.quantity,
        oi.unit_price,
        oi.total AS line_total
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
    WHERE oi.order_id = @OrderID
    
    -- Return shipping address
    SELECT 
        'shipping' AS address_type,
        a.street_1,
        a.street_2,
        a.city,
        a.state,
        a.postal_code,
        a.country
    FROM orders o
    JOIN addresses a ON o.shipping_address_id = a.id
    WHERE o.id = @OrderID
    
    -- Return payments
    SELECT 
        p.id AS payment_id,
        p.payment_method,
        p.transaction_id,
        p.amount,
        p.status,
        p.processed_at
    FROM payments p
    WHERE p.order_id = @OrderID
    ORDER BY p.created_at
END
