-- CreateOrder: Convert cart to order with inventory reservation
CREATE PROCEDURE CreateOrder
    @CustomerID INT,
    @ShippingAddressID INT,
    @BillingAddressID INT = NULL,
    @Notes NVARCHAR(500) = NULL
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @OrderID INT
    DECLARE @OrderNumber NVARCHAR(20)
    DECLARE @Subtotal DECIMAL(10,2)
    DECLARE @Tax DECIMAL(10,2)
    DECLARE @Shipping DECIMAL(10,2)
    DECLARE @Total DECIMAL(10,2)
    DECLARE @ItemCount INT
    
    -- Check cart has items
    SELECT @ItemCount = COUNT(*) FROM cart_items WHERE customer_id = @CustomerID
    
    IF @ItemCount = 0
    BEGIN
        RAISERROR('Cart is empty', 16, 1)
        RETURN
    END
    
    -- Verify all items are available
    IF EXISTS (
        SELECT 1 
        FROM cart_items ci
        JOIN inventory i ON ci.product_id = i.product_id
        WHERE ci.customer_id = @CustomerID
        AND ci.quantity > (i.quantity - i.reserved)
    )
    BEGIN
        RAISERROR('One or more items are no longer available in requested quantity', 16, 1)
        RETURN
    END
    
    -- Verify shipping address belongs to customer
    IF NOT EXISTS (
        SELECT 1 FROM addresses 
        WHERE id = @ShippingAddressID AND customer_id = @CustomerID
    )
    BEGIN
        RAISERROR('Invalid shipping address', 16, 1)
        RETURN
    END
    
    -- Use shipping as billing if not specified
    IF @BillingAddressID IS NULL
        SET @BillingAddressID = @ShippingAddressID
    
    -- Calculate subtotal
    SELECT @Subtotal = SUM(ci.quantity * p.price)
    FROM cart_items ci
    JOIN products p ON ci.product_id = p.id
    WHERE ci.customer_id = @CustomerID
    
    -- Calculate tax (8% example rate)
    SET @Tax = ROUND(@Subtotal * 0.08, 2)
    
    -- Calculate shipping (flat rate example)
    SET @Shipping = CASE WHEN @Subtotal >= 100 THEN 0 ELSE 9.99 END
    
    -- Calculate total
    SET @Total = @Subtotal + @Tax + @Shipping
    
    -- Generate order number (timestamp-based)
    SET @OrderNumber = 'ORD-' + CONVERT(NVARCHAR, GETDATE(), 112) + '-' + 
                       RIGHT('0000' + CAST(ABS(CHECKSUM(NEWID())) % 10000 AS NVARCHAR), 4)
    
    -- Create order
    INSERT INTO orders (order_number, customer_id, status, subtotal, tax, shipping, total,
                       shipping_address_id, billing_address_id, notes)
    VALUES (@OrderNumber, @CustomerID, 'pending', @Subtotal, @Tax, @Shipping, @Total,
            @ShippingAddressID, @BillingAddressID, @Notes)
    
    SET @OrderID = SCOPE_IDENTITY()
    
    -- Create order items from cart
    INSERT INTO order_items (order_id, product_id, quantity, unit_price, total)
    SELECT 
        @OrderID,
        ci.product_id,
        ci.quantity,
        p.price,
        ci.quantity * p.price
    FROM cart_items ci
    JOIN products p ON ci.product_id = p.id
    WHERE ci.customer_id = @CustomerID
    
    -- Reserve inventory
    UPDATE i
    SET i.reserved = i.reserved + ci.quantity,
        i.updated_at = GETDATE()
    FROM inventory i
    JOIN cart_items ci ON i.product_id = ci.product_id
    WHERE ci.customer_id = @CustomerID
    
    -- Clear cart
    DELETE FROM cart_items WHERE customer_id = @CustomerID
    
    -- Return order confirmation
    SELECT 
        o.id AS order_id,
        o.order_number,
        o.status,
        o.subtotal,
        o.tax,
        o.shipping,
        o.total,
        o.created_at
    FROM orders o
    WHERE o.id = @OrderID
    
    -- Return order items
    SELECT 
        oi.product_id,
        p.sku,
        p.name AS product_name,
        oi.quantity,
        oi.unit_price,
        oi.total AS line_total
    FROM order_items oi
    JOIN products p ON oi.product_id = p.id
    WHERE oi.order_id = @OrderID
END
