-- AddToCart: Add a product to customer's shopping cart
-- Updates quantity if product already in cart
CREATE PROCEDURE AddToCart
    @CustomerID INT,
    @ProductID INT,
    @Quantity INT = 1
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @ExistingQty INT
    DECLARE @Available INT
    DECLARE @Reserved INT
    
    -- Check if product exists and is active
    IF NOT EXISTS (SELECT 1 FROM products WHERE id = @ProductID AND is_active = 1)
    BEGIN
        RAISERROR('Product not found or inactive', 16, 1)
        RETURN
    END
    
    -- Check inventory
    SELECT @Available = quantity, @Reserved = reserved
    FROM inventory
    WHERE product_id = @ProductID
    
    IF @Available IS NULL
    BEGIN
        RAISERROR('Product not in inventory', 16, 1)
        RETURN
    END
    
    -- Check if already in cart
    SELECT @ExistingQty = quantity
    FROM cart_items
    WHERE customer_id = @CustomerID AND product_id = @ProductID
    
    IF @ExistingQty IS NOT NULL
    BEGIN
        -- Update existing cart item
        DECLARE @NewQty INT
        SET @NewQty = @ExistingQty + @Quantity
        
        -- Verify availability
        IF @NewQty > (@Available - @Reserved)
        BEGIN
            RAISERROR('Insufficient inventory', 16, 1)
            RETURN
        END
        
        UPDATE cart_items
        SET quantity = @NewQty
        WHERE customer_id = @CustomerID AND product_id = @ProductID
    END
    ELSE
    BEGIN
        -- Verify availability for new item
        IF @Quantity > (@Available - @Reserved)
        BEGIN
            RAISERROR('Insufficient inventory', 16, 1)
            RETURN
        END
        
        -- Insert new cart item
        INSERT INTO cart_items (customer_id, product_id, quantity)
        VALUES (@CustomerID, @ProductID, @Quantity)
    END
    
    -- Return updated cart
    SELECT 
        ci.product_id,
        p.name AS product_name,
        p.sku,
        ci.quantity,
        p.price AS unit_price,
        (ci.quantity * p.price) AS line_total
    FROM cart_items ci
    JOIN products p ON ci.product_id = p.id
    WHERE ci.customer_id = @CustomerID
END
