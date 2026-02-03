-- UpdateInventory: Adjust inventory levels (for receiving stock, corrections)
CREATE PROCEDURE UpdateInventory
    @ProductID INT,
    @QuantityChange INT,
    @Reason NVARCHAR(100) = 'Manual adjustment'
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @CurrentQty INT
    DECLARE @NewQty INT
    DECLARE @ProductName NVARCHAR(200)
    
    -- Get current inventory
    SELECT @CurrentQty = i.quantity, @ProductName = p.name
    FROM inventory i
    JOIN products p ON i.product_id = p.id
    WHERE i.product_id = @ProductID
    
    IF @CurrentQty IS NULL
    BEGIN
        -- Product exists but no inventory record - create one
        IF EXISTS (SELECT 1 FROM products WHERE id = @ProductID)
        BEGIN
            INSERT INTO inventory (product_id, quantity, reserved)
            VALUES (@ProductID, @QuantityChange, 0)
            
            SELECT @ProductName = name FROM products WHERE id = @ProductID
            SET @NewQty = @QuantityChange
        END
        ELSE
        BEGIN
            RAISERROR('Product not found', 16, 1)
            RETURN
        END
    END
    ELSE
    BEGIN
        SET @NewQty = @CurrentQty + @QuantityChange
        
        IF @NewQty < 0
        BEGIN
            RAISERROR('Adjustment would result in negative inventory', 16, 1)
            RETURN
        END
        
        UPDATE inventory
        SET quantity = @NewQty,
            updated_at = GETDATE()
        WHERE product_id = @ProductID
    END
    
    -- Return updated inventory status
    SELECT 
        p.id AS product_id,
        p.sku,
        p.name,
        @CurrentQty AS previous_quantity,
        @QuantityChange AS adjustment,
        i.quantity AS new_quantity,
        i.reserved,
        (i.quantity - i.reserved) AS available,
        @Reason AS reason,
        i.updated_at
    FROM products p
    JOIN inventory i ON p.id = i.product_id
    WHERE p.id = @ProductID
END
