-- GetInventoryReport: Inventory status with reorder alerts
CREATE PROCEDURE GetInventoryReport
    @LowStockOnly BIT = 0,
    @CategoryID INT = NULL
AS
BEGIN
    SET NOCOUNT ON
    
    SELECT 
        p.id AS product_id,
        p.sku,
        p.name AS product_name,
        c.name AS category_name,
        i.quantity AS total_quantity,
        i.reserved,
        (i.quantity - i.reserved) AS available,
        i.reorder_level,
        i.reorder_quantity,
        CASE 
            WHEN (i.quantity - i.reserved) <= 0 THEN 'OUT_OF_STOCK'
            WHEN (i.quantity - i.reserved) <= i.reorder_level THEN 'LOW_STOCK'
            ELSE 'IN_STOCK'
        END AS stock_status,
        p.cost,
        (i.quantity * p.cost) AS inventory_value,
        i.updated_at AS last_updated
    FROM products p
    JOIN inventory i ON p.id = i.product_id
    LEFT JOIN product_categories pc ON p.id = pc.product_id
    LEFT JOIN categories c ON pc.category_id = c.id
    WHERE p.is_active = 1
        AND (@CategoryID IS NULL OR pc.category_id = @CategoryID)
        AND (@LowStockOnly = 0 OR (i.quantity - i.reserved) <= i.reorder_level)
    ORDER BY 
        CASE 
            WHEN (i.quantity - i.reserved) <= 0 THEN 1
            WHEN (i.quantity - i.reserved) <= i.reorder_level THEN 2
            ELSE 3
        END,
        p.name
    
    -- Summary statistics
    SELECT 
        COUNT(*) AS total_products,
        SUM(CASE WHEN (i.quantity - i.reserved) <= 0 THEN 1 ELSE 0 END) AS out_of_stock_count,
        SUM(CASE WHEN (i.quantity - i.reserved) > 0 AND (i.quantity - i.reserved) <= i.reorder_level THEN 1 ELSE 0 END) AS low_stock_count,
        SUM(i.quantity * p.cost) AS total_inventory_value
    FROM products p
    JOIN inventory i ON p.id = i.product_id
    WHERE p.is_active = 1
END
