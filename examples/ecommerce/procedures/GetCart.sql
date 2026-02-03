-- GetCart: Retrieve customer's shopping cart with totals
CREATE PROCEDURE GetCart
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON
    
    -- Return cart items with product details
    SELECT 
        ci.id AS cart_item_id,
        ci.product_id,
        p.sku,
        p.name AS product_name,
        ci.quantity,
        p.price AS unit_price,
        (ci.quantity * p.price) AS line_total,
        i.quantity AS in_stock,
        CASE WHEN i.quantity - i.reserved >= ci.quantity THEN 1 ELSE 0 END AS is_available
    FROM cart_items ci
    JOIN products p ON ci.product_id = p.id
    LEFT JOIN inventory i ON p.id = i.product_id
    WHERE ci.customer_id = @CustomerID
    ORDER BY ci.added_at DESC
    
    -- Return cart summary
    SELECT 
        COUNT(*) AS item_count,
        SUM(ci.quantity) AS total_units,
        SUM(ci.quantity * p.price) AS subtotal
    FROM cart_items ci
    JOIN products p ON ci.product_id = p.id
    WHERE ci.customer_id = @CustomerID
END
