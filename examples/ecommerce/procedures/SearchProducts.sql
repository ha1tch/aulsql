-- SearchProducts: Search and filter product catalog
CREATE PROCEDURE SearchProducts
    @SearchTerm NVARCHAR(100) = NULL,
    @CategoryID INT = NULL,
    @MinPrice DECIMAL(10,2) = NULL,
    @MaxPrice DECIMAL(10,2) = NULL,
    @InStockOnly BIT = 0,
    @SortBy NVARCHAR(20) = 'name',
    @PageSize INT = 20,
    @PageNumber INT = 1
AS
BEGIN
    SET NOCOUNT ON
    
    DECLARE @Offset INT
    SET @Offset = (@PageNumber - 1) * @PageSize
    
    -- Get total count for pagination
    SELECT COUNT(*) AS total_count
    FROM products p
    LEFT JOIN inventory i ON p.id = i.product_id
    LEFT JOIN product_categories pc ON p.id = pc.product_id
    WHERE p.is_active = 1
        AND (@SearchTerm IS NULL OR p.name LIKE '%' + @SearchTerm + '%' OR p.description LIKE '%' + @SearchTerm + '%')
        AND (@CategoryID IS NULL OR pc.category_id = @CategoryID)
        AND (@MinPrice IS NULL OR p.price >= @MinPrice)
        AND (@MaxPrice IS NULL OR p.price <= @MaxPrice)
        AND (@InStockOnly = 0 OR (i.quantity - i.reserved) > 0)
    
    -- Return products
    SELECT 
        p.id AS product_id,
        p.sku,
        p.name,
        p.slug,
        p.description,
        p.price,
        p.is_featured,
        ISNULL(i.quantity - i.reserved, 0) AS available_quantity,
        CASE WHEN ISNULL(i.quantity - i.reserved, 0) > 0 THEN 1 ELSE 0 END AS in_stock,
        (SELECT AVG(CAST(rating AS DECIMAL(3,2))) FROM reviews WHERE product_id = p.id AND is_approved = 1) AS avg_rating,
        (SELECT COUNT(*) FROM reviews WHERE product_id = p.id AND is_approved = 1) AS review_count
    FROM products p
    LEFT JOIN inventory i ON p.id = i.product_id
    LEFT JOIN product_categories pc ON p.id = pc.product_id
    WHERE p.is_active = 1
        AND (@SearchTerm IS NULL OR p.name LIKE '%' + @SearchTerm + '%' OR p.description LIKE '%' + @SearchTerm + '%')
        AND (@CategoryID IS NULL OR pc.category_id = @CategoryID)
        AND (@MinPrice IS NULL OR p.price >= @MinPrice)
        AND (@MaxPrice IS NULL OR p.price <= @MaxPrice)
        AND (@InStockOnly = 0 OR (i.quantity - i.reserved) > 0)
    GROUP BY p.id, p.sku, p.name, p.slug, p.description, p.price, p.is_featured, i.quantity, i.reserved
    ORDER BY 
        CASE WHEN @SortBy = 'name' THEN p.name END ASC,
        CASE WHEN @SortBy = 'price_asc' THEN p.price END ASC,
        CASE WHEN @SortBy = 'price_desc' THEN p.price END DESC,
        CASE WHEN @SortBy = 'newest' THEN p.created_at END DESC
    OFFSET @Offset ROWS
    FETCH NEXT @PageSize ROWS ONLY
END
