-- GetInventoryReport.sql
-- Comprehensive inventory status report
-- Location: salesdb/inventory/

CREATE PROCEDURE inventory.GetInventoryReport
    @IncludeLowStock BIT = 1,
    @MinValue DECIMAL(10,2) = 0
AS
BEGIN
    -- Summary statistics
    SELECT 
        COUNT(*) AS TotalProducts,
        SUM(StockQty) AS TotalUnits,
        SUM(StockQty * Price) AS TotalInventoryValue,
        AVG(Price) AS AveragePrice,
        MIN(Price) AS MinPrice,
        MAX(Price) AS MaxPrice

    FROM Products
    WHERE (StockQty * Price) >= @MinValue

    -- Product details with value calculation
    SELECT 
        ProductID,
        Name,
        SKU,
        Price,
        StockQty,
        ReorderLevel,
        (StockQty * Price) AS StockValue,
        CASE 
            WHEN StockQty <= ReorderLevel THEN 'LOW'
            WHEN StockQty <= ReorderLevel * 2 THEN 'MEDIUM'
            ELSE 'OK'
        END AS StockStatus
    FROM Products
    WHERE (StockQty * Price) >= @MinValue
    ORDER BY StockValue DESC

    -- Low stock alerts (if requested)
    IF @IncludeLowStock = 1
    BEGIN
        SELECT 
            ProductID,
            Name,
            SKU,
            StockQty AS CurrentStock,
            ReorderLevel,
            (ReorderLevel - StockQty) AS UnitsNeeded,
            Price,
            (ReorderLevel - StockQty) * Price AS ReorderCost
        FROM Products
        WHERE StockQty <= ReorderLevel
        ORDER BY UnitsNeeded DESC
    END
END
