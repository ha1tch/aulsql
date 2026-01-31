-- MonthlySales.sql
-- Monthly sales report with nested procedure call
-- Location: salesdb/reporting/

CREATE PROCEDURE reporting.MonthlySales
    @Year INT,
    @Month INT,
    @ShowServerInfo BIT = 0
AS
BEGIN
    -- Optionally call global procedure to show server info
    IF @ShowServerInfo = 1
    BEGIN
        EXEC dbo.GetServerInfo
    END
    
    -- Return monthly sales summary
    SELECT 
        @Year AS ReportYear,
        @Month AS ReportMonth,
        'Q' + CAST((@Month - 1) / 3 + 1 AS VARCHAR(1)) AS Quarter,
        1000.00 + (@Month * 100) AS TotalSales,
        50 + @Month AS OrderCount,
        (1000.00 + (@Month * 100)) / (50 + @Month) AS AvgOrderValue
    
    -- Return top products for the month
    SELECT
        @Year AS Year,
        @Month AS Month,
        'Product A' AS ProductName,
        500.00 AS Revenue
    UNION ALL
    SELECT
        @Year,
        @Month,
        'Product B',
        300.00
    UNION ALL
    SELECT
        @Year,
        @Month,
        'Product C',
        200.00
END
