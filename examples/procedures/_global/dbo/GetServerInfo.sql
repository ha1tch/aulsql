-- GetServerInfo.sql
-- Global procedure available to all databases
-- Location: _global/dbo/

CREATE PROCEDURE dbo.GetServerInfo
AS
BEGIN
    SELECT 
        'aul' AS ServerName,
        '0.5.0' AS Version,
        'T-SQL Runtime' AS Engine,
        GETDATE() AS CurrentTime
END
