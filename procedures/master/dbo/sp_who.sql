-- sp_who.sql
-- System procedure in master database
-- Location: master/dbo/

CREATE PROCEDURE dbo.sp_who
AS
BEGIN
    -- Simulated session information
    SELECT 
        1 AS spid,
        'sa' AS loginame,
        'master' AS db_name,
        'AWAITING COMMAND' AS status,
        NULL AS cmd,
        0 AS cpu,
        0 AS physical_io
END
