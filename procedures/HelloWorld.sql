-- HelloWorld.sql
-- The simplest possible stored procedure for aul

CREATE PROCEDURE dbo.HelloWorld
AS
BEGIN
    SELECT 'Hello, World!' AS Message
END
