-- Greet.sql
-- A simple stored procedure with an input parameter

CREATE PROCEDURE dbo.Greet
    @Name VARCHAR(100) = 'World'
AS
BEGIN
    SELECT 'Hello, ' + @Name + '!' AS Greeting
END
