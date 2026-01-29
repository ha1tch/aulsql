CREATE PROCEDURE dbo.hello
    @Name VARCHAR(100)
AS
BEGIN
    SELECT 'Hello, ' + @Name AS Greeting;
END
