DECLARE @count int = 1;
DECLARE @maxdb int = 40;
DECLARE @dbname sysname;
DECLARE @sql nvarchar(MAX)

DROP TABLE IF EXISTS #cmds

CREATE TABLE #cmds (command nvarchar(MAX) NOT NULL)

WHILE @count <= @maxdb BEGIN


	SET @dbname = (SELECT TOP (1) name FROM sys.databases WHERE name LIKE 'AdventureWorksLT_%' ORDER BY NEWID())

	-- Change the length of some random columns
	SET @sql = 'USE [?]
	SELECT TOP(3) ''ALTER TABLE '' + QUOTENAME(DB_NAME()) + ''.'' 
						+ QUOTENAME(OBJECT_SCHEMA_NAME(c.object_id)) + ''.'' + QUOTENAME(OBJECT_NAME(c.object_id)) + 
					'' ALTER COLUMN '' + QUOTENAME(c.name) + '' '' + t.name + ''('' + CONVERT(VARCHAR(10), CEILING(c.max_length*1.2)) + '')''  +
					CASE WHEN c.is_nullable = 0 THEN '' NOT'' ELSE '''' END + '' NULL''
		FROM sys.tables AS tb
			INNER JOIN sys.columns AS c
				ON c.object_id = tb.object_id
			INNER JOIN sys.types AS t
				ON t.user_type_id = c.user_type_id
		WHERE OBJECTPROPERTY(c.object_id, ''IsMsShipped'') = 0
			AND t.name IN  (''varchar'', ''nvarchar'')
			AND c.max_length <> -1
		ORDER BY NEWID()'

	SET @sql = REPLACE(@sql, '?', @dbname);

	INSERT INTO #cmds
	EXECUTE sys.sp_executesql @stmt = @sql;

	-- Add comments to some random sql_modules
	SET @sql = 'USE [?]
	SELECT TOP(2) REPLACE(REPLACE([definition], '''''''', ''''''''''''), ''CREATE '', ''USE [?]'' + CHAR(10) + ''EXEC (''''--SOME COMMENTS'' + CHAR(10) + ''CREATE OR ALTER '') + '''''')''
		FROM sys.sql_modules
		ORDER BY NEWID()'
	
	SET @sql = REPLACE(@sql, '?', @dbname);

	INSERT INTO #cmds
	EXECUTE sys.sp_executesql @stmt = @sql;

	SET @count += 1;
	
END

DECLARE c CURSOR LOCAL STATIC FORWARD_ONLY
FOR SELECT command FROM #cmds

OPEN c
FETCH NEXT FROM c INTO @sql

WHILE @@FETCH_STATUS = 0 BEGIN

	PRINT @sql;
	BEGIN TRY
		EXECUTE sys.sp_executesql @stmt = @sql;
	END TRY
	BEGIN CATCH
	END CATCH
	FETCH NEXT FROM c INTO @sql
END
CLOSE c
DEALLOCATE c
