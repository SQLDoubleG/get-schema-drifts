RESTORE FILELISTONLY FROM DISK = 'AdventureWorksLT2019.bak'

DECLARE @dataPath nvarchar(512) = CONVERT(nvarchar(512), SERVERPROPERTY('InstanceDefaultDataPath'));
DECLARE @logPath nvarchar(512)	= CONVERT(nvarchar(512), SERVERPROPERTY('InstanceDefaultLogPath'));

DECLARE @count int = 101;
DECLARE @maxdb int = 200;
DECLARE @dbname sysname;

DECLARE @sql nvarchar(MAX) = N'RESTORE DATABASE [?] FROM DISK = ''AdventureWorksLT2019.bak''
WITH RECOVERY, REPLACE 
, MOVE ''AdventureWorksLT2012_Data''	TO ''[datapath]?.mdf''
, MOVE ''AdventureWorksLT2012_Log''		TO ''[logpath]?_log.ldf''';

DECLARE @restoresql nvarchar(MAX);


WHILE @count <= @maxdb BEGIN
	SET @dbname = 'AdventureWorksLT_' + RIGHT('000' +  CONVERT(varchar(5), @count), 3);
	SET @restoresql = REPLACE(@sql, '?', @dbname);
	SET @restoresql = REPLACE(@restoresql, '[datapath]', @dataPath);
	SET @restoresql = REPLACE(@restoresql, '[logpath]', @logPath);

	PRINT @restoresql;
	EXECUTE sys.sp_executesql @stmt = @restoresql;
	SET @count += 1;
END

GO
