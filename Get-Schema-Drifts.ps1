#Set-executionPolicy -ExecutionPolicy Unrestricted
Import-Module sqlps;
Clear-Host;
$StartDate=(Get-Date)

# Config values
$schema = "AdventureWorksLT"
$control_srv = 'localhost\mssql2019';
$control_db = "AdventureWorksLT";
$server_query = "SELECT 'localhost\MSSQL2019' AS ServerUrl UNION SELECT 'localhost\MSSQL2019B'"

############################################
# DO NOT TOUCH BELOW THIS LINE
############################################

$server_list =  Invoke-Sqlcmd -ServerInstance $control_srv `
                        -Database $control_db `
                        -DisableVariables `
                        -Query $server_query `
                        -ConnectionTimeout 0 `
                        -MaxCharLength 64000;

$sql = "SET NOCOUNT ON;
DECLARE @object_name	nvarchar(257) = '[@object_name]' 
DECLARE @sqlstring		nvarchar(MAX)
DECLARE @dbname			sysname = '[@dbname]'

SET @dbname = NULLIF(@dbname, '')
SET @object_name = NULLIF(@object_name, '')

IF OBJECT_ID('tempdb..#output') IS NOT NULL DROP TABLE #output

CREATE TABLE #output  (
    database_name sysname			NOT NULL, 
    object_type nvarchar(60)		NOT NULL, 
    object_name nvarchar(257)		NOT NULL, 
    object_definition nvarchar(MAX) NOT NULL
)

DECLARE dbs CURSOR FORWARD_ONLY READ_ONLY FAST_FORWARD LOCAL
	FOR SELECT name 
            FROM sys.databases 
 		    WHERE database_id > 4 
		        AND state = 0
		        AND (name = @dbname OR (@dbname IS NULL AND name LIKE 'AdventureWorksLT_%'))

OPEN dbs 
FETCH NEXT FROM dbs INTO @dbname 

WHILE @@FETCH_STATUS = 0 BEGIN 
	SET @sqlstring	= N'USE ' + QUOTENAME(@dbname) + N'

SET NOCOUNT ON;

SELECT DB_NAME() COLLATE DATABASE_DEFAULT AS [database_name] 
		, o.type_desc COLLATE DATABASE_DEFAULT AS [object_type]
		, OBJECT_SCHEMA_NAME(o.object_id) + ''.'' + OBJECT_NAME(o.object_id) COLLATE DATABASE_DEFAULT AS [object_name] 
		, LTRIM(RTRIM(OBJECT_DEFINITION(o.OBJECT_ID))) AS [object_definition]
	FROM sys.objects AS o
	WHERE o.type IN (''TR'',''FN'',''P'',''TF'',''V'')
        AND (o.object_id = OBJECT_ID(@object_name) OR @object_name IS NULL)

UNION ALL

SELECT DB_NAME() COLLATE DATABASE_DEFAULT AS [database_name]
		, type_desc COLLATE DATABASE_DEFAULT AS [type_desc] 
		, OBJECT_SCHEMA_NAME(object_id) + ''.'' + name COLLATE DATABASE_DEFAULT AS object_name
		, ''CREATE TABLE '' + QUOTENAME(OBJECT_SCHEMA_NAME(object_id)) + ''.'' + QUOTENAME(name) + ''('' + CHAR(10) +
			(STUFF(
					(SELECT '', '' + QUOTENAME(c.name) + '' '' + t.name + 
							CASE WHEN t.name IN (''char'', ''nchar'', ''binay'', ''varchar'', ''nvarchar'', ''varbinay'')
								THEN ''('' + CONVERT(SYSNAME, c.max_length) + '')'' 
								ELSE ''''
							END +
							CASE WHEN c.is_identity = 1 THEN '' IDENTITY'' ELSE '''' END +
							ISNULL('' COLLATE '' + c.collation_name, '''') +
							CASE WHEN c.is_nullable = 0 THEN '' NOT'' ELSE '''' END + '' NULL'' + CHAR(10)
						FROM sys.columns AS c
							INNER JOIN sys.types AS t
								ON t.user_type_id = c.user_type_id
						WHERE c.object_id = o.object_id
						ORDER BY column_id
						FOR XML PATH('''')), 1, 2, '''')) + '')'' COLLATE DATABASE_DEFAULT AS object_definition
	FROM sys.objects AS o
	WHERE type IN (''U'')
		AND OBJECTPROPERTY(object_id, ''IsMsShipped'') = 0
		AND (object_id = OBJECT_ID(@object_name) OR @object_name IS NULL)

UNION ALL

SELECT DB_NAME() COLLATE DATABASE_DEFAULT AS database_name 
		, o.type_desc COLLATE DATABASE_DEFAULT
		, SCHEMA_NAME(tt.schema_id) + ''.'' + tt.name COLLATE DATABASE_DEFAULT AS object_name
		, ''CREATE TYPE '' + QUOTENAME(SCHEMA_NAME(tt.schema_id)) + ''.'' + QUOTENAME(tt.name) + '' AS TABLE ('' + CHAR(10) +
			(STUFF(
					(SELECT '', '' + QUOTENAME(c.name) + '' '' + t.name + 
							CASE WHEN t.name IN (''char'', ''nchar'', ''binay'', ''varchar'', ''nvarchar'', ''varbinay'')
								THEN ''('' + CONVERT(SYSNAME, c.max_length) + '')'' 
								ELSE ''''
							END +
							CASE WHEN c.is_identity = 1 THEN '' IDENTITY'' ELSE '''' END +
							ISNULL('' COLLATE '' + c.collation_name, '''') +
							CASE WHEN c.is_nullable = 0 THEN '' NOT'' ELSE '''' END + '' NULL'' + CHAR(10)
						FROM sys.columns AS c
							INNER JOIN sys.types AS t
								ON t.user_type_id = c.user_type_id
						WHERE c.object_id = tt.type_table_object_id
						ORDER BY column_id
						FOR XML PATH('''')), 1, 2, '''')) + '')'' COLLATE DATABASE_DEFAULT AS object_definition
	FROM sys.objects AS o
		INNER JOIN sys.table_types AS tt
			ON tt.type_table_object_id = o.object_id
	WHERE (tt.type_table_object_id = OBJECT_ID(@object_name) OR @object_name IS NULL)
' 
	
	INSERT INTO #output (database_name, object_type, object_name, object_definition)
	EXECUTE sys.sp_executesql 
        @stmt = @sqlstring
        , @params = N'@object_name NVARCHAR(257)'
        , @object_name = @object_name;

	FETCH NEXT FROM dbs INTO @dbname 
END 
CLOSE dbs; 
DEALLOCATE dbs; 

SELECT SERVERPROPERTY('ComputerNamePhysicalNetBios') AS server_name
		, database_name
        , object_type
		, object_name
		, object_definition
        , CONVERT(varchar(100), HASHBYTES('md5', LTRIM(RTRIM(object_definition))),1) AS object_hash 		    
	FROM #output
    ORDER BY object_type
        , object_name";

$master_copy =  Invoke-Sqlcmd -ServerInstance $master_srv `
                                -Database $master_db `
                                -DisableVariables `
                                -Query $sql.Replace("[@object_name]", "").Replace("[@dbname]","$($master_db)") `
                                -ConnectionTimeout 0 `
                                -MaxCharLength 64000;

$object_types = $master_copy | Select-Object object_type -Unique

# to generate a single path per run
$path = "c:/temp/database_schemas/$($schema)/$(Get-Date -Format "yyyyMMddHHmmss")/";
if ( (Test-Path -path $path) -eq $false){
    #write-output "path does not exist"
    New-Item -path $path -ItemType Directory | Out-Null;
}

# to generate a single folder per object_type
foreach ($type in $object_types){
    if ( (Test-Path -Path "$($path)/$($type.object_type)" ) -eq $false){
        #write-output "path does not exist"
        New-Item -Path "$($path)/$($type.object_type)" -ItemType Directory | Out-Null;
    }    
}

foreach ($db_object in $master_copy){

    # to cummulate all copies of the object from all servers
    $local_copy = @();
    $errors = @();

    Write-Output "Processing $($db_object.object_type) => $($db_object.object_name)";

    $db_object_path = "$($path)/$($db_object.object_type)/$($db_object.object_name)";
        if ( (Test-Path -path $db_object_path) -eq $false){
            New-Item -path $db_object_path -ItemType Directory | Out-Null;
    }

    $outputfile = "$($db_object_path)/all_diffs.txt";
    $db_object | Select-Object database_name, object_name, object_hash | Out-File $outputfile -Append;

    foreach ($srv in $server_list){
    
        Write-Output "`tProcessing server $($srv.ServerUrl)";

        $objDataTable = Invoke-Sqlcmd -ServerInstance "$($srv.ServerUrl)" `
                                -Database "master" `
                                -DisableVariables `
                                -Query $sql.Replace("[@object_name]", $db_object.object_name).Replace("[@dbname]","") `
                                -ConnectionTimeout 0 `
                                -MaxCharLength 64000;                                

        if ($null -ne $objDataTable){
            $local_copy += $objDataTable;
        }
        else{
            $errors += [pscustomobject]@{server_name=$db.ServerUrl;database_name=$db.database_name;object_name=$db_object.object_name;object_hash=0x0};
        }
    }
    
    # Drifts are any row that does not match the object_hash
    # $local_copy | Select-Object database_name, objecT_hash | Where-Object -Property object_hash -ne $db_object.object_hash
    $drifts = $local_copy | Select-Object | Where-Object -Property object_hash -ne $db_object.object_hash

    if ($drifts.Count -eq 0){
    #if no drifts for this object, remove the noise
        Remove-Item -Path $db_object_path -Recurse;
    }
    else{
        # if there was any drift, get unique versions from all servers
        $diff_versions = $drifts | Select-Object object_hash, object_definition -Unique;

        # add each database that drifted to the list
        $errors | Select-Object server_name, database_name, object_name, object_hash | Out-File $outputfile -Append;
        $drifts | Select-Object server_name, database_name, object_name, object_hash | Sort-Object -Property object_hash | Out-File $outputfile -Append;
        
        # put the definition from the master object 
        $db_object.object_definition | Out-File "$($db_object_path)/$($db_object.object_name).sql" 

        foreach ($diff in $($diff_versions)){
            $diff.object_definition | Out-File "$($db_object_path)/$($diff.object_hash).sql" 
        }
    }
}

$EndDate=(Get-Date);

New-TimeSpan –Start $StartDate –End $EndDate | Format-Table -AutoSize;
