
-- run in master 
select  * from sys.resource_stats 
where start_time between '2017-05-04 07:00:20.4100000' and '2017-05-05 12:20:20.4100000' 
order by avg_cpu_percent desc




--run in user db

select * from sys.dm_db_resource_stats 
where end_time > '2017-05-08 12:39:47.760'



-- total size of database 
SELECT SUM(reserved_page_count)*8.0/1024
FROM sys.dm_db_partition_stats;
GO

-- Calculates the size of individual database objects.
SELECT '[' + sc.name +'].' +o.name, SUM(reserved_page_count) * 8.0 / 1024,min(type_desc)
FROM sys.dm_db_partition_stats, sys.objects o,sys.schemas sc 
WHERE sys.dm_db_partition_stats.object_id = o.object_id
and o.schema_id=sc.schema_id
 GROUP BY o.name,sc.name
order by 2 desc;


SELECT schema_name(schema_id), sys.objects.name, SUM(reserved_page_count) * 8.0 / 1024,min(type_desc)
FROM sys.dm_db_partition_stats, sys.objects
WHERE sys.dm_db_partition_stats.object_id = sys.objects.object_id

GROUP BY sys.objects.name,schema_id
order by 3 desc;

select top 10 * from sys.objects 
GO

-- top 5 query 

SELECT TOP 50 query_stats.query_hash AS "Query Hash",
SUM(query_stats.total_worker_time) / SUM(query_stats.execution_count) AS "Avg CPU Time",
sum(query_stats.total_elapsed_time)/ SUM(query_stats.execution_count) AS "Avg duration",
MIN(query_stats.statement_text) AS "Statement Text"
FROM
(SELECT QS.*,
SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1,
((CASE statement_end_offset
WHEN -1 THEN DATALENGTH(ST.text)
ELSE QS.statement_end_offset END
- QS.statement_start_offset)/2) + 1) AS statement_text
FROM sys.dm_exec_query_stats AS QS
CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) as ST) as query_stats
GROUP BY query_stats.query_hash
ORDER BY 3 DESC;
/* locks */

select * from sys.dm_tran_locks 
E_VenueSeat
/* missing index */
SELECT CONVERT (varchar, getdate(), 126) AS runtime,
mig.index_group_handle, mid.index_handle,
CONVERT (decimal (28,1), migs.avg_total_user_cost * migs.avg_user_impact *
(migs.user_seeks + migs.user_scans)) AS improvement_measure,
'CREATE INDEX missing_index_' + CONVERT (varchar, mig.index_group_handle) + '_' +
CONVERT (varchar, mid.index_handle) + ' ON ' + mid.statement + '
(' + ISNULL (mid.equality_columns,'')
+ CASE WHEN mid.equality_columns IS NOT NULL
AND mid.inequality_columns IS NOT NULL
THEN ',' ELSE '' END + ISNULL (mid.inequality_columns, '')
+ ')'
+ ISNULL (' INCLUDE (' + mid.included_columns + ')', '') AS create_index_statement,
migs.*,
mid.database_id,
mid.[object_id]
FROM sys.dm_db_missing_index_groups AS mig
INNER JOIN sys.dm_db_missing_index_group_stats AS migs
ON migs.group_handle = mig.index_group_handle
INNER JOIN sys.dm_db_missing_index_details AS mid
ON mig.index_handle = mid.index_handle
ORDER BY migs.avg_total_user_cost * migs.avg_user_impact * (migs.user_seeks + migs.user_scans) DESC

/* index usage */ 


select object_name(s.object_id),i.name
,'user reads' = user_seeks + user_scans + user_lookups
		,'system reads' = system_seeks + system_scans + system_lookups
		,'user writes' = user_updates
		,'system writes' = system_updates
from sys.dm_db_index_usage_stats s,sys.indexes i
where 
 object_name(s.object_id)='E_VenueSeat' and  i.object_id = s.object_id and  i.index_id = s.index_id


/* active sessions */ 



select r.session_id,r.total_elapsed_time/60000.0,
p.loginame  ,p.login_time ,p.nt_username ,p.net_address,p.program_name,
        p.blocked ,p.hostname,p.status 
		,r.status
		,substring(qt.text,r.statement_start_offset/2, 
			(case when r.statement_end_offset = -1 
			then len(convert(nvarchar(max), qt.text)) * 2 
			else r.statement_end_offset end -r.statement_start_offset)/2) 
		as query_text   --- this is the statement executing right now
		,qt.dbid
		,qt.objectid
		,r.cpu_time
		,r.reads
		,r.writes
		,r.logical_reads
		,r.scheduler_id
from sys.dm_exec_requests r
cross apply sys.dm_exec_sql_text(sql_handle) as qt
inner join sys.sysprocesses p on r.session_id = p.spid
where r.session_id > 50
order by total_elapsed_time desc
--order by r.scheduler_id, r.status, r.session_id

--last 1 hour average 
SELECT
AVG(avg_cpu_percent) AS 'Average CPU use in percent',
MAX(avg_cpu_percent) AS 'Maximum CPU use in percent',
AVG(avg_data_io_percent) AS 'Average data I/O in percent',
MAX(avg_data_io_percent) AS 'Maximum data I/O in percent',
AVG(avg_log_write_percent) AS 'Average log write use in percent',
MAX(avg_log_write_percent) AS 'Maximum log write use in percent',
AVG(avg_memory_usage_percent) AS 'Average memory use in percent',
MAX(avg_memory_usage_percent) AS 'Maximum memory use in percent'
FROM sys.dm_db_resource_stats;


-- query store 
SELECT actual_state, actual_state_desc, readonly_reason,   
    current_storage_size_mb, max_storage_size_mb  
FROM sys.database_query_store_options;

-- index contention 
Select  objectname=object_name(s.object_id)
	, indexname=i.name, i.index_id	--, partition_number
	, row_lock_count, row_lock_wait_count
	, [block %]=cast (100.0 * row_lock_wait_count / (1 + row_lock_count) as numeric(15,2))
	, row_lock_wait_in_ms
	, [avg row lock waits in ms]=cast (1.0 * row_lock_wait_in_ms / (1 + row_lock_wait_count) as numeric(15,2))
from sys.dm_db_index_operational_stats (null, NULL, NULL, NULL) s
	,sys.indexes i
where objectproperty(s.object_id,'IsUserTable') = 1
and i.object_id = s.object_id
and i.index_id = s.index_id
order by 4 desc

--query store 
select top 10 * from sys.query_store_runtime_stats where first_execution_Time 

--sys.query_store_query_text  Information about captured query texts. 
--sys.query_context_settings Different runtime combinations of semantics-affecting context settings (SET options that influence plan shape, language ID, ...) 
--sys.query_store_query Unique combination of query text and context settings 
--sys.query_store_plan Information about plans SQL Server uses to execute queries in the system. 
--sys.query_store_runtime_stats_interval Aggregation intervals (time windows) created in Query Store. 
--sys.query_store_runtime_stats Runtime statistics for executed query plans, aggregated on per-interval basis 

WITH AggregatedDurationLastHour
AS
(
   SELECT q.query_id, SUM(count_executions * avg_duration) AS total_duration,
   COUNT (distinct p.plan_id) AS number_of_plans
   FROM sys.query_store_query_text AS qt JOIN sys.query_store_query AS q 
   ON qt.query_text_id = q.query_text_id
   JOIN sys.query_store_plan AS p ON q.query_id = p.query_id
   JOIN sys.query_store_runtime_stats AS rs ON rs.plan_id = p.plan_id
   JOIN sys.query_store_runtime_stats_interval AS rsi 
   ON rsi.runtime_stats_interval_id = rs.runtime_stats_interval_id
   WHERE rsi.start_time >= DATEADD(hour, -1, GETUTCDATE()) 
   AND rs.execution_type_desc = 'Regular'
   GROUP BY q.query_id
)
,OrderedDuration
AS
(
   SELECT query_id, total_duration, number_of_plans, 
   ROW_NUMBER () OVER (ORDER BY total_duration DESC, query_id) AS RN
   FROM AggregatedDurationLastHour
)
SELECT qt.query_sql_text, object_name(q.object_id) AS containing_object,
total_duration AS total_duration_microseconds, number_of_plans,
CONVERT(xml, p.query_plan) AS query_plan_xml, p.is_forced_plan, p.last_compile_start_time,q.last_execution_time
FROM OrderedDuration od JOIN sys.query_store_query AS q ON q.query_id  = od.query_id
JOIN sys.query_store_query_text qt ON q.query_text_id = qt.query_text_id
JOIN sys.query_store_plan p ON q.query_id = p.query_id
WHERE OD.RN <=25 ORDER BY total_duration DESC

--only on azure 
-- will be effected by upgrade 
select * from sys.dm_db_objects_impacted_on_version_change  

-- wait stats 
WITH Waits
AS (SELECT wait_type, CAST(wait_time_ms / 1000. AS DECIMAL(12, 2)) AS [wait_time_s],
	CAST(100. * wait_time_ms / SUM(wait_time_ms) OVER () AS DECIMAL(12,2)) AS [pct],
	ROW_NUMBER() OVER (ORDER BY wait_time_ms DESC) AS rn
	FROM  sys.dm_db_wait_stats WITH (NOLOCK)
	WHERE wait_type NOT IN (N'CLR_SEMAPHORE', N'LAZYWRITER_SLEEP', N'RESOURCE_QUEUE',N'SLEEP_TASK',
			                N'SLEEP_SYSTEMTASK', N'SQLTRACE_BUFFER_FLUSH', N'WAITFOR', N'LOGMGR_QUEUE',
			                N'CHECKPOINT_QUEUE', N'REQUEST_FOR_DEADLOCK_SEARCH', N'XE_TIMER_EVENT',
			                N'BROKER_TO_FLUSH', N'BROKER_TASK_STOP', N'CLR_MANUAL_EVENT', N'CLR_AUTO_EVENT',
			                N'DISPATCHER_QUEUE_SEMAPHORE' ,N'FT_IFTS_SCHEDULER_IDLE_WAIT', N'XE_DISPATCHER_WAIT',
			                N'XE_DISPATCHER_JOIN', N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', N'ONDEMAND_TASK_QUEUE',
			                N'BROKER_EVENTHANDLER', N'SLEEP_BPOOL_FLUSH', N'SLEEP_DBSTARTUP', N'DIRTY_PAGE_POLL',
			                N'HADR_FILESTREAM_IOMGR_IOCOMPLETION',N'SP_SERVER_DIAGNOSTICS_SLEEP',
							N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP', N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',
							N'WAIT_XTP_HOST_WAIT', N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG', N'WAIT_XTP_CKPT_CLOSE')),
Running_Waits 
AS (SELECT W1.wait_type, wait_time_s, pct,
	SUM(pct) OVER(ORDER BY pct DESC ROWS UNBOUNDED PRECEDING) AS [running_pct]
	FROM Waits AS W1)
SELECT wait_type, wait_time_s, pct, running_pct
FROM Running_Waits
WHERE running_pct - pct <= 99
ORDER BY running_pct
OPTION (RECOMPILE);

--active transactions 
SELECT TranSession.[session_id], SessionS.[login_name] AS [Login Name], TranDB.[database_transaction_begin_time] AS [Start_Time], 

  CASE TranActive.transaction_type 

     WHEN 1 THEN 'Read/write transaction'

     WHEN 2 THEN 'Read-only transaction'

     WHEN 3 THEN 'System transaction'

  END AS [Transaction_Type], 

  CASE TranActive.transaction_state 

    WHEN 1 THEN 'The transaction has not been initialized'

    WHEN 2 THEN 'The transaction is active'

    WHEN 3 THEN 'The transaction has ended. This is used for read-only transactions'

    WHEN 5 THEN 'The transaction is in a prepared state and waiting resolution.'

    WHEN 6 THEN 'The transaction has been committed' 

    WHEN 7 THEN 'The transaction is being rolled back' 

    WHEN 8 THEN 'The transaction has been rolled back'

   END AS [Transaction_State], 

    TranDB.[database_transaction_log_record_count] AS [Log_Records], 

    TranDB.[database_transaction_log_bytes_used] AS [Log_Bytes_Used], 

    SQlText.[text] AS [Last_Transaction_Text], 

    SQLQP.[query_plan] AS [Last_Query_Plan] 

  FROM sys.dm_tran_database_transactions TranDB 

   INNER JOIN sys.dm_tran_session_transactions TranSession 

      ON TranSession.[transaction_id] = TranDB.[transaction_id] 

   INNER JOIN sys.dm_tran_active_transactions TranActive 

      ON TranSession.[transaction_id] = TranActive.[transaction_id] 

    INNER JOIN sys.dm_exec_sessions SessionS 

      ON SessionS.[session_id] = TranSession.[session_id] 

    INNER JOIN sys.dm_exec_connections Connections 

      ON Connections.[session_id] = TranSession.[session_id] 

    LEFT JOIN sys.dm_exec_requests Request 

      ON Request.[session_id] = TranSession.[session_id] 

    CROSS APPLY sys.dm_exec_sql_text (Connections.[most_recent_sql_handle]) AS SQlText 

    OUTER APPLY sys.dm_exec_query_plan (Request.[plan_handle]) AS SQLQP 



--Returns information about operations performed on databases in a Azure SQL Database server. run in master 

select * from sys.dm_operation_status
--7DA62A4C-0624-479E-B1F4-BBE38F073746	0	Database	Nirvana		ALTER DATABASE	2	COMPLETED	100	0		0	0	2017-04-13 08:50:56.897	2017-04-13 08:55:35.283
--Create database
--Copy database. Database Copy creates a record in this view on both the source and target servers.
--Alter database
--Change the performance level of a service tier
--Change the service tier of a database, such as changing from Basic to Standard.
--Setting up a Geo-Replication relationship
--Terminating a Geo-Replication relationship
--Restore database
--Delete database

select * from sys.dm_geo_replication_link_status
-- use master 
select * from sys.geo_replication_links
-- 
select sum(event_count),convert(char(8),start_time,112),datepart(hh,start_time) from sys.event_log 
group by convert(char(8),start_time,112),datepart(hh,start_time) 
order by 2 desc ,3 desc 

select * from sys.dm_continuous_copy_status


select * from sys.event_log

select count(*) from sys.dm_exec_requests

select count(*) from sys.dm_exec_sessions



CREATE EVENT SESSION [appuser] ON DATABASE 
ADD EVENT sqlserver.error_reported(
    ACTION(sqlserver.client_app_name,sqlserver.database_id,sqlserver.query_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)
    WHERE ((([package0].[divides_by_uint64]([sqlserver].[session_id],(5))) AND ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)))) AND ([sqlserver].[username]=N'nirvana_appuser'))),
ADD EVENT sqlserver.rpc_completed(
    ACTION(sqlserver.client_app_name,sqlserver.database_id,sqlserver.query_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)
    WHERE ((([package0].[divides_by_uint64]([sqlserver].[session_id],(5))) AND ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)))) AND ([sqlserver].[username]=N'nirvana_appuser'))),
ADD EVENT sqlserver.sp_statement_completed(
    ACTION(sqlserver.sql_text,sqlserver.username)
    WHERE ([sqlserver].[username]=N'nirvana_appuser')),
ADD EVENT sqlserver.sql_batch_completed(
    ACTION(sqlserver.client_app_name,sqlserver.database_id,sqlserver.query_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)
    WHERE ((([package0].[divides_by_uint64]([sqlserver].[session_id],(5))) AND ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)))) AND ([sqlserver].[username]=N'nirvana_appuser')))
ADD TARGET package0.asynchronous_file_target(SET filename=N'https://<blobstorage>/FileName.xel'),
ADD TARGET package0.ring_buffer(SET max_memory=(4096))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=OFF)
GO



CREATE EVENT SESSION [LongRunningQuery] ON DATABASE 
ADD EVENT sqlserver.rpc_completed(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.query_hash,sqlserver.session_id,sqlserver.username)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(100000))),
ADD EVENT sqlserver.sql_batch_completed(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.query_hash,sqlserver.session_id,sqlserver.username)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(100000))),
ADD EVENT sqlserver.sql_statement_completed(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.database_id,sqlserver.query_hash,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.username)
    WHERE ([package0].[greater_than_uint64]([sqlserver].[database_id],(4)) AND [package0].[equal_boolean]([sqlserver].[is_system],(0)) AND [duration]>(100000)))
ADD TARGET package0.asynchronous_file_target(SET filename=N'https://<blobstorage>/FileName.xel'),
ADD TARGET package0.ring_buffer(SET max_events_limit=(10000))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=OFF)
GO

CREATE EVENT SESSION [Capture_BlockedProcessReport] ON DATABASE 
ADD EVENT sqlserver.blocked_process_report
ADD TARGET package0.asynchronous_file_target(SET filename=N'https://dbsexportimport01.blob.core.windows.net/exportfolder/Prod_Falcon_blocking.xel'),
ADD TARGET package0.ring_buffer(SET max_events_limit=(10000))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=OFF)
GO




CREATE EVENT SESSION [ddl_change] ON DATABASE 
ADD EVENT sqlserver.object_altered(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.sql_text,sqlserver.username)),
ADD EVENT sqlserver.object_created(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.sql_text,sqlserver.username)),
ADD EVENT sqlserver.object_deleted(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname,sqlserver.sql_text,sqlserver.username))
ADD TARGET package0.asynchronous_file_target(SET filename=N'https://dbsexportimport01.blob.core.windows.net/exportfolder/Prod_Falcon_ddl.xel'),
ADD TARGET package0.ring_buffer(SET max_memory=(4096))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)
GO


	CREATE EVENT SESSION [login] ON DATABASE 
ADD EVENT sqlserver.login(SET collect_options_text=(1)
    ACTION(sqlserver.client_app_name,sqlserver.client_connection_id,sqlserver.client_hostname,sqlserver.context_info,sqlserver.username)),
ADD EVENT sqlserver.logout(
    ACTION(sqlserver.client_app_name,sqlserver.client_connection_id,sqlserver.client_hostname,sqlserver.context_info,sqlserver.session_id,sqlserver.username))
ADD TARGET package0.asynchronous_file_target(SET filename=N'https://<blobstorage>/login_loout.xel'),
ADD TARGET package0.ring_buffer(SET max_events_limit=(50000))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=ON,STARTUP_STATE=OFF)
GO

DBCC FLUSHAUTHCACHE 


  select * from sys.dm_os_performance_counters

 /* run in master*/

SELECT
   object_name
  ,CAST(f.event_data as XML).value
      ('(/event/@timestamp)[1]', 'datetime2')                      AS [timestamp]
  ,CAST(f.event_data as XML).value
      ('(/event/data[@name="error"]/value)[1]', 'int')             AS [error]
  ,CAST(f.event_data as XML).value
      ('(/event/data[@name="state"]/value)[1]', 'int')             AS [state]
  ,CAST(f.event_data as XML).value
      ('(/event/data[@name="is_success"]/value)[1]', 'bit')        AS [is_success]
  ,CAST(f.event_data as XML).value
      ('(/event/data[@name="database_name"]/value)[1]', 'sysname') AS [database_name]
FROM
  sys.fn_xe_telemetry_blob_target_read_file('el', null, null, null) AS f
WHERE
  object_name != 'login_event'  -- Login events are numerous.
  and
  '2015-06-21' < CAST(f.event_data as XML).value
        ('(/event/@timestamp)[1]', 'datetime2')
ORDER BY
  [timestamp] DESC
;

SELECT e.*
FROM sys.event_log AS e
WHERE e.database_name = 'myDbName'
AND e.event_category = 'connectivity'
AND 2 >= DateDiff
  (hour, e.end_time, GetUtcDate())
ORDER BY e.event_category,
  e.event_type, e.end_time;

SELECT c.*
FROM sys.database_connection_stats AS c
WHERE c.database_name = 'myDbName'
AND 24 >= DateDiff
  (hour, c.end_time, GetUtcDate())
ORDER BY c.end_time; 

CREATE EVENT SESSION [error_capture] ON DATABASE 
ADD EVENT sqlserver.error_reported(
    ACTION(sqlserver.client_app_name,sqlserver.client_hostname))
ADD TARGET package0.asynchronous_file_target(SET filename=N'https://<blobstorage>/FileName.xel'),
ADD TARGET package0.ring_buffer(SET max_memory=(4096))
WITH (MAX_MEMORY=4096 KB,EVENT_RETENTION_MODE=ALLOW_SINGLE_EVENT_LOSS,MAX_DISPATCH_LATENCY=30 SECONDS,MAX_EVENT_SIZE=0 KB,MEMORY_PARTITION_MODE=NONE,TRACK_CAUSALITY=OFF,STARTUP_STATE=OFF)





SELECT 
sum(event_count),event_type,convert(char(8),start_time,112)
--e.*
FROM sys.event_log AS e
WHERE e.database_name = 'Nirvana'
and event_type='connection_failed'
AND e.event_category = 'connectivity'
AND 2400 >= DateDiff
  (hour, e.end_time, GetUtcDate())
  group by convert(char(8),start_time,112),event_type
  order by 1 desc


/* Kullanýlmayan ve drop edilebilecek indeksler*/
SELECT A.*,b.* from
(
select 
OBJECT_NAME(PS.object_id, database_id) object_name,
avg_fragmentation_in_percent,
I.name index_name
from sys.dm_db_index_physical_stats(DB_ID(), null, null, null, 'LIMITED') PS
join sys.indexes I on PS.object_id = I.object_id and PS.index_id = I.index_id
where avg_fragmentation_in_percent>5 and page_count>10 --ideal >1000
) A 
inner join 
(
select object_name(s.object_id) object_name,i.name index_name
/*
,'user reads' = user_seeks + user_scans + user_lookups
		,'system reads' = system_seeks + system_scans + system_lookups
		,'user writes' = user_updates
		,'system writes' = system_updates */ 
from sys.dm_db_index_usage_stats s,sys.indexes i
where 
-- object_name(s.object_id)='E_VenueSeat' and 
  i.object_id = s.object_id and  i.index_id = s.index_id
  and user_seeks + user_scans + user_lookups=0
) B
on A.object_name = B.object_name and A.index_name=B.index_name
where avg_fragmentation_in_percent>50
order by 2 desc



SELECT TOP 10
   ps.total_elapsed_time , ps.execution_count/DATEDIFF(MI ,ps.cached_time,GETDATE()),ps.total_elapsed_time/ps.execution_count/1000000.0  as 'sure',ps.total_logical_reads/ ps.execution_count ,p.name AS [SP Name]
    , ps.execution_count
    , ps.cached_time,ps.plan_handle ,qp.query_plan 
FROM 
    sys.procedures p WITH (NOLOCK)
INNER JOIN 
    sys.dm_exec_procedure_stats ps WITH (NOLOCK)
	cross apply sys.dm_exec_query_plan (ps.plan_handle ) as qp 
	inner join sys.dm_exec_cached_plans  cp  on ps.plan_handle = cp.plan_handle 
	ON  p.[object_id] = ps.[object_id]
WHERE 
    ps.database_id = DB_ID() 
	--and p.objecT_id =1190555575  --1177315504 1127831230
	--AND ps.execution_count/DATEDIFF(MI ,'2016-06-21 10:21:42.770',GETDATE()) > 10
ORDER BY 
    1 DESC 
OPTION 
    (RECOMPILE);



select *,
'alter index ' + '['+index_name +']'+ ' on ' + schema_name  + '.' + OBJECT_NAME(object_id, database_id) + Maintenance_Mode stmt 
--into msdb.dbo.tmp_indexdefrag
from (
select DB_NAME(database_id) database_name, database_id,
OBJECT_SCHEMA_NAME(PS.object_id, database_id) schema_name,
OBJECT_NAME(PS.object_id, database_id) object_name,  PS.object_id, 
avg_fragmentation_in_percent,
I.name index_name, PS.index_id,
Maintenance_Mode = case when avg_fragmentation_in_percent>25 then ' REBUILD WITH (ONLINE=ON) ' else 'REORGANIZE' end
from sys.dm_db_index_physical_stats(DB_ID(), null, null, null, 'LIMITED') PS
join sys.indexes I on PS.object_id = I.object_id and PS.index_id = I.index_id
where avg_fragmentation_in_percent>5 and page_count>10 --ideal >1000
) X
order by X.avg_fragmentation_in_percent desc
