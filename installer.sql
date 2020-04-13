/*

Installer for collecting SQL Server Query execution statistics
Created by Balazs Berki balazsberki.com

Usage:
After executiing the below commands to create the tables and procedures, create an SQL Server Agent Job with the following T-SQL command:

exec [dbo].[collect_execution_statistics]

*/

CREATE TABLE [dbo].[query_statistics](
	[timeStamp] [datetime2](7) NULL,
	[dbid] [int] NULL,
	[objectid] [bigint] NULL,
	[text] [nvarchar](max) NULL,
	[sql_handle] [varbinary](64) NULL,
	[statement_start_offset] [int] NULL,
	[statement_end_offset] [int] NULL,
	[plan_generation_num] [bigint] NULL,
	[plan_handle] [varbinary](64) NULL,
	[creation_time] [datetime] NULL,
	[last_execution_time] [datetime] NULL,
	[execution_count] [bigint] NULL,
	[total_worker_time] [bigint] NULL,
	[last_worker_time] [bigint] NULL,
	[min_worker_time] [bigint] NULL,
	[max_worker_time] [bigint] NULL,
	[total_physical_reads] [bigint] NULL,
	[last_physical_reads] [bigint] NULL,
	[min_physical_reads] [bigint] NULL,
	[max_physical_reads] [bigint] NULL,
	[total_logical_writes] [bigint] NULL,
	[last_logical_writes] [bigint] NULL,
	[min_logical_writes] [bigint] NULL,
	[max_logical_writes] [bigint] NULL,
	[total_logical_reads] [bigint] NULL,
	[last_logical_reads] [bigint] NULL,
	[min_logical_reads] [bigint] NULL,
	[max_logical_reads] [bigint] NULL,
	[total_clr_time] [bigint] NULL,
	[last_clr_time] [bigint] NULL,
	[min_clr_time] [bigint] NULL,
	[max_clr_time] [bigint] NULL,
	[total_elapsed_time] [bigint] NULL,
	[last_elapsed_time] [bigint] NULL,
	[min_elapsed_time] [bigint] NULL,
	[max_elapsed_time] [bigint] NULL,
	[query_hash] [binary](8) NULL,
	[query_plan_hash] [binary](8) NULL,
	[total_rows] [bigint] NULL,
	[last_rows] [bigint] NULL,
	[min_rows] [bigint] NULL,
	[max_rows] [bigint] NULL,
	[statement_sql_handle] [varbinary](64) NULL,
	[statement_context_id] [bigint] NULL,
	[total_dop] [bigint] NULL,
	[last_dop] [bigint] NULL,
	[min_dop] [bigint] NULL,
	[max_dop] [bigint] NULL,
	[total_grant_kb] [bigint] NULL,
	[last_grant_kb] [bigint] NULL,
	[min_grant_kb] [bigint] NULL,
	[max_grant_kb] [bigint] NULL,
	[total_used_grant_kb] [bigint] NULL,
	[last_used_grant_kb] [bigint] NULL,
	[min_used_grant_kb] [bigint] NULL,
	[max_used_grant_kb] [bigint] NULL,
	[total_ideal_grant_kb] [bigint] NULL,
	[last_ideal_grant_kb] [bigint] NULL,
	[min_ideal_grant_kb] [bigint] NULL,
	[max_ideal_grant_kb] [bigint] NULL,
	[total_reserved_threads] [bigint] NULL,
	[last_reserved_threads] [bigint] NULL,
	[min_reserved_threads] [bigint] NULL,
	[max_reserved_threads] [bigint] NULL,
	[total_used_threads] [bigint] NULL,
	[last_used_threads] [bigint] NULL,
	[min_used_threads] [bigint] NULL,
	[max_used_threads] [bigint] NULL
)
GO


CREATE TABLE [dbo].[procedure_statistics](
	[timeStamp] [datetime2](7) NULL,
	[database_id] [int] NOT NULL,
	[object_id] [int] NOT NULL,
	[type] [char](2) NULL,
	[type_desc] [nvarchar](60) NULL,
	[sql_handle] [varbinary](64) NOT NULL,
	[plan_handle] [varbinary](64) NOT NULL,
	[cached_time] [datetime] NULL,
	[last_execution_time] [datetime] NULL,
	[execution_count] [bigint] NOT NULL,
	[total_worker_time] [bigint] NOT NULL,
	[last_worker_time] [bigint] NOT NULL,
	[min_worker_time] [bigint] NOT NULL,
	[max_worker_time] [bigint] NOT NULL,
	[total_physical_reads] [bigint] NOT NULL,
	[last_physical_reads] [bigint] NOT NULL,
	[min_physical_reads] [bigint] NOT NULL,
	[max_physical_reads] [bigint] NOT NULL,
	[total_logical_writes] [bigint] NOT NULL,
	[last_logical_writes] [bigint] NOT NULL,
	[min_logical_writes] [bigint] NOT NULL,
	[max_logical_writes] [bigint] NOT NULL,
	[total_logical_reads] [bigint] NOT NULL,
	[last_logical_reads] [bigint] NOT NULL,
	[min_logical_reads] [bigint] NOT NULL,
	[max_logical_reads] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL,
	[last_elapsed_time] [bigint] NOT NULL,
	[min_elapsed_time] [bigint] NOT NULL,
	[max_elapsed_time] [bigint] NOT NULL
)
GO



CREATE PROCEDURE [dbo].[collect_execution_statistics]
AS
BEGIN
	declare @ts datetime2 = getdate()
	declare @tsPrev datetime2 =  dateadd(hh,-1,getdate())
	declare @elaThreshold bigint = 60000000

	insert into dbo.procedure_statistics
	select @ts as timeStamp
	, [database_id]
	, [object_id]
	, [type]
	, [type_desc]
	, [sql_handle]
	, [plan_handle]
	, [cached_time]
	, [last_execution_time]
	, [execution_count]
	, [total_worker_time]
	, [last_worker_time]
	, [min_worker_time]
	, [max_worker_time]
	, [total_physical_reads]
	, [last_physical_reads]
	, [min_physical_reads]
	, [max_physical_reads]
	, [total_logical_writes]
	, [last_logical_writes]
	, [min_logical_writes]
	, [max_logical_writes]
	, [total_logical_reads]
	, [last_logical_reads]
	, [min_logical_reads]
	, [max_logical_reads]
	, [total_elapsed_time]
	, [last_elapsed_time]
	, [min_elapsed_time]
	, [max_elapsed_time]
	from sys.dm_exec_procedure_stats with (NOLOCK)
	where last_execution_time > @tsPrev
	and total_elapsed_time > @elaThreshold 

	insert into dbo.query_statistics
	select @ts as timeStamp
	 , t.dbid
	 , t.objectid
	 , case 
		when t.objectid is null then t.text
		else SUBSTRING(t.text,s.statement_start_offset/2,(CASE WHEN s.statement_end_offset = -1 then LEN(CONVERT(nvarchar(max), t.text)) * 2 ELSE s.statement_end_offset end - s.statement_start_offset)/2)
	  end
	 , s.[sql_handle]
	 , s.[statement_start_offset]
	 , s.[statement_end_offset]
	 , s.[plan_generation_num]
	 , s.[plan_handle]
	 , s.[creation_time]
	 , s.[last_execution_time]
	 , s.[execution_count]
	 , s.[total_worker_time]
	 , s.[last_worker_time]
	 , s.[min_worker_time]
	 , s.[max_worker_time]
	 , s.[total_physical_reads]
	 , s.[last_physical_reads]
	 , s.[min_physical_reads]
	 , s.[max_physical_reads]
	 , s.[total_logical_writes]
	 , s.[last_logical_writes]
	 , s.[min_logical_writes]
	 , s.[max_logical_writes]
	 , s.[total_logical_reads]
	 , s.[last_logical_reads]
	 , s.[min_logical_reads]
	 , s.[max_logical_reads]
	 , s.[total_clr_time]
	 , s.[last_clr_time]
	 , s.[min_clr_time]
	 , s.[max_clr_time]
	 , s.[total_elapsed_time]
	 , s.[last_elapsed_time]
	 , s.[min_elapsed_time]
	 , s.[max_elapsed_time]
	 , s.[query_hash]
	 , s.[query_plan_hash]
	 , s.[total_rows]
	 , s.[last_rows]
	 , s.[min_rows]
	 , s.[max_rows]
	 , s.[statement_sql_handle]
	 , s.[statement_context_id]
	 , s.[total_dop]
	 , s.[last_dop]
	 , s.[min_dop]
	 , s.[max_dop]
	 , s.[total_grant_kb]
	 , s.[last_grant_kb]
	 , s.[min_grant_kb]
	 , s.[max_grant_kb]
	 , s.[total_used_grant_kb]
	 , s.[last_used_grant_kb]
	 , s.[min_used_grant_kb]
	 , s.[max_used_grant_kb]
	 , s.[total_ideal_grant_kb]
	 , s.[last_ideal_grant_kb]
	 , s.[min_ideal_grant_kb]
	 , s.[max_ideal_grant_kb]
	 , s.[total_reserved_threads]
	 , s.[last_reserved_threads]
	 , s.[min_reserved_threads]
	 , s.[max_reserved_threads]
	 , s.[total_used_threads]
	 , s.[last_used_threads]
	 , s.[min_used_threads]
	 , s.[max_used_threads]
	from sys.dm_exec_query_stats s with (NOLOCK)
	CROSS APPLY sys.dm_exec_sql_text(s.sql_handle) AS t
	where last_execution_time > @tsPrev
	and total_elapsed_time > @elaThreshold

END
GO


CREATE FUNCTION [dbo].[get_query_statistics](@start_date datetime2 = NULL
	, @end_date datetime2 = NULL
	, @sql_handle varbinary(64) = NULL
	, @plan_handle varbinary(64) = NULL)
RETURNS @q_stats_temp TABLE(
	[start_date] [datetime2](7) NULL,
	[end_date] [datetime2](7) NULL,
	[db_name] [NVARCHAR](512) NULL,
	[objectid] [bigint] NULL,
	[text] [nvarchar](max) NULL,
	[sql_handle] [varbinary](64) NULL,
	[statement_start_offset] [int] NULL,
	[statement_end_offset] [int] NULL,
	[plan_generation_num] [bigint] NULL,
	[plan_handle] [varbinary](64) NULL,
	[creation_time] [datetime] NULL,
	[last_execution_time] [datetime] NULL,
	[execution_count] [bigint] NULL,
	[worker_time] [bigint] NULL,
	[last_worker_time] [bigint] NULL,
	[min_worker_time] [bigint] NULL,
	[max_worker_time] [bigint] NULL,
	[physical_reads] [bigint] NULL,
	[last_physical_reads] [bigint] NULL,
	[min_physical_reads] [bigint] NULL,
	[max_physical_reads] [bigint] NULL,
	[logical_writes] [bigint] NULL,
	[last_logical_writes] [bigint] NULL,
	[min_logical_writes] [bigint] NULL,
	[max_logical_writes] [bigint] NULL,
	[logical_reads] [bigint] NULL,
	[last_logical_reads] [bigint] NULL,
	[min_logical_reads] [bigint] NULL,
	[max_logical_reads] [bigint] NULL,
	[clr_time] [bigint] NULL,
	[last_clr_time] [bigint] NULL,
	[min_clr_time] [bigint] NULL,
	[max_clr_time] [bigint] NULL,
	[elapsed_time] [bigint] NULL,
	[last_elapsed_time] [bigint] NULL,
	[min_elapsed_time] [bigint] NULL,
	[max_elapsed_time] [bigint] NULL,
	[query_hash] [binary](8) NULL,
	[query_plan_hash] [binary](8) NULL,
	[rows] [bigint] NULL,
	[last_rows] [bigint] NULL,
	[min_rows] [bigint] NULL,
	[max_rows] [bigint] NULL,
	[statement_sql_handle] [varbinary](64) NULL,
	[statement_context_id] [bigint] NULL,
	[total_dop] [bigint] NULL,
	[last_dop] [bigint] NULL,
	[min_dop] [bigint] NULL,
	[max_dop] [bigint] NULL,
	[total_grant_kb] [bigint] NULL,
	[last_grant_kb] [bigint] NULL,
	[min_grant_kb] [bigint] NULL,
	[max_grant_kb] [bigint] NULL,
	[total_used_grant_kb] [bigint] NULL,
	[last_used_grant_kb] [bigint] NULL,
	[min_used_grant_kb] [bigint] NULL,
	[max_used_grant_kb] [bigint] NULL,
	[total_ideal_grant_kb] [bigint] NULL,
	[last_ideal_grant_kb] [bigint] NULL,
	[min_ideal_grant_kb] [bigint] NULL,
	[max_ideal_grant_kb] [bigint] NULL,
	[total_reserved_threads] [bigint] NULL,
	[last_reserved_threads] [bigint] NULL,
	[min_reserved_threads] [bigint] NULL,
	[max_reserved_threads] [bigint] NULL,
	[total_used_threads] [bigint] NULL,
	[last_used_threads] [bigint] NULL,
	[min_used_threads] [bigint] NULL,
	[max_used_threads] [bigint] NULL,
	INDEX ix_dates clustered(start_date, end_date)
)
AS
BEGIN
	DECLARE @sqlPlanHandle VARBINARY(64)
	DECLARE @sqlHandle VARBINARY(64)
	DECLARE @statement_start_offset INT
	DECLARE @query_hash binary(8)
	DECLARE @query_plan_hash binary(8)
	DECLARE @plan_gen_num INT
	DECLARE @calc_start_date datetime2
	DECLARE @min_possible_date datetime2
	
	select top 1 @min_possible_date = timestamp 
	from dbo.query_statistics s
	where timestamp > ( select MIN(timestamp) from dbo.query_statistics )
	order by timestamp
	
	IF @start_date is NULL SET @start_date = GETDATE()-1
	IF @start_date < @min_possible_date SET @calc_start_date = @min_possible_date
	ELSE select top 1 @calc_start_date = timestamp 
		from dbo.query_statistics s
		where timestamp >= @start_date
		order by timestamp
	
	IF @end_date is NULL SET @end_date = GETDATE()
	
	DECLARE sql_list CURSOR for
	select distinct sql_handle, plan_handle, statement_start_offset, query_hash, query_plan_hash, plan_generation_num
	from dbo.query_statistics
	where timestamp between @calc_start_date and @end_date
	and (@sql_handle is NULL OR sql_handle = @sql_handle)
	and (@plan_handle is NULL OR plan_handle = @plan_handle)
	
	OPEN sql_list
	FETCH NEXT FROM sql_list INTO @sqlHandle, @sqlPlanHandle, @statement_start_offset, @query_hash, @query_plan_hash, @plan_gen_num
	
	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		with result as (
		select
			isnull(lag(timestamp) OVER (order by timestamp), ( select top 1 timestamp from dbo.query_statistics where timestamp < s.timestamp order by timestamp desc)) as start_date
			, timestamp as end_date
			, db_name([dbid]) as [db_name]
			, [objectid]
			, [text]
			, [sql_handle]
			, [statement_start_offset]
			, [statement_end_offset]
			, [plan_generation_num]
			, [plan_handle]
			, [creation_time]
			, [last_execution_time]
			, isnull(execution_count - lag(execution_count) OVER (order by timestamp), execution_count) as execution_count
			, isnull([total_worker_time] - lag(total_worker_time) OVER (order by timestamp), total_worker_time) as worker_time
			, [last_worker_time]
			, [min_worker_time]
			, [max_worker_time]
			, isnull([total_physical_reads] - lag([total_physical_reads]) OVER (order by timestamp), total_physical_reads) as [physical_reads]
			, [last_physical_reads]
			, [min_physical_reads]
			, [max_physical_reads]
			, isnull([total_logical_writes] - lag(total_logical_writes) OVER (order by timestamp), total_logical_writes) as [logical_writes]
			, [last_logical_writes]
			, [min_logical_writes] 
			, [max_logical_writes] 
			, isnull([total_logical_reads] - lag(total_logical_reads) OVER (order by timestamp), total_logical_reads) as [logical_reads]
			, [last_logical_reads] 
			, [min_logical_reads] 
			, [max_logical_reads] 
			, isnull([total_clr_time] - lag(total_clr_time) OVER (order by timestamp), total_clr_time) as [clr_time]
			, [last_clr_time] 
			, [min_clr_time] 
			, [max_clr_time] 
			, isnull([total_elapsed_time]  - lag(total_elapsed_time) OVER (order by timestamp), total_elapsed_time) as [elapsed_time]
			, [last_elapsed_time] 
			, [min_elapsed_time] 
			, [max_elapsed_time] 
			, [query_hash]
			, [query_plan_hash]
			, isnull([total_rows]   - lag(total_rows) OVER (order by timestamp), total_rows) as [rows]
			, [last_rows] 
			, [min_rows] 
			, [max_rows] 
			, [statement_sql_handle]
			, [statement_context_id] 
			, isnull([total_dop]    - lag(total_dop) OVER (order by timestamp), total_dop) as [total_dop]
			, [last_dop] 
			, [min_dop] 
			, [max_dop] 
			, isnull([total_grant_kb] - lag(total_grant_kb) OVER (order by timestamp), total_grant_kb) as [total_grant_kb]
			, [last_grant_kb] 
			, [min_grant_kb] 
			, [max_grant_kb] 
			, isnull([total_used_grant_kb]  - lag(total_used_grant_kb) OVER (order by timestamp), total_used_grant_kb) as [total_used_grant_kb]
			, [last_used_grant_kb] 
			, [min_used_grant_kb] 
			, [max_used_grant_kb] 
			, isnull([total_ideal_grant_kb] - lag(total_ideal_grant_kb) OVER (order by timestamp), total_ideal_grant_kb) as [total_ideal_grant_kb] 
			, [last_ideal_grant_kb] 
			, [min_ideal_grant_kb] 
			, [max_ideal_grant_kb] 
			, isnull([total_reserved_threads] - lag(total_reserved_threads) OVER (order by timestamp), total_reserved_threads) as [total_reserved_threads]  
			, [last_reserved_threads] 
			, [min_reserved_threads] 
			, [max_reserved_threads] 
			, isnull([total_used_threads] - lag(total_used_threads) OVER (order by timestamp), total_used_threads) as [total_used_threads]   
			, [last_used_threads] 
			, [min_used_threads] 
			, [max_used_threads] 
		FROM dbo.query_statistics s
		WHERE sql_handle = @sqlHandle
		and plan_handle = @sqlPlanHandle
		and statement_start_offset = @statement_start_offset
		and query_hash = @query_hash
		and query_plan_hash = @query_plan_hash
		and plan_generation_num = @plan_gen_num 
		and  timestamp between @calc_start_date and @end_date)
		insert into @q_stats_temp
		select * from result
		where end_date <> (select MIN(end_date) from result)

		FETCH NEXT FROM sql_list INTO @sqlHandle, @sqlPlanHandle, @statement_start_offset, @query_hash, @query_plan_hash, @plan_gen_num
	END
	
	CLOSE sql_list
	DEALLOCATE sql_list

RETURN
END
GO



CREATE FUNCTION [dbo].[get_procedure_statistics](@start_date datetime2 = NULL
	, @end_date datetime2 = NULL
	, @sql_handle varbinary(64) = NULL
	, @plan_handle varbinary(64) = NULL)
RETURNS @p_stats_temp TABLE(
	[start_date] [datetime2](7) NULL,
	[end_date] [datetime2](7) NULL,
	[db_name] [NVARCHAR](512) NULL,
	[objectid] [bigint] NULL,
	[schema_name] [nvarchar](256) NULL,
	[object_name] [nvarchar](512) NULL,
	[type] [char](2) NULL,
	[type_desc] [nvarchar](60) NULL,
	[sql_handle] [varbinary](64) NOT NULL,
	[plan_handle] [varbinary](64) NOT NULL,
	[cached_time] [datetime] NULL,
	[last_execution_time] [datetime] NULL,
	[execution_count] [bigint] NOT NULL,
	[total_worker_time] [bigint] NOT NULL,
	[last_worker_time] [bigint] NOT NULL,
	[min_worker_time] [bigint] NOT NULL,
	[max_worker_time] [bigint] NOT NULL,
	[total_physical_reads] [bigint] NOT NULL,
	[last_physical_reads] [bigint] NOT NULL,
	[min_physical_reads] [bigint] NOT NULL,
	[max_physical_reads] [bigint] NOT NULL,
	[total_logical_writes] [bigint] NOT NULL,
	[last_logical_writes] [bigint] NOT NULL,
	[min_logical_writes] [bigint] NOT NULL,
	[max_logical_writes] [bigint] NOT NULL,
	[total_logical_reads] [bigint] NOT NULL,
	[last_logical_reads] [bigint] NOT NULL,
	[min_logical_reads] [bigint] NOT NULL,
	[max_logical_reads] [bigint] NOT NULL,
	[total_elapsed_time] [bigint] NOT NULL,
	[last_elapsed_time] [bigint] NOT NULL,
	[min_elapsed_time] [bigint] NOT NULL,
	[max_elapsed_time] [bigint] NOT NULL,
	INDEX ix_dates clustered(start_date, end_date)
)
AS
BEGIN
	DECLARE @sqlPlanHandle VARBINARY(64)
	DECLARE @sqlHandle VARBINARY(64)
	DECLARE @calc_start_date datetime2
	DECLARE @min_possible_date datetime2
	DECLARE @database_id INT
	DECLARE @object_id INT
	
	select top 1 @min_possible_date = timestamp 
	from dbo.procedure_statistics s
	where timestamp > ( select MIN(timestamp) from dbo.procedure_statistics )
	order by timestamp
	
	IF @start_date is NULL SET @start_date = GETDATE()-1
	IF @start_date < @min_possible_date SET @calc_start_date = @min_possible_date
	ELSE select top 1 @calc_start_date = timestamp 
		from dbo.procedure_statistics s
		where timestamp >= @start_date
		order by timestamp
	
	IF @end_date is NULL SET @end_date = GETDATE()
	
	DECLARE proc_list CURSOR for
	select distinct database_id, [object_id], sql_handle, plan_handle
	from dbo.procedure_statistics
	where timestamp between @calc_start_date and @end_date
	and (@sql_handle is NULL OR sql_handle = @sql_handle)
	and (@plan_handle is NULL OR plan_handle = @plan_handle)
	
	OPEN proc_list
	FETCH NEXT FROM proc_list INTO @database_id, @object_id, @sqlHandle, @sqlPlanHandle
	
	WHILE (@@FETCH_STATUS = 0)
	BEGIN
		with result as (
		select
			isnull(lag(timestamp) OVER (order by timestamp), ( select top 1 timestamp from dbo.procedure_statistics where timestamp < s.timestamp order by timestamp desc)) as start_date
			, timestamp as end_date
			, db_name([database_id]) as [db_name]
			, [object_id] as [objectid]
			, object_schema_name([object_id], [database_id]) as [schema_name]
			, object_name([object_id], [database_id]) as [object_name]
			, [type]
			, [type_desc]
			, [sql_handle]
			, [plan_handle]
			, [cached_time]
			, [last_execution_time]
			, isnull(execution_count - lag(execution_count) OVER (order by timestamp), execution_count) as execution_count
			, isnull([total_worker_time] - lag(total_worker_time) OVER (order by timestamp), total_worker_time) as worker_time
			, [last_worker_time]
			, [min_worker_time]
			, [max_worker_time]
			, isnull([total_physical_reads] - lag([total_physical_reads]) OVER (order by timestamp), total_physical_reads) as [physical_reads]
			, [last_physical_reads]
			, [min_physical_reads]
			, [max_physical_reads]
			, isnull([total_logical_writes] - lag(total_logical_writes) OVER (order by timestamp), total_logical_writes) as [logical_writes]
			, [last_logical_writes]
			, [min_logical_writes] 
			, [max_logical_writes] 
			, isnull([total_logical_reads] - lag(total_logical_reads) OVER (order by timestamp), total_logical_reads) as [logical_reads]
			, [last_logical_reads] 
			, [min_logical_reads] 
			, [max_logical_reads] 
			, isnull([total_elapsed_time]  - lag(total_elapsed_time) OVER (order by timestamp), total_elapsed_time) as [elapsed_time]
			, [last_elapsed_time] 
			, [min_elapsed_time] 
			, [max_elapsed_time] 
		FROM dbo.procedure_statistics s
		WHERE sql_handle = @sqlHandle
		and plan_handle = @sqlPlanHandle
		and [database_id] = @database_id
		and [object_id] = @object_id
		and [timestamp] between @calc_start_date and @end_date)
		insert into @p_stats_temp
		select * from result
		where end_date <> (select MIN(end_date) from result)

		FETCH NEXT FROM proc_list INTO @database_id, @object_id, @sqlHandle, @sqlPlanHandle
	END
	
	CLOSE proc_list
	DEALLOCATE proc_list

RETURN
END
GO
