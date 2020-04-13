# SQL-Server-Query-Statistics
When you cannot use Query Store but you still would like to collect query statistics in SQL Server

To install, follow the instructions in installer.sql. 

# Usage
select * 
from [dbo].[get_procedure_statistics](<begin time>, <end time>, <SQL handle>, <Plan Handle>)


# Usage examples
Running with default parameters a list of every recorded query or procedure is shown for the last 24 Hours.
select * 
from [dbo].[get_procedure_statistics](NULL, NULL, NULL, NULL)

select * 
from [dbo].[get_query_statistics](NULL, NULL, NULL, NULL)

Filtering for a time period:
select * from dbo.get_query_stats('2018-10-10 17:00:00.1900000'
    ,'2018-10-10 19:00:01.3133333'
    , NULL
    , NULL)
order by start_date desc

Filtering for a SQL handle:
select * from dbo.get_query_stats(NULL, NULL
    , 0x030006000EF21B27FFA3D201C1A9000000000000000000000000000000000000000000000000000000000000
    , NULL
order by start_date desc
