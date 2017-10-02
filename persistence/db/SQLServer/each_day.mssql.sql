
/*
  Author: Fred Wild

  This script populates the daily summary tables with values rolled up from the
  hourly stats and counts for the prior day

  This script must be run daily, preferably at the start of the new day, or in
  any case before reports are generated.  This procedure provides the data
  used by reports that show prior day data (the majority of them)

*/

use vmtdb ;

IF OBJECT_ID (N'dbo.populate_by_day_tables', N'P') IS NOT NULL
DROP PROCEDURE dbo.populate_by_day_tables ;
go

CREATE PROCEDURE dbo.populate_by_day_tables AS
BEGIN
    declare @top_of_day datetime ;
    declare @top_of_day_millis bigint ;
    declare @hours_to_retain bigint ;
    declare @days_to_retain bigint ;
    declare @oldest_millis bigint ;
    declare @last_stats_time_millis bigint ;

    set @top_of_day        = convert(date, dbo.now());
    set @top_of_day_millis = dbo.unix_timestamp(@top_of_day)*1000 ;


    set @hours_to_retain = ISNULL((select value from entity_attrs where name = 'numRetainedHours'),72) ;
    set @days_to_retain  = ISNULL((select value from entity_attrs where name = 'numRetainedDays'),60) ;

    set @oldest_millis = @top_of_day_millis - 1000*60*60*@hours_to_retain ;
    delete from pm_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from vm_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from ds_stats_by_hour where snapshot_time < @oldest_millis ;

    set @oldest_millis = @top_of_day_millis - 1000*60*60*24*@days_to_retain ;
    delete from pm_stats_by_day where snapshot_time < @oldest_millis ;
    delete from vm_stats_by_day where snapshot_time < @oldest_millis ;
    delete from ds_stats_by_day where snapshot_time < @oldest_millis ;

    delete from notifications where clear_time < @oldest_millis ;

    set @days_to_retain = ISNULL((select value from entity_attrs where name = 'auditLogRetentionDays'),365) ;
    set @oldest_millis = @top_of_day_millis - 1000*60*60*24*@days_to_retain ;
    delete from audit_log_entries where snapshot_time < @oldest_millis ;



    set @last_stats_time_millis  = ISNULL((select max(snapshot_time) as t1
                from pm_stats_by_day where property_type = 'priceIndex'),0) ;

    insert into pm_stats_by_day
      select * from pm_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;


    set @last_stats_time_millis  = ISNULL((select max(snapshot_time) as t1
                from vm_stats_by_day where property_type = 'priceIndex'),0) ;

    insert into vm_stats_by_day
      select * from vm_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;


    set @last_stats_time_millis  = ISNULL((select max(snapshot_time) as t1
                from ds_stats_by_day where property_type = 'priceIndex'),0) ;

    insert into ds_stats_by_day
      select * from ds_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;


    IF day(convert(date, (dbo.now()))) = 1
    BEGIN

        INSERT INTO cluster_members SELECT * FROM cluster_membership;



        set @last_stats_time_millis  = ISNULL((select max(snapshot_time) as t1
                    from pm_stats_by_month where property_type = 'priceIndex'),0) ;

        insert into pm_stats_by_month
          select * from pm_summary_stats_by_month
          where snapshot_time > @last_stats_time_millis ;



        set @last_stats_time_millis  = ISNULL((select max(snapshot_time) as t1
                    from vm_stats_by_month where property_type = 'priceIndex'),0) ;

        insert into vm_stats_by_month
          select * from vm_summary_stats_by_month
          where snapshot_time > @last_stats_time_millis ;



        set @last_stats_time_millis  = ISNULL((select max(snapshot_time) as t1
                    from ds_stats_by_month where property_type = 'priceIndex'),0) ;

        insert into ds_stats_by_month
          select * from ds_summary_stats_by_month
          where snapshot_time > @last_stats_time_millis ;

    END

END
go

/*
  Author: Fred Wild

  This procedure populates the capcity_projection table used for extrapolating
  capacity usage trends.

*/


IF OBJECT_ID (N'dbo.populate_ext', N'P') IS NOT NULL
DROP PROCEDURE dbo.populate_ext ;
go

CREATE PROCEDURE dbo.populate_ext AS
BEGIN
  IF OBJECT_ID (N'dbo.capacity_projection', N'U') IS NOT NULL
  DROP TABLE dbo.capacity_projection

  create table dbo.capacity_projection (
    group_uuid          varchar(80),
    group_name          varchar(250),
    recorded_on         datetime,
    property_type       varchar(80),
    capacity            decimal(18),
    used_capacity       decimal(18),
    pct_used            decimal(5,2),
    available_capacity  decimal(18),
    pct_available       decimal(5,2)
  )

  insert into dbo.capacity_projection
  select
    max(group_uuid),
    group_name,
    recorded_on,
    property_type,
    round(sum(capacity),0) as capacity,
    round(sum(used_capacity),0) as used_capacity,
    round(round(sum(used_capacity),0)/round(sum(capacity),0)*100,2) as pct_used,
    round(sum(available_capacity),0) as available_capacity,
    round(round(sum(available_capacity),0)/round(sum(capacity),0)*100,2) as pct_available
  from
    pm_capacity_by_day_per_pm_group
  where
    ISNULL(group_name,'') <> ''
    and property_type <> 'Ballooning'
  group by
    group_name, recorded_on, property_type
  having
    sum(capacity) > 0
    and recorded_on between convert(date, dbo.date_sub(dbo.now(), 61)) and convert(date, dbo.date_sub(dbo.now(), 1))
  order by
    group_name, recorded_on, property_type ;

  DECLARE @grp VARCHAR(250);
  DECLARE @grp_name VARCHAR(250);

  DECLARE @d1       datetime ;
  DECLARE @d2       datetime ;
  DECLARE @days     INT;
  DECLARE @midpoint datetime ;

  DECLARE @avg_m1	decimal(15,3) ;
  DECLARE @avg_m2	decimal(15,3) ;
  DECLARE @mapping	decimal(15,3) ;

  set @d1       = (select min(recorded_on) from capacity_projection) ;
  set @d2       = (select max(recorded_on) from capacity_projection) ;
  set @days     = (select datediff(d,@d1,@d2)) ;
  set @midpoint = (select dbo.date_sub(@d2, @days/2));


  IF @days >= 15
  BEGIN

      IF OBJECT_ID (N'dbo.capacity_projection_ext', N'U') IS NOT NULL
      DROP TABLE dbo.capacity_projection_ext ;

      select * into capacity_projection_ext from capacity_projection ;

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.capacity_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if(@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;

          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'CPU'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'Mem'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (select (@avg_m2 - @avg_m1)*2.0) ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'Swapping'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update capacity_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into capacity_projection select * from capacity_projection_ext ;

      /* add the extrapolated data in again for 2 * the interval */

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.capacity_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'CPU'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'Mem'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (select (@avg_m2 - @avg_m1)*2.0) ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'Swapping'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update capacity_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into capacity_projection select * from capacity_projection_ext ;



      /* add the extrapolated data in again for 3 * the interval */

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.capacity_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'CPU'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'CPU'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'Mem'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'Mem'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (select (@avg_m2 - @avg_m1)*2.0) ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'Swapping'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'Swapping'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'IOThroughput'
                      and group_name = @grp_name;





          set @avg_m1 = (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;
          if (@mapping <= 0.0 and @avg_m2 < 12.0) SET @mapping = 0.0;


          update capacity_projection_ext set pct_used = pct_used + @mapping
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set used_capacity = pct_used/100.0 * capacity
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set pct_available = (100.0-pct_used)
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          update capacity_projection_ext set available_capacity = pct_available/100.0 * capacity
                      where property_type = 'NetThroughput'
                      and group_name = @grp_name;

          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1 ;

      update capacity_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into capacity_projection select * from capacity_projection_ext ;


      /* get rid of bad datapoints to the sub-zero side */

      update capacity_projection set capacity = 0.0           where capacity < 0.0 ;
      update capacity_projection set used_capacity = 0.0      where used_capacity < 0.0 ;
      update capacity_projection set pct_used = 0.0           where pct_used < 0.0 ;
      update capacity_projection set available_capacity = 0.0 where available_capacity < 0.0 ;
      update capacity_projection set pct_available = 0.0      where pct_available < 0.0 ;

  END

END
go


/*
  Author: Fred Wild

  This procedure populates the storage_projection table used for extrapolating
  storage usage trends.

*/


IF OBJECT_ID (N'dbo.populate_storage_ext', N'P') IS NOT NULL
DROP PROCEDURE dbo.populate_storage_ext ;
go

CREATE PROCEDURE dbo.populate_storage_ext AS
BEGIN
  DECLARE @grp VARCHAR(250);
  DECLARE @grp_name VARCHAR(250);

  DECLARE @d1       datetime ;
  DECLARE @d2       datetime ;
  DECLARE @days     INT;
  DECLARE @midpoint datetime ;

  DECLARE @avg_m1	decimal(15,3) ;
  DECLARE @avg_m2	decimal(15,3) ;
  DECLARE @mapping	decimal(15,3) ;

  IF OBJECT_ID (N'dbo.storage_projection', N'U') IS NOT NULL
  DROP TABLE dbo.storage_projection ;

  select * into dbo.storage_projection
    from vm_storage_used_by_day_per_vm_group
    where recorded_on between convert(date, dbo.date_sub(dbo.now(), 61)) and convert(date, dbo.date_sub(dbo.now(), 1)) ;

  set @d1       = (select min(recorded_on) from storage_projection) ;
  set @d2       = (select max(recorded_on) from storage_projection) ;
  set @days     = (select datediff(d,@d1,@d2)) ;
  set @midpoint = (select dbo.date_sub(@d2, @days/2));


  IF @days >= 15
  BEGIN

      IF OBJECT_ID (N'dbo.storage_projection_ext', N'U') IS NOT NULL
      DROP TABLE dbo.storage_projection_ext ;
      select * into dbo.storage_projection_ext from dbo.storage_projection ;

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.storage_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;

          update storage_projection_ext set storage_used = storage_used + @mapping
                      where group_name = @grp_name;


          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update storage_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into storage_projection select * from storage_projection_ext ;


      /* add the extrapolated data in again for 2 * the  */

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.storage_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;

          update storage_projection_ext set storage_used = storage_used + @mapping
                      where group_name = @grp_name;


          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update storage_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into storage_projection select * from storage_projection_ext ;



      /* add the extrapolated data in again for 3 * the  */

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.storage_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;

          update storage_projection_ext set storage_used = storage_used + @mapping
                      where group_name = @grp_name;


          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update storage_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into storage_projection select * from storage_projection_ext ;


      /* get rid of bad datapoints to the sub-zero side */

      update storage_projection set storage_used = 0.0 where storage_used < 0.0 ;

  END

END
go


/*
  Author: Fred Wild

  This procedure populates the storage_projection table used for extrapolating
  storage usage trends.

*/

IF OBJECT_ID (N'dbo.populate_vm_count_ext', N'P') IS NOT NULL
DROP PROCEDURE dbo.populate_vm_count_ext ;
go

CREATE PROCEDURE dbo.populate_vm_count_ext AS
BEGIN
  DECLARE @grp VARCHAR(250);
  DECLARE @grp_NAME VARCHAR(250);

  DECLARE @d1       datetime ;
  DECLARE @d2       datetime ;
  DECLARE @days     INT;
  DECLARE @midpoint datetime ;

  DECLARE @avg_m1	decimal(15,3) ;
  DECLARE @avg_m2	decimal(15,3) ;
  DECLARE @mapping	decimal(15,3) ;

  IF OBJECT_ID (N'dbo.vm_count_projection', N'U') IS NOT NULL
  DROP TABLE dbo.vm_count_projection ;

  select * INTO vm_count_projection from pm_vm_count_by_day_per_pm_group
    where recorded_on between convert(date, dbo.date_sub(dbo.now(), 61)) and convert(date, dbo.date_sub(dbo.now(), 1)) ;


  set @d1       = (select min(recorded_on) from vm_count_projection) ;
  set @d2       = (select max(recorded_on) from vm_count_projection) ;
  set @days     = (select datediff(d,@d1,@d2)) ;
  set @midpoint = (select dbo.date_sub(@d2, @days/2));


  IF @days >= 15
  BEGIN

      IF OBJECT_ID (N'dbo.vm_count_projection_ext', N'U') IS NOT NULL
      DROP TABLE dbo.vm_count_projection_ext ;

      select * into dbo.vm_count_projection_ext from dbo.vm_count_projection ;

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.vm_count_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;

          update vm_count_projection_ext set vm_count = vm_count + @mapping
                      where group_name = @grp_name;


          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update vm_count_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into vm_count_projection select * from vm_count_projection_ext ;


      /* add the extrapolated data in again for 2 * the */

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.vm_count_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;

          update vm_count_projection_ext set vm_count = vm_count + @mapping
                      where group_name = @grp_name;


          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1;

      update vm_count_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into vm_count_projection select * from vm_count_projection_ext ;



      /* add the extrapolated data in again for 3 * the */

      DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM dbo.vm_count_projection;
      OPEN cursor_1;
      FETCH NEXT FROM cursor_1 INTO @grp;
      WHILE @@FETCH_STATUS = 0
      BEGIN
          set @grp_name = @grp ;


          set @avg_m1 = (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 = (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping = (@avg_m2 - @avg_m1)*2.0 ;

          update vm_count_projection_ext set vm_count = vm_count + @mapping
                      where group_name = @grp_name;


          FETCH NEXT FROM cursor_1 INTO @grp;
      END

      CLOSE cursor_1;
      DEALLOCATE cursor_1 ;

      update vm_count_projection_ext set recorded_on = dateadd(d, @days+1, recorded_on) ;

      insert into vm_count_projection select * from vm_count_projection_ext ;


      /* get rid of bad datapoints to the sub-zero side */

      update vm_count_projection set vm_count = 0 where vm_count < 0 ;

  END

END
go


/*
    Various utility functions for reports
*/


--

IF OBJECT_ID(N'dbo.ftn_vm_count_for_month', N'FN') IS NOT NULL DROP FUNCTION dbo.ftn_vm_count_for_month;
go

CREATE FUNCTION dbo.ftn_vm_count_for_month (@month_day_1 date) RETURNS int
BEGIN

    declare @ms_day_1 bigint ;
    declare @ms_day_n bigint ;
    declare @count int ;

    set @ms_day_1 = dbo.start_of_month_ms(@month_day_1) ;
    set @ms_day_n = dbo.end_of_month_ms(@month_day_1) ;

    set @count = (select count(uuid) as n_vms
                    from (select distinct uuid from vm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END
go


--

IF OBJECT_ID(N'dbo.ftn_pm_count_for_month', N'FN') IS NOT NULL DROP FUNCTION dbo.ftn_pm_count_for_month;
go

CREATE FUNCTION dbo.ftn_pm_count_for_month (@month_day_1 date) RETURNS int
BEGIN

    declare @ms_day_1 bigint ;
    declare @ms_day_n bigint ;
    declare @count int ;

    set @ms_day_1 = dbo.start_of_month_ms(@month_day_1) ;
    set @ms_day_n = dbo.end_of_month_ms(@month_day_1) ;

    set @count = (select count(uuid) as n_pms
                    from (select distinct uuid from pm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END
go


--

IF OBJECT_ID(N'dbo.cluster_nm1_factor', N'FN') IS NOT NULL DROP FUNCTION dbo.cluster_nm1_factor;
go

CREATE FUNCTION dbo.cluster_nm1_factor(@arg_group_name varchar(255)) RETURNS float
BEGIN
    declare @n_hosts int ;
    declare @factor float ;

    set @n_hosts = (select count(*) from pm_group_members where group_name = @arg_group_name) ;

    IF @n_hosts > 0
      set @factor = (@n_hosts-1.0) / @n_hosts ;
    ELSE
      set @factor = 0 ;

    return @factor ;
END
go


--

IF OBJECT_ID(N'dbo.cluster_nm2_factor', N'FN') IS NOT NULL DROP FUNCTION dbo.cluster_nm2_factor;
go

CREATE FUNCTION dbo.cluster_nm2_factor(@arg_group_name varchar(255)) RETURNS float
BEGIN
    declare @n_hosts int ;
    declare @factor float ;

    set @n_hosts = (select count(*) from pm_group_members where group_name = @arg_group_name) ;

    IF @n_hosts > 1
      set @factor = (@n_hosts-2.0) / @n_hosts ;
    ELSE
      set @factor = 0 ;

    return @factor ;
END
go


/*
    -- -- Execute procedures that prepare report data -- --
*/

select '  running populate_by_day_tables...' as progress ;
exec dbo.populate_by_day_tables;
go

select '  running populate_ext...' as progress ;
exec dbo.populate_ext;
go

select '  running populate_storage_ext...' as progress ;
exec dbo.populate_storage_ext;
go

select '  running populate_vm_count_ext...'  as progress ;
exec dbo.populate_vm_count_ext;
go

/*
    -- --
*/
