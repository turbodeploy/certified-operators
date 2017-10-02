
/*
  Author: Fred Wild

  This script populates the daily summary tables with values rolled up from the
  hourly stats and counts for the prior day

  This script must be run daily, preferably at the start of the new day, or in 
  any case before reports are generated.  This procedure provides the data
  used by reports that show prior day data (the majority of them)

*/

use vmtdb ;

delimiter //

DROP PROCEDURE IF EXISTS populate_by_day_tables //
CREATE PROCEDURE populate_by_day_tables()
BEGIN

    set @top_of_day             := timestamp(concat(curdate(),' 00:00:00')) ;
    set @top_of_day_millis      := unix_timestamp(@top_of_day)*1000 ;


    set @hours_to_retain = IFNULL((select value from entity_attrs where name = 'numRetainedHours' order by id DESC limit 1),72) ;
    set @days_to_retain  = IFNULL((select value from entity_attrs where name = 'numRetainedDays'  order by id DESC limit 1),60) ;

    
    /**
     * Delete data older then the Defined retention period 
     */
    
    set @oldest_millis := @top_of_day_millis - 1000*60*60*@hours_to_retain ;
    select "    Deleting from hourly tables, entries older than: " as '  Aggregations progress', @oldest_millis as millis ;
    delete from pm_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from vm_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from ds_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from vdc_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from app_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from sw_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from da_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from sc_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from iom_stats_by_hour where snapshot_time < @oldest_millis ;
    delete from ch_stats_by_hour where snapshot_time < @oldest_millis ;



    set @oldest_millis := @top_of_day_millis - 1000*60*60*24*@days_to_retain ;
    select "    Deleting from daily tables, entries older than: " as '  Aggregations progress', @oldest_millis as millis ;
    delete from pm_stats_by_day where snapshot_time < @oldest_millis ;
    delete from vm_stats_by_day where snapshot_time < @oldest_millis ;
    delete from ds_stats_by_day where snapshot_time < @oldest_millis ;
    delete from vdc_stats_by_day where snapshot_time < @oldest_millis ;
    delete from app_stats_by_day where snapshot_time < @oldest_millis ;
    delete from sw_stats_by_day where snapshot_time < @oldest_millis ;
    delete from da_stats_by_day where snapshot_time < @oldest_millis ;
    delete from sc_stats_by_day where snapshot_time < @oldest_millis ;
    delete from iom_stats_by_day where snapshot_time < @oldest_millis ;
    delete from ch_stats_by_day where snapshot_time < @oldest_millis ;
    delete from cluster_stats_by_day where date(recorded_on) < date(date_sub(@top_of_day, interval @days_to_retain day)) ;
	
    delete from notifications where clear_time < @oldest_millis ;

    set @days_to_retain = IFNULL((select value from entity_attrs where name = 'auditLogRetentionDays'  order by id DESC limit 1),365) ;
    set @oldest_millis := @top_of_day_millis - 1000*60*60*24*@days_to_retain ;
    select "    Deleting audit log entries older than: " as '  Aggregations progress', @oldest_millis as millis ;
    delete from audit_log_entries where snapshot_time < @oldest_millis ;
 
	set @days_to_retain = IFNULL((select value from entity_attrs where name = 'numRetainedDays' order by id DESC limit 1),60) ;
    set @oldest_millis := @top_of_day_millis - 1000*60*60*24*@days_to_retain ;
    set @oldest_date := date(from_unixtime(@oldest_millis/1000));
    select "    Deleting cluster members entries older than: " as '  Aggregations progress', @oldest_date as oldest_date_clster_tables ;
    delete from cluster_members where recorded_on < @oldest_date and recorded_on!=end_of_month(recorded_on);
    
    
    /**
     * Insert new data to the daily tables
     */
    
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
                from pm_stats_by_day where property_type = 'priceIndex'),0) ;
                
	select "    Inserting daily PM stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into pm_stats_by_day
      select * from pm_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;


    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
                from vm_stats_by_day where property_type = 'priceIndex'),0) ;
	
	select "    Inserting daily VM stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into vm_stats_by_day
      select * from vm_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;


    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
                from ds_stats_by_day where property_type = 'priceIndex'),0) ;
                
	select "    Inserting daily DS stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into ds_stats_by_day
      select * from ds_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

      
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from vdc_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily VDC stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into vdc_stats_by_day
    	select * from vdc_summary_stats_by_day
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
      
    
	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from app_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily APP stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into app_stats_by_day
    	select * from app_summary_stats_by_day
    	where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
      
    
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from sw_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily SW stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into sw_stats_by_day
    	select * from sw_summary_stats_by_day
    	where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
      
    
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from da_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily DA stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into da_stats_by_day
    	select * from da_summary_stats_by_day
    	where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

    
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from sc_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily SC stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into sc_stats_by_day
    	select * from sc_summary_stats_by_day
    	where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
    	
    
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from iom_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily IOM stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into iom_stats_by_day
    	select * from iom_summary_stats_by_day
    	where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
    	
	
    set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1 
    	from ch_stats_by_day where property_type = 'priceIndex'),0) ;
    	
	select "    Inserting daily CH stats between: " as '  Aggregations progress', @last_stats_time_millis as from_millis, @top_of_day_millis as to_millis ;
    insert into ch_stats_by_day
    	select * from ch_summary_stats_by_day
    	where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
    	
    	
	select "    Inserting cluster membership for the previous day.." as '  Aggregations progress';
    insert into cluster_members select * from cluster_membership;
	
   	select "    Inserting by-cluster stats.." as '  Aggregations progress';
    call populate_AllClusters_PreviousDayAggStats();
    
    /**
     * Insert data to the monthly tables
     */
    
    IF day(now()) = 1 THEN

        set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
                    from pm_stats_by_month where property_type = 'priceIndex'),0) ;

		select "    Inserting monthly PM stats beginning: " as  '  Aggregations progress', @last_stats_time_millis as from_millis ;
        insert into pm_stats_by_month
          select * from pm_summary_stats_by_month
          where snapshot_time > @last_stats_time_millis ;



        set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
                    from vm_stats_by_month where property_type = 'priceIndex'),0) ;

		select "    Inserting monthly VM stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
        insert into vm_stats_by_month
          select * from vm_summary_stats_by_month
          where snapshot_time > @last_stats_time_millis ;



        set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
                    from ds_stats_by_month where property_type = 'priceIndex'),0) ;

		select "    Inserting monthly DS stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into ds_stats_by_month
		  select * from ds_summary_stats_by_month
		  where snapshot_time > @last_stats_time_millis ;
        
		  
        set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from vdc_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly VDC stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into vdc_stats_by_month 
			select * from vdc_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
		
		set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from app_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly APP stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into app_stats_by_month
			select * from app_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;
		
			
		
		set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from sw_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly SW stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into sw_stats_by_month
			select * from sw_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

			
			
		set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from da_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly DA stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into da_stats_by_month
			select * from da_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

			
			
		set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from ch_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly CH stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into ch_stats_by_month
			select * from ch_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

			
			
		set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from iom_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly IOM stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into iom_stats_by_month
			select * from iom_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

			
			
		set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
			from sc_stats_by_month where property_type = 'priceIndex'),0) ;
			
		select "    Inserting monthly SC stats beginning: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		insert into sc_stats_by_month
			select * from sc_summary_stats_by_month
			where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_day_millis ;

			
			
		set @last_stats_time_millis  := IFNULL((select max(recorded_on) as t1 from cluster_stats_by_month),date('1970-01-01')) ;
		
 		select "    Inserting monthly cluster stats since: " as '  Aggregations progress', @last_stats_time_millis as from_millis ;
		   	insert into cluster_stats_by_month 
			select * from cluster_summary_stats_by_month
			where recorded_on > @last_stats_time_millis ;
    
    END IF;
    
    select " Finished data aggregation" as progress;

END //

delimiter ;



/*
  Author: Fred Wild

  This procedure populates the capcity_projection table used for extrapolating
  capacity usage trends. 

*/


delimiter //

DROP PROCEDURE IF EXISTS populate_ext //
CREATE PROCEDURE populate_ext()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE grp VARCHAR(250);
  DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM vmtdb.capacity_projection;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  drop table if exists capacity_projection ;
  create table capacity_projection (
    group_uuid          varchar(80),
    group_name          varchar(250),
    recorded_on         date,
    property_type       varchar(80),
    capacity            decimal(18),
    used_capacity       decimal(18),
    pct_used            decimal(5,2),
    available_capacity  decimal(18),
    pct_available       decimal(5,2)
  ) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


  insert into capacity_projection
  select
    group_uuid,
    group_name,
    recorded_on,
    property_type,
    round(sum(capacity)) as capacity,
    round(sum(used_capacity)) as used_capacity,
    round(round(sum(used_capacity))/round(sum(capacity))*100,2) as pct_used,
    round(sum(available_capacity)) as available_capacity,
    round(round(sum(available_capacity))/round(sum(capacity))*100,2) as pct_available
  from
    pm_capacity_by_day_per_pm_group
  where
    ifnull(group_name,'') <> ''
    and property_type <> 'Ballooning'
  group by
    group_name, recorded_on, property_type
  having
    capacity > 0
    and recorded_on between date(date_sub(now(),interval 61 day)) and date(date_sub(now(),interval 1 day))
  order by
    group_name, recorded_on, property_type
  ;


  set @d1       := (select min(recorded_on) from capacity_projection) ;
  set @d2       := (select max(recorded_on) from capacity_projection) ;
  set @days     := (select datediff(@d2,@d1)) ;
  set @midpoint := (select date_sub(@d2, interval @days/2 day));


  IF @days >= 15 THEN

      drop table if exists capacity_projection_ext ;
      create table capacity_projection_ext select * from capacity_projection ;

      OPEN cursor_1;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (select (@avg_m2 - @avg_m1)*2.0) ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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

        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update capacity_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into capacity_projection select * from capacity_projection_ext ;


      /* add the extrapolated data in again for 2 * the interval */

      OPEN cursor_1;

      set done := false ;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (select (@avg_m2 - @avg_m1)*2.0) ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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

        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update capacity_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into capacity_projection select * from capacity_projection_ext ;



      /* add the extrapolated data in again for 3 * the interval */

      OPEN cursor_1;

      set done := false ;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'CPU'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Mem'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'Swapping'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (select (@avg_m2 - @avg_m1)*2.0) ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'IOThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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





          set @avg_m1 := (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(pct_used) from capacity_projection
                            where property_type = 'NetThroughput'
                            and group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;
          set @mapping := if(@mapping > 0.0,@mapping,if(@avg_m2 < 12.0,0.0,@mapping));


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

        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update capacity_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into capacity_projection select * from capacity_projection_ext ;


      /* get rid of bad datapoints to the sub-zero side */

      update capacity_projection set capacity = 0.0           where capacity < 0.0 ;
      update capacity_projection set used_capacity = 0.0      where used_capacity < 0.0 ;
      update capacity_projection set pct_used = 0.0           where pct_used < 0.0 ;
      update capacity_projection set available_capacity = 0.0 where available_capacity < 0.0 ;
      update capacity_projection set pct_available = 0.0      where pct_available < 0.0 ;

  END IF;

END //

delimiter ;


/*
  Author: Fred Wild

  This procedure populates the storage_projection table used for extrapolating
  storage usage trends.

*/


delimiter //

DROP PROCEDURE IF EXISTS populate_storage_ext //
CREATE PROCEDURE populate_storage_ext()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE grp VARCHAR(250);
  DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM vmtdb.storage_projection;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  drop table if exists storage_projection ;
  create table storage_projection
    select * from vm_storage_used_by_day_per_vm_group
    where recorded_on between date(date_sub(now(),interval 61 day)) and date(date_sub(now(),interval 1 day)) ;


  set @d1       := (select min(recorded_on) from storage_projection) ;
  set @d2       := (select max(recorded_on) from storage_projection) ;
  set @days     := (select datediff(@d2,@d1)) ;
  set @midpoint := (select date_sub(@d2, interval @days/2 day));


  IF @days >= 15 THEN

      drop table if exists storage_projection_ext ;
      create table storage_projection_ext select * from storage_projection ;

      OPEN cursor_1;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;

          update storage_projection_ext set storage_used = storage_used + @mapping
                      where group_name = @grp_name;


        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update storage_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into storage_projection select * from storage_projection_ext ;


      /* add the extrapolated data in again for 2 * the interval */

      OPEN cursor_1;

      set done := false ;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;

          update storage_projection_ext set storage_used = storage_used + @mapping
                      where group_name = @grp_name;


        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update storage_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into storage_projection select * from storage_projection_ext ;



      /* add the extrapolated data in again for 3 * the interval */

      OPEN cursor_1;

      set done := false ;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(storage_used) from storage_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;

          update storage_projection_ext set storage_used = storage_used + @mapping
                      where group_name = @grp_name;


        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update storage_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into storage_projection select * from storage_projection_ext ;


      /* get rid of bad datapoints to the sub-zero side */

      update storage_projection set storage_used = 0.0 where storage_used < 0.0 ;

  END IF;

END //

delimiter ;


/*
  Author: Fred Wild

  This procedure populates the storage_projection table used for extrapolating
  storage usage trends.

*/


delimiter //

DROP PROCEDURE IF EXISTS populate_vm_count_ext //
CREATE PROCEDURE populate_vm_count_ext()
BEGIN
  DECLARE done INT DEFAULT 0;
  DECLARE grp VARCHAR(250);
  DECLARE cursor_1 CURSOR FOR SELECT DISTINCT group_name FROM vmtdb.vm_count_projection;
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

  drop table if exists vm_count_projection ;
  create table vm_count_projection
    select * from pm_vm_count_by_day_per_pm_group
    where recorded_on between date(date_sub(now(),interval 61 day)) and date(date_sub(now(),interval 1 day)) ;


  set @d1       := (select min(recorded_on) from vm_count_projection) ;
  set @d2       := (select max(recorded_on) from vm_count_projection) ;
  set @days     := (select datediff(@d2,@d1)) ;
  set @midpoint := (select date_sub(@d2, interval @days/2 day));


  IF @days >= 15 THEN

      drop table if exists vm_count_projection_ext ;
      create table vm_count_projection_ext select * from vm_count_projection ;

      OPEN cursor_1;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;

          update vm_count_projection_ext set vm_count = vm_count + @mapping
                      where group_name = @grp_name;


        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update vm_count_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into vm_count_projection select * from vm_count_projection_ext ;


      /* add the extrapolated data in again for 2 * the interval */

      OPEN cursor_1;

      set done := false ;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;

          update vm_count_projection_ext set vm_count = vm_count + @mapping
                      where group_name = @grp_name;


        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update vm_count_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into vm_count_projection select * from vm_count_projection_ext ;



      /* add the extrapolated data in again for 3 * the interval */

      OPEN cursor_1;

      set done := false ;

      REPEAT
        FETCH cursor_1 INTO grp;
        IF NOT done THEN
          set @grp_name := grp ;


          set @avg_m1 := (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on < @midpoint) ;

          set @avg_m2 := (select avg(vm_count) from vm_count_projection
                            where group_name = @grp_name
                            and recorded_on >= @midpoint
                            and recorded_on <= @d2) ;

          set @mapping := (@avg_m2 - @avg_m1)*2.0 ;

          update vm_count_projection_ext set vm_count = vm_count + @mapping
                      where group_name = @grp_name;


        END IF;
      UNTIL done END REPEAT;

      CLOSE cursor_1;

      update vm_count_projection_ext set recorded_on = date_add(recorded_on, interval @days+1 day) ;

      insert into vm_count_projection select * from vm_count_projection_ext ;


      /* get rid of bad datapoints to the sub-zero side */

      update vm_count_projection set vm_count = 0 where vm_count < 0 ;

  END IF;

END //

delimiter ;


/*
    Various utility functions for reports
*/

delimiter //

DROP FUNCTION IF EXISTS ftn_vm_count_for_month //
CREATE FUNCTION ftn_vm_count_for_month (month_day_1 date) RETURNS int DETERMINISTIC
BEGIN

    set @ms_day_1 := start_of_month_ms(month_day_1) ;
    set @ms_day_n := end_of_month_ms(month_day_1) ;

    set @count := (select count(uuid) as n_vms
                    from (select distinct uuid from vm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END //

DROP FUNCTION IF EXISTS ftn_pm_count_for_month //
CREATE FUNCTION ftn_pm_count_for_month (month_day_1 date) RETURNS int DETERMINISTIC
BEGIN

    set @ms_day_1 := start_of_month_ms(month_day_1) ;
    set @ms_day_n := end_of_month_ms(month_day_1) ;

    set @count := (select count(uuid) as n_pms
                    from (select distinct uuid from pm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END //


DROP FUNCTION IF EXISTS cluster_nm1_factor //
CREATE FUNCTION cluster_nm1_factor(arg_group_name varchar(255)) RETURNS float DETERMINISTIC
BEGIN
    set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;

    IF @n_hosts > 0 THEN
      set @factor := (@n_hosts-1.0) / @n_hosts ;
    ELSE
      set @factor := 0 ;
    END IF ;

    return @factor ;
END //

DROP FUNCTION IF EXISTS cluster_nm2_factor //
CREATE FUNCTION cluster_nm2_factor(arg_group_name varchar(255)) RETURNS float DETERMINISTIC
BEGIN
    set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;

    IF @n_hosts > 1 THEN
      set @factor := (@n_hosts-2.0) / @n_hosts ;
    ELSE
      set @factor := 0 ;
    END IF ;

    return @factor ;
END //

delimiter ;



/* 
    -- -- Execute procedures that prepare report data -- --
*/

select '  running populate_by_day_tables()...' as progress ;
call populate_by_day_tables();

select '  running populate_ext()...' as progress ;
call populate_ext();

select '  running populate_storage_ext()...' as progress ;
call populate_storage_ext();

select '  running populate_vm_count_ext()...'  as progress ;
call populate_vm_count_ext();

/* 
    -- --
*/

