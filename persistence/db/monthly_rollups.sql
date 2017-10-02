use vmtdb;

delimiter //

DROP PROCEDURE IF EXISTS monthly_rollups //
CREATE PROCEDURE monthly_rollups()
BEGIN

  # SET THE FIRST DATE
  set @top_of_month := timestamp(concat(date(start_of_month(now())),' 00:00:00')) ;
  set @top_of_month_millis := unix_timestamp(@top_of_month)*1000 ;


  set @last_stats_time_millis := IFNULL((select max(snapshot_time) as t1 
    from pm_stats_by_month where property_type = 'priceIndex'), 0) ;  
  select now() as '', "    Inserting monthly PM stats : " as  '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
  insert into pm_stats_by_month
    select * from pm_summary_stats_by_month
    where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis  ;


  set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
    from vm_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly VM stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
    insert into vm_stats_by_month
      select * from vm_summary_stats_by_month
      where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis  ;


  set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
    from ds_stats_by_month where property_type = 'priceIndex'),0) ;
  select now() as '', "    Inserting monthly DS stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
  insert into ds_stats_by_month
    select * from ds_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


  set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from vdc_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly VDC stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into vdc_stats_by_month 
		select * from vdc_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from app_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly APP stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into app_stats_by_month
		select * from app_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from sw_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly SW stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into sw_stats_by_month
		select * from sw_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from da_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly DA stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into da_stats_by_month
		select * from da_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from ch_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly CH stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into ch_stats_by_month
		select * from ch_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from iom_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly IOM stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into iom_stats_by_month
		select * from iom_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time_millis  := IFNULL((select max(snapshot_time) as t1
		from sc_stats_by_month where property_type = 'priceIndex'),0) ;
	select now() as '', "    Inserting monthly SC stats : " as '', date(date_add(from_unixtime(@last_stats_time_millis/1000), interval 1 day)) as '', " -> " as '',  date(date_add(from_unixtime(@top_of_month_millis/1000), interval -1 day)) as '';
	insert into sc_stats_by_month
		select * from sc_summary_stats_by_month
		where snapshot_time > @last_stats_time_millis and snapshot_time < @top_of_month_millis ;


	set @last_stats_time  := IFNULL((select max(recorded_on) as t1 from cluster_stats_by_month), date('1970-01-01')) ;
	select now() as '', "    Inserting monthly cluster stats : " as '', date(date_add(end_of_month(@last_stats_time), interval 1 day)) as '', " -> " as '',  date(date_add(@top_of_month, interval -1 day)) as '';
 	insert into cluster_stats_by_month 
		select * from cluster_summary_stats_by_month
		where recorded_on > @last_stats_time and recorded_on < @top_of_month;

  select now() as '', "    Finished data aggregation" as '';
END //

delimiter ;

/* 
    -- -- Execute procedures -- --
*/

select now() as '', 'START monthly rollups...' as '';
call monthly_rollups();
select now() as '', 'FINISHED monthly rollups' as '';