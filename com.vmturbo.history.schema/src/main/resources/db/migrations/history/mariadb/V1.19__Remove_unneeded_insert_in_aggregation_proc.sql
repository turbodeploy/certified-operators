/*
 * The aggregate stored proc inserts a record into the long-defunct aggregation_status table,
 * which fails in MySQL due to an explicit NULL value passed for the 'created' field. We're
 * deleting the insert statement and removing the table in order to fix this.
 */

DELIMITER //

-- Drop and recreate aggregate procedure to implement a single pass aggregation

-- 'aggregate' procedure
DROP PROCEDURE IF EXISTS `aggregate`//
CREATE DEFINER=CURRENT_USER PROCEDURE `aggregate`(IN statspref CHAR(10))
  aggregate_proc:BEGIN
    DECLARE requires_aggregation INT;
    DECLARE aggregation_meta_exists INT;
    DECLARE number_of_unaggregated_rows INT;
    DECLARE number_of_unaggregated_rows_hour INT;
    DECLARE number_of_unaggregated_rows_day INT;
    DECLARE min_created_time TIMESTAMP;
    DECLARE max_created_time TIMESTAMP;

    set @aggregation_id = md5(now());

    set sql_mode='';

    set @start_of_aggregation=now();

    /* HOURLY AGGREAGATION BEGIN */
    select concat(now(),' INFO:  Starting hourly aggregation ',statspref);
    set @start_of_aggregation_hourly=now();

    /* Find the oldest and newest records in the yyyy_stats_latest table */

    set @sql=concat('select min(snapshot_time) into @min_created_time from ',statspref,'_stats_latest where \'aggregation\'=\'aggregation\'');
    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @sql=concat('select max(snapshot_time) into @max_created_time from ',statspref,'_stats_latest where \'aggregation\'=\'aggregation\'');
    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* The aggregation_meta_data table contains the details of the most recent rollup timestamps processed in the hourly, daily and monthly tables          */
    /* Used as a starting point for future aggregations.                                                                                                    */
    /* Each aggregation cycle is defined by rows of data with between a starting time and an ending time.                                                   */
    /* The next aggregation of the hourly, daily, and monthly tables will be any data inserted with a timestamp greater than the last cycle processed       */

    set @sql=concat('select * from aggregation_meta_data where aggregate_table=\'',statspref,'_stats_latest\'');
    PREPARE stmt from @sql;
    EXECUTE stmt;
    set @aggregation_meta_exists=FOUND_ROWS();
    DEALLOCATE PREPARE stmt;
    select @sql;

    select @aggregation_meta_exists;

    /* If an entry for xxxx_stats_latest does not exist in the aggreggation_meta_data table, create a new entry with default values */
    IF (@aggregation_meta_exists=0) THEN
        SELECT 'NO META DATA FOUND.  CREATING...';

        set @sql=concat('INSERT INTO aggregation_meta_data (aggregate_table, last_aggregated_by_hour, last_aggregated_by_day, last_aggregated_by_month) VALUES (\'',statspref,'_stats_latest\',\'2000-01-01 12:00:00\',\'2000-01-01 12:00:00\',\'2000-01-01 12:00:00\')');
        PREPARE stmt from @sql;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;

    /* Check if there is any new data in the _latest table for aggregation, and exit procedure if no new data exists */
    set @sql=concat('SELECT 1 FROM ',statspref,'_stats_latest where snapshot_time>(SELECT last_aggregated_by_hour FROM aggregation_meta_data WHERE aggregate_table=\'',statspref,'_stats_latest\') limit 1');
    select @sql;
    PREPARE stmt from @sql;
    EXECUTE stmt;
    set @requires_aggregation=FOUND_ROWS();
    DEALLOCATE PREPARE stmt;

    IF (@requires_aggregation=0) THEN
        select 'Exiting.. Nothing to do';
        set @end_of_aggregation=now();
        insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION TOTAL',statspref,0,@start_of_aggregation,@end_of_aggregation,time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)));
        leave aggregate_proc;
    END IF;

    /* HOURLY AGGREGATION */

    /* create view returning only unaggregated rows */
    set @sql=concat('create or replace view ',statspref,'_hourly_ins_vw as
      select date_format(a.snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time,
      a.uuid,
      a.producer_uuid,
      a.property_type,
      a.property_subtype,
      a.relation,
      a.commodity_key,
      a.hour_key,
      a.day_key,
      a.month_key,
      a.capacity,
      a.effective_capacity,
      a.min_value,
      a.max_value,
      a.avg_value,
      1 as samples,
      1 as new_samples
      from ',statspref,'_stats_latest a, aggregation_meta_data b where a.snapshot_time > b.last_aggregated_by_hour and b.aggregate_table=\'',statspref,'_stats_latest\'');
--      max(a.capacity) as capacity,
--      max(a.effective_capacity) as effective_capacity,
--      min(a.min_value) as min_value,
--      max(a.max_value) as max_value,
--      avg(a.avg_value) as avg_value,
--      count(*) as samples,
--      count(*) as new_samples
--      from ',statspref,'_stats_latest a, aggregation_meta_data b where a.snapshot_time > b.last_aggregated_by_hour and b.aggregate_table=\'',statspref,'_stats_latest\'  group by hour_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    /* Aggregate hourly data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */

    set @sql=concat('insert into ',statspref,'_stats_by_hour
         (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,effective_capacity,
         max_value,min_value,avg_value,samples,aggregated,new_samples,hour_key,day_key,month_key)
           select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,effective_capacity,
           max_value,min_value,avg_value,samples,1,new_samples,hour_key,day_key,month_key from ',statspref,'_hourly_ins_vw b
      on duplicate key update ',
                    statspref,'_stats_by_hour.snapshot_time=b.snapshot_time,',
                    statspref,'_stats_by_hour.uuid=b.uuid,',
                    statspref,'_stats_by_hour.producer_uuid=b.producer_uuid,',
                    statspref,'_stats_by_hour.property_type=b.property_type,',
                    statspref,'_stats_by_hour.property_subtype=b.property_subtype,',
                    statspref,'_stats_by_hour.relation=b.relation,',
                    statspref,'_stats_by_hour.commodity_key=b.commodity_key,',
                    statspref,'_stats_by_hour.min_value=if(b.min_value<',statspref,'_stats_by_hour.min_value, b.min_value, ',statspref,'_stats_by_hour.min_value),',
                    statspref,'_stats_by_hour.max_value=if(b.max_value>',statspref,'_stats_by_hour.max_value,b.max_value,',statspref,'_stats_by_hour.max_value),',
                    statspref,'_stats_by_hour.avg_value=((',statspref,'_stats_by_hour.avg_value*',statspref,'_stats_by_hour.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_hour.samples+b.new_samples),',
                    statspref,'_stats_by_hour.samples=',statspref,'_stats_by_hour.samples+b.new_samples,',
                    statspref,'_stats_by_hour.new_samples=b.new_samples,',
                    statspref,'_stats_by_hour.hour_key=b.hour_key,',
                    statspref,'_stats_by_hour.day_key=b.day_key,',
                    statspref,'_stats_by_hour.month_key=b.month_key,',
                    statspref,'_stats_by_hour.aggregated=1');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set number_of_unaggregated_rows = ROW_COUNT();


    /* When the hourly aggregation is complete, UPDATE the aggregation_meta_data table with the most recent created_time processed */
    /* This value is used as the starting point for the next scheduled aggregation cycle */

    set @sql=concat('UPDATE aggregation_meta_data SET last_aggregated_by_hour = @max_created_time where aggregate_table=\'',statspref,'_stats_latest\'') ;
    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    set @end_of_aggregation_hourly=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Hourly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows,', Start time: ',@start_of_aggregation_hourly,', End time: ',@end_of_aggregation_hourly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION HOURLY',statspref,number_of_unaggregated_rows,@start_of_aggregation_hourly,@end_of_aggregation_hourly,time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)));
    /* END  HOURLY */


    /* DAILY AGGREGATION BEGIN */

    set @start_of_aggregation_daily=now();

    set @sql=concat('create or replace view ',statspref,'_daily_ins_vw as
        select date_format(a.snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time,
        a.uuid,
        a.producer_uuid,
        a.property_type,
        a.property_subtype,
        a.relation,
        a.commodity_key,
        a.day_key,
        a.month_key,
        a.capacity,
        a.effective_capacity,
        a.min_value,
        a.max_value,
        a.avg_value,
        1 as samples,
        1 as new_samples
        from ',statspref,'_stats_latest a, aggregation_meta_data b where a.snapshot_time > b.last_aggregated_by_day and b.aggregate_table=\'',statspref,'_stats_latest\'');
--        max(a.capacity) as capacity,
--        max(a.effective_capacity) as effective_capacity,
--        min(a.min_value) as min_value,
--        max(a.max_value) as max_value,
--        avg(a.avg_value) as avg_value,
--        count(*) as samples,
--        count(*) as new_samples
--        from ',statspref,'_stats_latest a, aggregation_meta_data b where a.snapshot_time > b.last_aggregated_by_day and b.aggregate_table=\'',statspref,'_stats_latest\'  group by day_key');




    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    /* Aggregate daily data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */

    set @sql=concat('insert into ',statspref,'_stats_by_day
 (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,effective_capacity,
 max_value,min_value,avg_value,samples,aggregated,new_samples,day_key,month_key)
   select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,effective_capacity,
   max_value,min_value,avg_value,samples,1,new_samples,day_key,month_key from ',statspref,'_daily_ins_vw b
on duplicate key update
',statspref,'_stats_by_day.snapshot_time=b.snapshot_time,
',statspref,'_stats_by_day.uuid=b.uuid,
',statspref,'_stats_by_day.producer_uuid=b.producer_uuid,
',statspref,'_stats_by_day.property_type=b.property_type,
',statspref,'_stats_by_day.property_subtype=b.property_subtype,
',statspref,'_stats_by_day.relation=b.relation,
',statspref,'_stats_by_day.commodity_key=b.commodity_key,
',statspref,'_stats_by_day.min_value=if(b.min_value<',statspref,'_stats_by_day.min_value, b.min_value, ',statspref,'_stats_by_day.min_value),
',statspref,'_stats_by_day.max_value=if(b.max_value>',statspref,'_stats_by_day.max_value,b.max_value,',statspref,'_stats_by_day.max_value),
',statspref,'_stats_by_day.avg_value=((',statspref,'_stats_by_day.avg_value*',statspref,'_stats_by_day.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_day.samples+b.new_samples),
',statspref,'_stats_by_day.samples=',statspref,'_stats_by_day.samples+b.new_samples,
',statspref,'_stats_by_day.new_samples=b.new_samples,
',statspref,'_stats_by_day.day_key=b.day_key,
',statspref,'_stats_by_day.month_key=b.month_key,
',statspref,'_stats_by_day.aggregated=1');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set number_of_unaggregated_rows_hour = ROW_COUNT();


    /* When the daily aggregation is complete, UPDATE the aggregation_meta_data table with the most recent created_time processed */
    /* This value is used as the starting point for the next scheduled aggregation cycle */

    set @sql=concat('UPDATE aggregation_meta_data SET last_aggregated_by_day = @max_created_time where aggregate_table=\'',statspref,'_stats_latest\'') ;
    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    set @end_of_aggregation_daily=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Daily: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_hour,', Start time: ',@start_of_aggregation_daily,', End time: ',@end_of_aggregation_daily,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION DAILY',statspref,number_of_unaggregated_rows_hour,@start_of_aggregation_daily,@end_of_aggregation_daily,time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)));


    /* END DAILY AGGREGATION */


    /* MONTHLY AGGREGATION BEGIN */
    set @start_of_aggregation_monthly=now();

    set @sql=concat('create or replace view ',statspref,'_monthly_ins_vw as
        select date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
        a.uuid,
        a.producer_uuid,
        a.property_type,
        a.property_subtype,
        a.relation,
        a.commodity_key,
        a.month_key,
        a.capacity,
        a.effective_capacity,
        a.min_value,
        a.max_value,
        a.avg_value,
        1 as samples,
        1 as new_samples
        from ',statspref,'_stats_latest a, aggregation_meta_data b where a.snapshot_time > b.last_aggregated_by_month and b.aggregate_table=\'',statspref,'_stats_latest\'');
--        max(a.capacity) as capacity,
--        max(a.effective_capacity) as effective_capacity,
--        min(a.min_value) as min_value,
--        max(a.max_value) as max_value,
--        avg(a.avg_value) as avg_value,
--        count(*) as samples,
--        count(*) as new_samples
--        from ',statspref,'_stats_latest a, aggregation_meta_data b where a.snapshot_time > b.last_aggregated_by_month and b.aggregate_table=\'',statspref,'_stats_latest\'  group by month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    /* Aggregate monthly data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */


    set @sql=concat('insert into ',statspref,'_stats_by_month
        (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,effective_capacity,
        max_value,min_value,avg_value,samples,new_samples,month_key)
    select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,effective_capacity,
        max_value,min_value,avg_value,samples,new_samples,month_key from ',statspref,'_monthly_ins_vw b where \'aggregation\'=\'aggregation\'
        on duplicate key update
        ',statspref,'_stats_by_month.snapshot_time=b.snapshot_time,
        ',statspref,'_stats_by_month.uuid=b.uuid,
        ',statspref,'_stats_by_month.producer_uuid=b.producer_uuid,
        ',statspref,'_stats_by_month.property_type=b.property_type,
        ',statspref,'_stats_by_month.property_subtype=b.property_subtype,
        ',statspref,'_stats_by_month.relation=b.relation,
        ',statspref,'_stats_by_month.commodity_key=b.commodity_key,
        ',statspref,'_stats_by_month.min_value=if(b.min_value<',statspref,'_stats_by_month.min_value, b.min_value, ',statspref,'_stats_by_month.min_value),
        ',statspref,'_stats_by_month.max_value=if(b.max_value>',statspref,'_stats_by_month.max_value,b.max_value,',statspref,'_stats_by_month.max_value),
        ',statspref,'_stats_by_month.avg_value=((',statspref,'_stats_by_month.avg_value*',statspref,'_stats_by_month.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_month.samples+b.new_samples),
        ',statspref,'_stats_by_month.samples=',statspref,'_stats_by_month.samples+b.new_samples,
        ',statspref,'_stats_by_month.new_samples=b.new_samples,
        ',statspref,'_stats_by_month.month_key=b.month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set number_of_unaggregated_rows_day = ROW_COUNT();



    /* When the monthly aggregation is complete, UPDATE the aggregation_meta_data table with the most recent created_time processed */
    /* This value is used as the starting point for the next scheduled aggregation cycle */

    set @sql=concat('UPDATE aggregation_meta_data SET last_aggregated_by_month = @max_created_time where aggregate_table=\'',statspref,'_stats_latest\'') ;
    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    set @end_of_aggregation_monthly=now();
    set @end_of_aggregation_monthly=now();

    /* END MONTHLY AGGREGATION */

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Monthly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation_monthly,', End time: ',@end_of_aggregation_monthly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION MONTHLY',statspref,number_of_unaggregated_rows_day,@start_of_aggregation_monthly,@end_of_aggregation_monthly,time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)));

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Total: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation,', End time: ',@end_of_aggregation,', Total Time: ', time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION TOTAL',statspref,number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,@start_of_aggregation,@end_of_aggregation,time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)));
END//

-- DROP aggregation_status table

DROP TABLE IF EXISTS aggregation_status//

DELIMITER ;
