/*
  Drop and recreate 'market_aggregate' procedure and normal entity 'aggregate' procedure to
  account for new effective_capacity column
*/

-- 'market_aggregate' procedure
DELIMITER //
DROP PROCEDURE IF EXISTS `market_aggregate`//
CREATE DEFINER=CURRENT_USER PROCEDURE `market_aggregate`(IN statspref CHAR(10))
  BEGIN
    DECLARE v_stats_table varchar(32);
    DECLARE v_snapshot_time datetime;
    DECLARE v_topology_context_id bigint(20);
    DECLARE v_topology_id bigint(20);
    DECLARE v_entity_type varchar(80);
    DECLARE v_property_type varchar(36);
    DECLARE v_property_subtype varchar(36);
    DECLARE v_capacity decimal(15,3);
    DECLARE v_avg_value decimal(15,3);
    DECLARE v_min_value decimal(15,3);
    DECLARE v_max_value decimal(15,3);
    DECLARE v_relation integer;
    DECLARE v_aggregated boolean;
    DECLARE v_effective_capacity decimal(15,3);
    DECLARE done int default false;
    DECLARE cur1 CURSOR for select * from mkt_stats_vw;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=TRUE;
    -- dynamic query to prepare view of rows not aggregated
    set v_stats_table=concat(statspref,'_stats_latest');
    DROP VIEW IF EXISTS mkt_stats_vw;
    SET @query = CONCAT('CREATE VIEW mkt_stats_vw as select * from ',statspref,'_stats_latest where aggregated=false');

    PREPARE stmt from @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    -- open cursor
    open cur1;

    read_loop: LOOP
      fetch cur1 into v_snapshot_time,v_topology_context_id,v_topology_id,v_entity_type,v_property_type,v_property_subtype,v_capacity,v_avg_value,v_min_value,v_max_value,v_relation,v_aggregated,v_effective_capacity;
      if done THEN
        LEAVE read_loop;
      end if;


      -- HOURLY MARKET AGGREGATE
      -- Set stats table to process ie.  market_stats_by_hour

      SET @stats_table = CONCAT(statspref,'_stats_by_hour');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_hour table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_topology_context_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;


      -- Build update sql statement
      set @update_hourly_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           effective_capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and topology_context_id<=>?
                                  and entity_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_hourly_sql=CONCAT('insert into ',@stats_table,
                                    ' (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples,effective_capacity)
                                    values (?,?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the market_stats_hourly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_hourly_sql;
        SET @p1=v_avg_value;
        SET @p2=v_capacity;
        SET @p3=v_effective_capacity;
        SET @p4=v_min_value;
        SET @p5=v_max_value;
        SET @p6=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p7=v_topology_context_id;
        SET @p8=v_entity_type;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_hourly_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p2=v_topology_context_id;
        SET @p4=v_entity_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);
        SET @p13=v_effective_capacity;

        EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13;
        DEALLOCATE PREPARE stmt;


      END IF;



      -- DAILY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_day

      SET @stats_table = CONCAT(statspref,'_stats_by_day');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_day table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
      SET @p2=v_topology_context_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;



      set @update_daily_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           effective_capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and topology_context_id<=>?
                                  and entity_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_daily_sql=CONCAT('insert into ',@stats_table,
                                   ' (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples,effective_capacity)
                                   values (?,?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the market_stats_hourly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_daily_sql;
        SET @p1=v_avg_value;
        SET @p2=v_capacity;
        SET @p3=v_effective_capacity;
        SET @p4=v_min_value;
        SET @p5=v_max_value;
        SET @p6=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p7=v_topology_context_id;
        SET @p8=v_entity_type;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11;
        DEALLOCATE PREPARE stmt;

      ELSE
        PREPARE stmt from @insert_daily_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_topology_context_id;
        SET @p4=v_entity_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);
        SET @p13=v_effective_capacity;

        EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13;
        DEALLOCATE PREPARE stmt;


      END IF;




      -- MONTHLY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_month

      SET @stats_table = CONCAT(statspref,'_stats_by_month');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_month table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_topology_context_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;



      set @update_monthly_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           effective_capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and topology_context_id<=>?
                                  and entity_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_monthly_sql=CONCAT('insert into ',@stats_table,
                                     ' (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples,effective_capacity)
                                     values (?,?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the market_stats_monthly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_monthly_sql;
        SET @p1=v_avg_value;
        SET @p2=v_capacity;
        SET @p3=v_effective_capacity;
        SET @p4=v_min_value;
        SET @p5=v_max_value;
        SET @p6=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p7=v_topology_context_id;
        SET @p8=v_entity_type;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_monthly_sql;
        SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_topology_context_id;
        SET @p4=v_entity_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);
        SET @p13=v_effective_capacity;

        EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13;
        DEALLOCATE PREPARE stmt;

      END IF;





      /* Mark _latest row as aggregated */
      set @latest=concat(statspref,'_stats_latest');
      set @latest_sql=CONCAT('update ',@latest,' set aggregated=true where snapshot_time<=>? and topology_context_id<=>? and topology_id<=>? and entity_type<=>? and property_type<=>? and property_subtype<=>?  and relation<=>? ');
      PREPARE stmt from @latest_sql;
      SET @p1=v_snapshot_time;
      SET @p2=v_topology_context_id;
      SET @p3=v_topology_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;

      -- loop until all rows aggregated
    END LOOP;
    close cur1;
    -- delete temporary view
    DROP VIEW mkt_stats_vw;
  END //


-- 'aggregate' procedure
DELIMITER //
DROP PROCEDURE IF EXISTS `aggregate`//
CREATE DEFINER=CURRENT_USER PROCEDURE `aggregate`(IN statspref CHAR(10))
  aggregate_proc:BEGIN
    DECLARE running_aggregations INT;
    DECLARE number_of_unaggregated_rows INT;
    DECLARE number_of_unaggregated_rows_hour INT;
    DECLARE number_of_unaggregated_rows_day INT;

    set @aggregation_id = md5(now());

    set sql_mode='';

    set running_aggregations = CHECKAGGR();
    if running_aggregations > 0 then
      select 'Aggregations already running... exiting rollup function.' as '';
      leave aggregate_proc;
    end if;

    insert into aggregation_status values ('Running', null);

    set @start_of_aggregation=now();

    /* HOURLY AGGREAGATION BEGIN */
    select concat(now(),' INFO:  Starting hourly aggregation ',statspref) as '';
    set @start_of_aggregation_hourly=now();

    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    set @sql=concat('update ',statspref,'_stats_latest a set a.aggregated=2 where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    /* select number_of_unaggregated_rows; */


    if number_of_unaggregated_rows = 0 then
      select 'Nothing to aggregate...' as '';
      delete from aggregation_status;
      leave aggregate_proc;
    end if;


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
      max(a.capacity) as capacity,
      max(a.effective_capacity) as effective_capacity,
      min(a.min_value) as min_value,
      max(a.max_value) as max_value,
      avg(a.avg_value) as avg_value,
      count(*) as samples,
      count(*) as new_samples
      from ',statspref,'_stats_latest a where a.aggregated=2 group by hour_key');

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
           max_value,min_value,avg_value,samples,0,new_samples,hour_key,day_key,month_key from ',statspref,'_hourly_ins_vw b
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
                    statspref,'_stats_by_hour.aggregated=0');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('update ',statspref,'_stats_latest set aggregated=1 where aggregated=2');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation_hourly=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Hourly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows,', Start time: ',@start_of_aggregation_hourly,', End time: ',@end_of_aggregation_hourly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION HOURLY',statspref,number_of_unaggregated_rows,@start_of_aggregation_hourly,@end_of_aggregation_hourly,time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)));
    /* END  HOURLY */


    /* DAILY AGGREGATION BEGIN */

    set @start_of_aggregation_daily=now();

    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    select concat(now(),' INFO:  Starting daily aggregation ',statspref) as '';

    set @sql=concat('update ',statspref,'_stats_by_hour a
set a.aggregated=2
where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows_hour = ROW_COUNT();

    DEALLOCATE PREPARE stmt;
    /* select @sql; */

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
max(a.capacity) as capacity,
max(a.effective_capacity) as effective_capacity,
min(a.min_value) as min_value,
max(a.max_value) as max_value,
avg(a.avg_value) as avg_value,
sum(samples) as samples,
sum(new_samples) as new_samples
from ',statspref,'_stats_by_hour a where a.aggregated=2 group by day_key');


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
   max_value,min_value,avg_value,samples,0,new_samples,day_key,month_key from ',statspref,'_daily_ins_vw b
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
',statspref,'_stats_by_day.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @sql=concat('update ',statspref,'_stats_by_hour set aggregated=1 where aggregated=2');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* select @sql; */
    set @end_of_aggregation_daily=now();

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Daily: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_hour,', Start time: ',@start_of_aggregation_daily,', End time: ',@end_of_aggregation_daily,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION DAILY',statspref,number_of_unaggregated_rows_hour,@start_of_aggregation_daily,@end_of_aggregation_daily,time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)));


    /* END DAILY AGGREGATION */


    /* MONTHLY AGGREGATION BEGIN */
    set @start_of_aggregation_monthly=now();
    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    select concat(now(),' INFO:  Starting monthly aggregation ',statspref) as '';

    set @sql=concat('update ',statspref,'_stats_by_day a
set a.aggregated=2
where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows_day = ROW_COUNT();

    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('create or replace view ',statspref,'_monthly_ins_vw as
select date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
a.uuid,
a.producer_uuid,
a.property_type,
a.property_subtype,
a.relation,
a.commodity_key,
a.month_key,
max(a.capacity) as capacity,
max(a.effective_capacity) as effective_capacity,
min(a.min_value) as min_value,
max(a.max_value) as max_value,
avg(a.avg_value) as avg_value,
sum(samples) as samples,
sum(new_samples) as new_samples
from ',statspref,'_stats_by_day a where a.aggregated=2 group by month_key');

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
   max_value,min_value,avg_value,samples,new_samples,month_key from ',statspref,'_monthly_ins_vw b
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


    set @sql=concat('update ',statspref,'_stats_by_day set aggregated=1 where aggregated=2');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */
    set @end_of_aggregation_monthly=now();

    /* END MONTHLY AGGREGATION */


    set @sql=concat('delete from aggregation_status');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Monthly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation_monthly,', End time: ',@end_of_aggregation_monthly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION MONTHLY',statspref,number_of_unaggregated_rows_day,@start_of_aggregation_monthly,@end_of_aggregation_monthly,time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)));

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Total: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation,', End time: ',@end_of_aggregation,', Total Time: ', time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION TOTAL',statspref,number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,@start_of_aggregation,@end_of_aggregation,time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)));

  END //