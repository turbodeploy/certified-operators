-- This migration drops and recreates all stored executable objects appearing in the
-- history database in in conjunction with a change to database provisioning logic
-- so that initial migrations are no longer executed with root privileges. Recreation is
-- needed so that these objects, which may already be in the database with a DEFINER attribute
-- of 'root', will hereafter have a non-root definer and will no longer execute with root
-- privileges.

DELIMITER //

DROP EVENT IF EXISTS aggregate_spend_event //
CREATE EVENT aggregate_spend_event
ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  call aggregateSpend('service');
  call aggregateSpend('vm');
  call aggregateSpend('app');
END
//

DROP EVENT IF EXISTS purge_audit_log_expired_days //
CREATE EVENT purge_audit_log_expired_days
ON SCHEDULE EVERY 1 DAY
DO BEGIN
  DELETE FROM audit_log_entries where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from audit_log_retention_policies where policy_name='retention_days');
END
//

DROP EVENT IF EXISTS purge_expired_days_spend //
CREATE EVENT purge_expired_days_spend
ON SCHEDULE EVERY 1 DAY
DO BEGIN
  call purge_expired_days_spend('service');
  call purge_expired_days_spend('vm');
  call purge_expired_days_spend('app');
END
//

DROP EVENT IF EXISTS purge_expired_hours_spend //
CREATE EVENT purge_expired_hours_spend
ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  call purge_expired_hours_spend('service');
  call purge_expired_hours_spend('vm');
  call purge_expired_hours_spend('app');
END
//

DROP EVENT IF EXISTS purge_expired_months_spend //
CREATE EVENT purge_expired_months_spend
ON SCHEDULE EVERY 1 MONTH
DO BEGIN
  call purge_expired_months_spend('service');
  call purge_expired_months_spend('vm');
  call purge_expired_months_spend('app');
END
//

DROP EVENT IF EXISTS purge_expired_percentile_data //
CREATE EVENT purge_expired_percentile_data
ON SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON COMPLETION PRESERVE ENABLE
DO BEGIN
    CALL `purge_percentile_data`('percentile_retention_days', 'day');
END
//

DROP EVENT IF EXISTS purge_expired_systemload_data //
CREATE EVENT purge_expired_systemload_data
ON SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON COMPLETION NOT PRESERVE ENABLE
DO BEGIN
    CALL `purge_expired_data`('system_load', 'snapshot_time', 'systemload_retention_days', 'day');
END
//

DROP PROCEDURE IF EXISTS aggregateSpend //
CREATE PROCEDURE aggregateSpend(IN spendpref CHAR(10))
  BEGIN
    /* DAILY AGGREGATE */
    set @daily_insert_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=2
where b.snapshot_time is null
and b.uuid is null
and b.provider_uuid is null
and b.property_type is null
and b.property_subtype is null
and b.rate is null
and a.aggregated=0');
    PREPARE stmt from @daily_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @view_delete=concat('drop view if exists ',spendpref,'_daily_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @daily_insert_view=concat('create view ',spendpref,'_daily_ins_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  count(*) as samples,
  count(*) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
  on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is null
  and b.uuid is null
  and b.provider_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=2 group by 1,2,3,4,5');
    PREPARE stmt from @daily_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @perform_daily_inserts_sql=concat('insert into ',spendpref,'_spend_by_day (snapshot_time, uuid, provider_uuid,property_type, property_subtype, rate,samples,new_samples,aggregated)
     select snapshot_time, uuid, provider_uuid, property_type, property_subtype, rate,samples,new_samples,0 from ',spendpref,'_daily_ins_vw');
    PREPARE stmt from @perform_daily_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=4 where aggregated=2');
    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    set @daily_update_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=3,
a.new_samples=1
where a.aggregated=0');
    PREPARE stmt from @daily_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @view_delete=concat('drop view if exists ',spendpref,'_daily_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @daily_update_view=concat('create view ',spendpref,'_daily_upd_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  count(*) as samples,
  count(*) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
  on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is not null
  and b.uuid is not null
  and b.provider_uuid is not null
  and b.property_type is not null
  and b.property_subtype is not null
  and a.aggregated=3 group by 1,2,3,4,5');
    PREPARE stmt from @daily_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @perform_daily_updates_sql=concat('update ',spendpref,'_spend_by_day a, ',spendpref,'_daily_upd_vw b
  set a.snapshot_time=b.snapshot_time,
  a.uuid=b.uuid,
  a.provider_uuid=b.provider_uuid,
  a.property_type=b.property_type,
  a.property_subtype=b.property_subtype,
  a.rate=((a.rate*a.samples)+(b.rate*b.samples))/(a.samples+b.samples),
  a.samples=a.samples+b.samples,
  a.new_samples=a.new_samples+b.new_samples,
  a.aggregated=0
  where a.snapshot_time<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype');
    PREPARE stmt from @perform_daily_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=4 where aggregated=3');
    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /*
	 * These can be useful for debugging intermediate state of the rollup
	 */
    /*
    drop view if exists ',spendpref,'_ins_vw;
    drop view if exists ',spendpref,'_upd_vw;
    */
    /* MONTHLY AGGREGATE */
    set @monthly_insert_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=5
where b.snapshot_time is null
and b.uuid is null
and b.provider_uuid is null
and b.property_type is null
and b.property_subtype is null
and a.aggregated=4');
    PREPARE stmt from @monthly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @view_delete=concat('drop view if exists ',spendpref,'_monthly_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @monthly_insert_view=concat('create view ',spendpref,'_monthly_ins_vw as
  select date_format(last_day(a.snapshot_time),"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  sum(a.samples) as samples,
  sum(a.new_samples) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
  on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is null
  and b.uuid is null
  and b.provider_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=5 group by 1,2,3,4,5');
    PREPARE stmt from @monthly_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @perform_monthly_inserts_sql=concat('insert into ',spendpref,'_spend_by_month (snapshot_time, uuid, provider_uuid,property_type,property_subtype,rate,samples)
     select snapshot_time, uuid, provider_uuid,property_type,property_subtype,rate,samples from ',spendpref,'_monthly_ins_vw');
    PREPARE stmt from @perform_monthly_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=1 where aggregated=5');
    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    set @monthly_update_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=6
where a.aggregated=4');
    PREPARE stmt from @monthly_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @view_delete=concat('drop view if exists ',spendpref,'_monthly_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @monthly_update_view=concat('create view ',spendpref,'_monthly_upd_vw as
  select date_format(last_day(a.snapshot_time),"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  sum(a.samples) as samples,
  sum(a.new_samples) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
  on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is not null
  and b.uuid is not null
  and b.provider_uuid is not null
  and b.property_type is not null
  and b.property_subtype is not null
  and a.aggregated=6 group by 1,2,3,4,5');
    PREPARE stmt from @monthly_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @perform_monthly_updates_sql=concat('update ',spendpref,'_spend_by_month a, ',spendpref,'_monthly_upd_vw b
  set a.snapshot_time=b.snapshot_time,
  a.uuid=b.uuid,
  a.provider_uuid=b.provider_uuid,
  a.property_type=b.property_type,
  a.property_subtype=b.property_subtype,
  a.rate=((a.rate*a.samples)+(b.rate*b.samples))/(a.samples+b.samples),
  a.samples=a.samples+b.samples
  where a.snapshot_time<=>b.snapshot_time and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype');
    PREPARE stmt from @perform_monthly_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=1 where aggregated=6');
    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /*
    drop view if exists ',spendpref,'_ins_vw;
    drop view if exists ',spendpref,'_upd_vw;
    */
  END //

DROP PROCEDURE IF EXISTS cluster_stats_rollup //
CREATE PROCEDURE cluster_stats_rollup(
    -- the table containing the records to be rolled up
    IN source_table CHAR(40),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(40),
    -- the snapshot time of the source records to be processed - must have zero milliseconds value
    IN snapshot_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME,
    -- whether the source table has a "samples" column (constant 1 is used in avg calculations
    -- if not)
    IN source_has_samples TINYINT,
    -- record count reported after upsert operation, as an output parameter
    OUT record_count INT
)
BEGIN
    -- columns to be set in the INSERT phase of the upsert
    SET @insert_columns=CONCAT_WS('#', '',
        'recorded_on', 'internal_name', 'property_type', 'property_subtype', 'value', 'samples',
        '');
    -- values for those columns
    SET @insert_values=CONCAT_WS('#', '',
        -- `recorded_on` in rollup table is always the provided rollup time
        CONCAT("'", rollup_time, "'"),
        'internal_name', 'property_type', 'property_subtype', 'value',
        -- if source table has no `samples` column, use literal 1
        IF(source_has_samples, 'samples', '1'),
        '');
    -- columns to be updated in UPDATE phase, whenever it happens
    SET @update_columns = CONCAT_WS('#', '', 'value', 'samples',
        '');
    -- values for update columns
    SET @update_values = CONCAT_WS('#', '',
        rollup_avg('value', 'samples', rollup_table),
        rollup_incr('samples', rollup_table),
        '');

    CALL generic_rollup(source_table, rollup_table, snapshot_time, rollup_time, 'recorded_on',
        @insert_columns, @insert_values, @update_columns, @update_values, NULL, record_count);
END //

DROP PROCEDURE IF EXISTS entity_stats_rollup //
CREATE PROCEDURE entity_stats_rollup(
    -- the table containing the records to be rolled up
    IN source_table CHAR(40),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(40),
    -- the snapshot time of the source records to be processed - must have zero milliseconds value
    IN snapshot_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME,
    -- exclusive lower bound on hour_key values for this shard (or null for the lowest shard)
    IN hour_key_low CHAR(32),
    -- exclusive upper bound on hour_key values for this shard (or null for highest shard)
    IN hour_key_high CHAR(32),
    -- whether the hour_key column should be copied from source recrods to rollup records
    IN copy_hour_key TINYINT,
    -- whether the day_key column should be copied from source records to rollup records
    IN copy_day_key TINYINT,
    -- whether the month_key column should be copied from source recors to rollup records
    IN copy_month_key TINYINT,
    -- whether the source table has a "samples" column (constant 1 is used in avg calculations
    -- if not)
    IN source_has_samples TINYINT,
    -- record count reported after upsert operation, as an output parameter
    OUT record_count INT
)
BEGIN
    -- Our extra condition... only include records that match our hour_key range
	SET @hour_cond = IF(hour_key_low IS NOT NULL,
	    IF(hour_key_high IS NOT NULL,
	        CONCAT('hour_key BETWEEN ''', hour_key_low, ''' AND ''', hour_key_high, ''''),
	        CONCAT('hour_key > ''', hour_key_low, '''')),
	    CONCAT('hour_key < ''', hour_key_high, ''''));


    -- All the columns to participate in the INSERT phase
	SET @insert_columns=CONCAT_WS('#', '',
	    'snapshot_time', 'uuid', 'producer_uuid', 'property_type', 'property_subtype',
	    'relation', 'commodity_key', 'capacity', 'effective_capacity',
	    'max_value', 'min_value', 'avg_value', 'samples',
	    -- omit unwanted rollup key columns
	    IF(copy_hour_key, 'hour_key', NULL),
	    IF(copy_day_key, 'day_key', NULL),
	    IF(copy_month_key, 'month_key', NULL),
	    '');
	-- Values for all those columns. Most just come from the same columns in source table
	SET @insert_values=CONCAT_WS('#', '',
	    -- the `snapshot_time` is always our rollup time
	    CONCAT("'", rollup_time, "'"),
	    'uuid', 'producer_uuid', 'property_type', 'property_subtype',
	    'relation', 'commodity_key', 'capacity', 'effective_capacity',
	    'max_value', 'min_value', 'avg_value',
        -- `samples` is literal 1 if source doesn't have samples
	    IF(source_has_samples, 'samples', '1'),
	    -- omit unwanted rollup keys
	    IF(copy_hour_key, 'hour_key', NULL),
	    IF(copy_day_key, 'day_key', NULL),
	    IF(copy_month_key, 'month_key', NULL),
	    '');
    -- columns to receive updated values if in UPDATE phase, whenever it happens
    SET @update_columns = CONCAT_WS('#', '',
        'max_value', 'min_value', 'avg_value', 'samples', 'capacity', 'effective_capacity',
        '');
    -- values for the updated columns
    SET @update_values = CONCAT_WS('#', '',
       rollup_max('max_value', rollup_table),
       rollup_min('min_value', rollup_table),
       rollup_avg('avg_value', 'samples', rollup_table),
       rollup_incr('samples', rollup_table),
       rollup_max('capacity', rollup_table),
       rollup_max('effective_capacity', rollup_table),
       '');

    -- perform the upsert, and transmit record count back to caller
    CALL generic_rollup(source_table, rollup_table, snapshot_time, rollup_time, 'snapshot_time',
        @insert_columns, @insert_values, @update_columns, @update_values, @hour_cond, record_count);
END //

DROP PROCEDURE IF EXISTS generic_rollup //
CREATE PROCEDURE generic_rollup(
    -- the table containing the records to be rolled up
    IN source_table CHAR(40),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(40),
    -- the time of the source records to be processed - must have zero milliseconds value
    IN source_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME,
    -- name of column in source table that represents the source record's timestamp
    IN source_time_column CHAR(40),
    -- hash-separated names of columns participating in the INSERT side of the upsert
    IN insert_columns VARCHAR(1000),
    -- hash-separated values for INSERT columns
    IN insert_values VARCHAR(1000),
    -- hash-separated names of columns updated in UPDATE side of upsert
    IN update_columns VARCHAR(1000),
    -- hash-separated values for update columns.
    IN update_values VARCHAR(1000),
    -- WHERE clause condition to be joined (by AND) with the timestamp field condition, or NULL
    IN cond VARCHAR(1000),
    -- output variable where record count will be recorded
    OUT record_count INT
)
BEGIN
	-- source_time param value will always be at 0 msec, but values in source table may not be
	SET @time_cond = CONCAT_WS(' ', source_time_column,
	    'BETWEEN', CONCAT("'", source_time, "'"),
	    'AND',  CONCAT("'", DATE_ADD(source_time, INTERVAL 1 SECOND), "'"));
    SET @updates = rollup_updates(update_columns, update_values, rollup_table);
    set @sql=CONCAT_WS(' ',
        'INSERT INTO', rollup_table, '(', REPLACE(TRIM('#' FROM insert_columns), '#', ', '), ')',
        'SELECT', REPLACE(TRIM('#' FROM insert_values), '#', ', '),
        'FROM', source_table,
        'WHERE', @time_cond, IF(cond IS NOT NULL, CONCAT_WS(' ', 'AND', cond), ''),
        'ON DUPLICATE KEY UPDATE',
        @updates);
    SELECT @sql as 'Rolllup SQL';
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET record_count = ROW_COUNT();
END //

DROP PROCEDURE IF EXISTS market_aggregate //
CREATE DEFINER=CURRENT_USER PROCEDURE `market_aggregate`(IN statspref CHAR(10))
  BEGIN
    DECLARE v_stats_table varchar(32);
    DECLARE v_snapshot_time datetime;
    DECLARE v_topology_context_id bigint(20);
    DECLARE v_topology_id bigint(20);
    DECLARE v_entity_type varchar(80);
    DECLARE v_environment_type tinyint;
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
      fetch cur1 into v_snapshot_time,v_topology_context_id,v_topology_id,v_entity_type,v_property_type,v_property_subtype,v_capacity,v_avg_value,v_min_value,v_max_value,v_relation,v_aggregated,v_effective_capacity,v_environment_type;
      if done THEN
        LEAVE read_loop;
      end if;

      -- HOURLY MARKET AGGREGATE
      -- Set stats table to process ie.  market_stats_by_hour

      SET @stats_table = CONCAT(statspref,'_stats_by_hour');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and environment_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_hour table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_topology_context_id;
      SET @p3=v_entity_type;
      SET @p4=v_environment_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
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
                                  and environment_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_hourly_sql=CONCAT('insert into ',@stats_table,
                                    ' (snapshot_time,topology_context_id,entity_type,environment_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples,effective_capacity)
                                    values (?,?,?,?,?,?,?,?,?,?,?,?,?)');



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
        SET @p9=v_environment_type;
        SET @p10=v_property_type;
        SET @p11=v_property_subtype;
        SET @p12=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_hourly_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p2=v_topology_context_id;
        SET @p3=v_entity_type;
        SET @p4=v_environment_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);
        SET @p13=v_effective_capacity;

        EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13;
        DEALLOCATE PREPARE stmt;


      END IF;


      -- DAILY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_day

      SET @stats_table = CONCAT(statspref,'_stats_by_day');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and environment_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_day table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
      SET @p2=v_topology_context_id;
      SET @p3=v_entity_type;
      SET @p4=v_environment_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
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
                                  and environment_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_daily_sql=CONCAT('insert into ',@stats_table,
                                   ' (snapshot_time,topology_context_id,entity_type,environment_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples,effective_capacity)
                                   values (?,?,?,?,?,?,?,?,?,?,?,?,?)');



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
        SET @p9=v_environment_type;
        SET @p10=v_property_type;
        SET @p11=v_property_subtype;
        SET @p12=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;

      ELSE
        PREPARE stmt from @insert_daily_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_topology_context_id;
        SET @p3=v_entity_type;
        SET @p4=v_environment_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);
        SET @p13=v_effective_capacity;

        EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13;
        DEALLOCATE PREPARE stmt;


      END IF;




      -- MONTHLY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_month

      SET @stats_table = CONCAT(statspref,'_stats_by_month');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and environment_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_month table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_topology_context_id;
      SET @p3=v_entity_type;
      SET @p4=v_environment_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
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
                                  and environment_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_monthly_sql=CONCAT('insert into ',@stats_table,
                                     ' (snapshot_time,topology_context_id,entity_type,environment_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples,effective_capacity)
                                     values (?,?,?,?,?,?,?,?,?,?,?,?,?)');



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
        SET @p9=v_environment_type;
        SET @p10=v_property_type;
        SET @p11=v_property_subtype;
        SET @p12=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p4,@p4,@p5,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_monthly_sql;
        SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_topology_context_id;
        SET @p3=v_entity_type;
        SET @p4=v_environment_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);
        SET @p13=v_effective_capacity;

        EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12,@p13;
        DEALLOCATE PREPARE stmt;

      END IF;





      /* Mark _latest row as aggregated */
      set @latest=concat(statspref,'_stats_latest');
      set @latest_sql=CONCAT('update ',@latest,' set aggregated=true where snapshot_time<=>? and topology_context_id<=>? and topology_id<=>? and entity_type<=>? and environment_type<=>? and property_type<=>? and property_subtype<=>?  and relation<=>? ');
      PREPARE stmt from @latest_sql;
      SET @p1=v_snapshot_time;
      SET @p2=v_topology_context_id;
      SET @p3=v_topology_id;
      SET @p4=v_entity_type;
      SET @p5=v_environment_type;
      SET @p6=v_property_type;
      SET @p7=v_property_subtype;
      SET @p8=v_relation;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7,@p8;
      DEALLOCATE PREPARE stmt;

      -- loop until all rows aggregated
    END LOOP;
    close cur1;
    -- delete temporary view
    DROP VIEW mkt_stats_vw;
  END //

DROP PROCEDURE IF EXISTS purge_expired_cluster_stats //
CREATE PROCEDURE purge_expired_cluster_stats()
BEGIN
    CALL purge_expired_data('cluster_stats_latest', 'recorded_on', 'retention_latest_hours', 'hour');
    CALL purge_expired_data('cluster_stats_by_hour', 'recorded_on', 'retention_hours', 'hour');
    CALL purge_expired_data('cluster_stats_by_day', 'recorded_on', 'retention_days', 'day');
    CALL purge_expired_data('cluster_stats_by_month', 'recorded_on', 'retention_months', 'month');
END //

DROP PROCEDURE IF EXISTS purge_expired_data //
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_data`(IN tableName CHAR(40), IN timestampColumn CHAR(40), IN policyName CHAR(50), IN periodName CHAR(20))
MODIFIES SQL DATA
BEGIN
    SET @currentTime = current_timestamp;
    SET @purge_sql=concat('DELETE FROM ',tableName,'
             where ',timestampColumn,'<(select date_sub("',@currentTime,'", interval retention_period ',periodName,')
             from retention_policies where policy_name="',policyName,'") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
END //

DROP PROCEDURE IF EXISTS purge_expired_days //
CREATE PROCEDURE purge_expired_days(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_day
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period day)
             from retention_policies where policy_name="retention_days") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END //

DROP PROCEDURE IF EXISTS purge_expired_days_spend //
CREATE PROCEDURE purge_expired_days_spend(IN spendpref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',spendpref,'_spend_by_day
             where snapshot_time<(select date_sub(current_timestamp, interval value day)
             from entity_attrs where name="numRetainedDays") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END //

DROP PROCEDURE IF EXISTS purge_expired_hours //
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_hours`(IN statspref CHAR(10),
IN filter_for_purge CHAR(72),
IN ret_name CHAR(50))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_hour
             WHERE snapshot_time<(SELECT date_sub(current_timestamp, interval retention_period hour)
             FROM retention_policies WHERE policy_name=''',ret_name,''') ',filter_for_purge,' limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END //

DROP PROCEDURE IF EXISTS purge_expired_hours_spend //
CREATE PROCEDURE purge_expired_hours_spend(IN spendpref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',spendpref,'_spend_by_hour
             where snapshot_time<(select date_sub(current_timestamp, interval value hour)
             from entity_attrs where name="numRetainedHours") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END //

DROP PROCEDURE IF EXISTS purge_expired_months //
CREATE PROCEDURE purge_expired_months(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_month
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period month)
             from retention_policies where policy_name="retention_months") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END //

DROP PROCEDURE IF EXISTS purge_expired_months_spend //
CREATE PROCEDURE purge_expired_months_spend(IN spendpref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',spendpref,'_spend_by_month
             where snapshot_time<(select date_sub(current_timestamp, interval value month)
             from entity_attrs where name="numRetainedMonths") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END //

DROP PROCEDURE IF EXISTS purge_percentile_data //
CREATE DEFINER = CURRENT_USER PROCEDURE `purge_percentile_data`(
IN policyName CHAR(50),
IN periodName CHAR(20)) MODIFIES SQL DATA
BEGIN
SET @purge_sql = concat('
DELETE
	pb
FROM
	`percentile_blobs` pb
JOIN (
	SELECT
		date_sub(
		FROM_UNIXTIME((SELECT `aggregation_window_length` FROM `percentile_blobs` WHERE `start_timestamp` = 0) / 1000),
		INTERVAL `retention_period` ',periodName,') as oldestRecord
	FROM
		`retention_policies`
	WHERE
		`policy_name` = "',policyName,'" ) ds ON
	FROM_UNIXTIME(`start_timestamp` / 1000) < ds.oldestRecord
WHERE
	`start_timestamp` != 0;');
PREPARE stmt FROM @purge_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END //

DROP PROCEDURE IF EXISTS rotate_partition //
CREATE DEFINER=CURRENT_USER PROCEDURE rotate_partition(
    IN stats_table CHAR(40) CHARACTER SET utf8mb4,  asOfTime DATETIME)
BEGIN
    DECLARE debug INT DEFAULT FALSE;

    DECLARE sql_statement varchar (1000);

    DECLARE done INT DEFAULT FALSE;

    # formatted datetime of the boundary for that we are iterating through
    # partitions name follow this pattern: beforeYYYYMMDDHHmmSS
    DECLARE part_fmt CHAR(20);
    # number of seconds into the past to be covered by active partitions - based on retention policies
    DECLARE num_seconds INT;
    # number of seconds into the future to be covered by active partitions other than the 'future' partition.
    # these partitions will receive new records until they are rotated into the past by a later execution of
    # this stored proc. This should be long enough to avoid putting new data in danger of being dropped, since
    # the future partition is always truncated by this stored proc.
    DECLARE num_seconds_for_future  INT;
    DECLARE retention_type CHAR(20);
    DECLARE idle_timeout_secs INT;

    # used in the continue handler in case we want to print out the error
    DECLARE E INT DEFAULT 0;
    DECLARE M TEXT DEFAULT NULL;

    # cursor for iterating over existing partitions, returning formatted partition boundary times
    # partitions with names that _do not_ begin with 'before' (including start and future) are excluded
    DECLARE cur1 CURSOR FOR (SELECT substring(partition_name, 7) FROM information_schema.partitions
        WHERE table_name = stats_table COLLATE utf8mb4_unicode_ci AND table_schema = database()
            AND substring(PARTITION_NAME, 1, 6) = 'before' ORDER BY partition_name ASC);


    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    # we do not want to exit from this procedure in case we are dropping a partition that is not
    # existing (for whatever reason), specially when dropping and recreating the future partition
    DECLARE CONTINUE HANDLER FOR 1507 SET E='1507', M="Error in list of partitions to %s";
    DECLARE CONTINUE HANDLER FOR 1508 SET E='1508', M="Cannot remove all partitions, use DROP TABLE instead";

    # capture start time for partitioning performance measurement
    set @partitioning_id = md5(now());
    set @start_of_partitioning=now();

    # check which table we need to rotate, and set variables for it
    set retention_type = substring_index(stats_table, '_', -1);
    CASE retention_type
      WHEN 'latest' then
        select retention_period into num_seconds from retention_policies where policy_name='retention_latest_hours';
        set num_seconds = num_seconds*(60*60);
        # set future to 3 hours
        set num_seconds_for_future = 3*(60*60);

      WHEN 'hour' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_hours';
        set num_seconds = num_seconds*(60*60);
        # set future to 8 hours
        set num_seconds_for_future = 8*(60*60);

      WHEN 'day' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_days';
        set num_seconds = num_seconds*(24*60*60);
        # set future to 3 days
        set num_seconds_for_future = 3*(24*60*60);

      WHEN 'month' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_months';
        set num_seconds = num_seconds*(31*24*60*60);
        # set future to 3 months
        set num_seconds_for_future = 3*(31*24*60*60);
    END CASE;



    # calculate what should be the most distant partition boundary in the past, and a formatted version of it
    # we normally do everything based on current time, but for testing we permit an alternative time to be specified
    set @current_utc := IFNULL(asOfTime, utc_timestamp);
    set @oldest_part := date_sub(@current_utc, INTERVAL num_seconds SECOND);
    set @oldest_part_fmt := format_14_digit_datetime(@oldest_part);

    # create future partitions for next X hours/days
    set @future_part := date_add(@current_utc, INTERVAL num_seconds_for_future SECOND);
    set @future_part_fmt := format_14_digit_datetime(@future_part);

    IF debug THEN
        SELECT concat('Retention range at ', utc_timestamp, ': [', @oldest_part_fmt, ' => ', @future_part_fmt, ']') AS '';
    END IF;

    # var to store the maximum partition date existing right now
    SET @max_part_fmt := 0;

    # iterate over the cursor and drop all the old partitions not needed anymore
    OPEN cur1;
    read_loop: LOOP
      FETCH cur1 INTO part_fmt;
      IF done THEN
        LEAVE read_loop;
      END IF;

      IF debug THEN
          SELECT concat('Existing Partition: ', part_fmt, ': ', IF(part_fmt < @oldest_part_fmt, 'drop', 'keep')) AS '';
      END IF;

      # if current partition is older than the last partition, drop it
      IF part_fmt < @oldest_part_fmt THEN
        set @sql_statement = concat('ALTER TABLE ', stats_table, ' DROP PARTITION before', part_fmt);
        PREPARE stmt FROM @sql_statement;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
      END IF;

      # set the current partition as the partition encountered with max date
      set @max_part_fmt := part_fmt;
    END LOOP;
    CLOSE cur1;

    # as a safety measure in order to not pile up data, and fill up the disk (which will result in stopping the whole platform)
    # we want to make sure that the future partition is empty, so that when we will trigger the repartition to create new ones,
    # the future will not contain any data, and hence we will not copy any data over to the new partitions (which will take
    # additional time). this means that if for some reason (we are slow, and cannot keep up with incoming data/aggregation)
    # the future partition contains some data, those data will be dropped. This choice has been done to maintain the platform
    # alive, even if that means to drop data, instead of keep all the data, but then die because of disk full.
    set @sql_statement = concat('ALTER TABLE ', stats_table, ' TRUNCATE PARTITION future;');
    PREPARE stmt from @sql_statement;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    # now we'll start adding partitions as needed, starting with the boundary time of the last existing partition encountered
    # we don't mess with existing partitions that were not dropped above, so in the normal case we'll be creating new partitions
    # with boundaries that are beyond that of the latest existing parittion. However, stats tables newly created during migration
    # start with only a 'start' partition at 0, and a 'future' partition at MAX_VALUE. In that case, we'll be creating a whole
    # set of new partitions.

    # check if our current starting point is earlier than the oldest partition that we want to have, and
    # if so, case use the oldest partition boundary as a starting point instead of any current paritition
    # this normally happens when we had no prior partitions, so @max_part_fmt = 0
    IF @max_part_fmt < @oldest_part_fmt THEN
        SET @max_part_fmt := @oldest_part_fmt;
    END IF;

    # calculate the time period between partitions that will yield 40 partitions from oldest to
    # future boundaries
    set @delta := (to_seconds(@future_part) - to_seconds(@oldest_part)) DIV 40;
    IF debug THEN SELECT concat('Partition delta: ', @delta) AS ''; END IF;

    # begin a sql stmt to reorganize the "future" partition by adding new partitions to it
    set @sql_statement = concat('alter table ', stats_table, ' REORGANIZE PARTITION future into (');

    # add delta to find earliest partition to be created
    set @add_part := date_add(@max_part_fmt, INTERVAL @delta SECOND);
    set @add_part_fmt := format_14_digit_datetime(@add_part);

    # continue adding the delta until we reach the future date
    WHILE @add_part_fmt <= @future_part_fmt DO

      IF debug THEN SELECT concat('Adding partition at boundary ', @add_part_fmt) AS ''; END IF;
      # append another partition
      set @sql_statement = concat(@sql_statement, '\n  partition before', @add_part_fmt, ' VALUES LESS THAN (to_seconds(\'', @add_part, '\')), ');

      # increase the date by another delta
      set @add_part := date_add(@add_part, INTERVAL @delta SECOND);
      set @add_part_fmt := format_14_digit_datetime(@add_part);

    END WHILE;

    # finish the alter partition statement
    set @sql_statement = concat(@sql_statement, '\n  partition future VALUES LESS THAN MAXVALUE);');
    IF debug THEN SELECT concat('Reorganize future: ',  @sql_statement) AS ''; END IF;
    # execute it
    PREPARE stmt from @sql_statement;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    IF debug THEN
        call summarize_stats_partitions(stats_table);
    END IF;

    # capture end time of partitioning.  Log timings to standard out, and appl_performance table
    SET @end_of_partitioning=now();
    SELECT concat(now(),'   INFO: PERFORMANCE: Partitioning ID: ',@partitioning_id, ', Partitioning of: ,',stats_table,',
        Start time: ',@start_of_partitioning,', End time: ',@end_of_partitioning,',
        Total Time: ', time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)),' seconds') AS '';

    INSERT INTO appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds)
     VALUES (@partitioning_id, 'REPARTITION', stats_table, 0, @start_of_partitioning, @end_of_partitioning,
        time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)));

END //

DROP PROCEDURE IF EXISTS summarize_stats_partitions //
CREATE PROCEDURE summarize_stats_partitions(IN stats_table VARCHAR(30) CHARACTER SET utf8mb4)
BEGIN
    # dump a handy table showing the time overall time range, and the now-current partition boundaries
    DROP TABLE IF EXISTS _summary;
    CREATE TEMPORARY TABLE _summary (row INTEGER AUTO_INCREMENT PRIMARY KEY, time DATETIME);
    INSERT INTO _summary SELECT NULL, str_to_date(substring(partition_name, 7), '%Y%m%d%H%i%s')
        FROM information_schema.partitions
        WHERE table_name = stats_table  COLLATE utf8mb4_unicode_ci AND table_schema = database()
        AND substring(partition_name, 1, 6) = 'before';

    SET @previous_time = NULL;
    SELECT time as 'boundary time', delta FROM (
        SELECT time, timediff(time, @previous_time) as delta,
             @previous_time := time as _junk
         FROM _summary ORDER BY row
    ) as x;

    SELECT count(*)-2 AS 'partition count' FROM _summary;

    DROP TABLE _summary;
END //

DROP FUNCTION IF EXISTS format_14_digit_datetime //
CREATE FUNCTION format_14_digit_datetime(boundary DATETIME) RETURNS char(20)
DETERMINISTIC
BEGIN
    RETURN year(boundary)*10000000000 + month(boundary)*100000000 + day(boundary)*1000000
        + hour(boundary)*10000 + minute(boundary)*100 + second(boundary);
END //

DROP FUNCTION IF EXISTS rollup_avg //
CREATE FUNCTION rollup_avg(name VARCHAR(40), samples_name VARCHAR(40), rollup_table VARCHAR(40)) RETURNS varchar(1000)
BEGIN
    RETURN CONCAT(
        '(  (', rollup_table, '.', name, ' * ', rollup_table, '.', samples_name, ') + ',
        '   (VALUES(', name, ') * VALUES(', samples_name, '))) ',
        '/ (', rollup_table, '.', samples_name, ' + VALUES(', samples_name, '))');
END //

DROP FUNCTION IF EXISTS rollup_incr //
CREATE FUNCTION rollup_incr(name VARCHAR(40), rollup_table VARCHAR(40)) RETURNS varchar(1000)
BEGIN
    RETURN CONCAT(rollup_table, '.', name, ' + VALUES(', name, ')');
END //

DROP FUNCTION IF EXISTS rollup_max //
CREATE FUNCTION rollup_max(name VARCHAR(40), rollup_table VARCHAR(40)) RETURNS varchar(1000)
BEGIN
    RETURN CONCAT('IF(', rollup_table, '.', name, '>VALUES(', name, '),',
        rollup_table, '.', name, ',VALUES(', name, '))');
END //

DROP FUNCTION IF EXISTS rollup_min //
CREATE FUNCTION rollup_min(name VARCHAR(40), rollup_table VARCHAR(40)) RETURNS varchar(1000)
BEGIN
    -- CONCAT(rollup_table, '.', name) will yield existing value, while
    -- CONCAT('VALUES(', name, ')' will yield the new incoming value from the source table
    RETURN CONCAT('IF(', rollup_table, '.', name, '<VALUES(', name, '),',
        rollup_table, '.', name, ',VALUES(', name, '))');
END //

DROP FUNCTION IF EXISTS rollup_updates //
CREATE FUNCTION rollup_updates(cols VARCHAR(1000), vals VARCHAR(1000), tbl VARCHAR(40)) RETURNS varchar(1000)
BEGIN
    DECLARE update_string VARCHAR(1000);
    DECLARE col VARCHAR(40);
    DECLARE colspos INT;
    DECLARE val VARCHAR(1000);
    DECLARE valspos INT;
    SET update_string = '';
    SET cols = TRIM(LEADING '#' FROM cols);
    SET vals = TRIM(LEADING '#' FROM vals);
    SET colspos = LOCATE('#', cols);
    -- loop through all the columns
    WHILE colspos > 0 DO
        -- extract the next column name
        SET col = LEFT(cols, colspos - 1);
        -- and its corresponding expression
        SET valspos = LOCATE('#', vals);
        SET val = LEFT(vals, valspos - 1);
        -- compute the "column=value" string for this pair and append it our accumulating list
        SET update_string = CONCAT(update_string, CONCAT_WS(' ', col, '=', val), ', ');
        SET cols = SUBSTRING(cols, colspos + 1);
        SET vals = SUBSTRING(vals, valspos + 1);
        SET colspos = LOCATE('#', cols);
    END WHILE;
    -- drop the initial ", " from the first pair we added
    RETURN TRIM(', ' FROM update_string);
END //

DROP FUNCTION IF EXISTS start_of_day //
CREATE FUNCTION start_of_day(ref_date datetime) RETURNS datetime
DETERMINISTIC
BEGIN
    return date(date_format(ref_date, convert('%Y-%m-%d 00:00:00' using utf8))) ;
  END //

DROP FUNCTION IF EXISTS start_of_hour //
CREATE FUNCTION start_of_hour(ref_date timestamp) RETURNS timestamp
DETERMINISTIC
BEGIN
    return timestamp(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8))) ;
  END //

DROP FUNCTION IF EXISTS start_of_month //
CREATE FUNCTION start_of_month(ref_date datetime) RETURNS datetime
DETERMINISTIC
BEGIN
    return date_sub(ref_date, interval day(ref_date)-1 day) ;
  END //

DELIMITER ;
