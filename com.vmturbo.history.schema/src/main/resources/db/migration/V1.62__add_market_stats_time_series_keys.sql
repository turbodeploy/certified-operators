-- Code under th POSTGRES_PRIMARY_KEY feature flag requires that a new field, `time_series_key`,
-- appear in the various market_stats tables. While this field will only be accessed under code
-- where the feature flag is enabled, the jOOQ Java model for the history component's schema is
-- built from the "legacy" migrations. So we will add this column in the legacy schema, but it
-- will remain unused in the legacy scenario.
--
-- A more consequential migration (V1.61.1) makes this change and others to market-stats tables in
-- the MARIADB migrations used in non-legacy scenarios. That migration will need to be repeated
-- when we retire the feature flag and leave the legacy scenario behind, since otherwise,
-- customers migrating from legacy at that time will miss that migration.
--
-- We also must redefine the `market_aggregate` stored proc, since its current cursor definition
-- uses a `SELECT *` statement, which picks up the new column, and the rest of the stored proc
-- is not prepared for its appearance. For example, the attempt to fetch from the cursor fails
-- because there is no variable provided to receive the `time_series_key` value. We address this
-- by listing the SELECT columns explicitly in the cursor definition, leaving out `time_series_key`.
-- The rest of the stored proc then works correctly, oblivious to the presence of the new column.

DROP PROCEDURE IF EXISTS _exec;
DELIMITER //
CREATE PROCEDURE _exec(sql_stmt text)
BEGIN
    -- we assume that errors executing any of our options are due to the statement having
    -- previously been executed, so preconditions are not met. This will happen when this
    -- migration is executed a second time after we retire POSTGRES_PRIMARY_DB feature flag.
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
    PREPARE stmt FROM sql_stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

CALL _exec('ALTER TABLE market_stats_latest ADD COLUMN time_series_key char(32) DEFAULT NULL');
CALL _exec('ALTER TABLE market_stats_by_hour ADD COLUMN time_series_key char(32) DEFAULT NULL');
CALL _exec('ALTER TABLE market_stats_by_day ADD COLUMN time_series_key char(32) DEFAULT NULL');
CALL _exec('ALTER TABLE market_stats_by_month ADD COLUMN time_series_key char(32) DEFAULT NULL');

DROP PROCEDURE _exec;

DROP PROCEDURE IF EXISTS market_aggregate;

DELIMITER //
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
    DECLARE cur1 CURSOR for select snapshot_time,topology_context_id,topology_id,entity_type,property_type,property_subtype,capacity,avg_value,min_value,max_value,relation,aggregated,effective_capacity,environment_type from mkt_stats_vw;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=TRUE;

    SET time_zone='+00:00';

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
DELIMITER ;