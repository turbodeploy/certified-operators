DELIMITER //

/* INVOKE OLD AGGREGATION */

DROP PROCEDURE IF EXISTS run_old_ri_aggregation //

CREATE PROCEDURE run_old_ri_aggregation()
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SELECT "Error running old RI aggregation" as '';
      END;

      CALL aggregate_reserved_instance_coverage;
      CALL aggregate_reserved_instance_utilization;

END //

CALL run_old_ri_aggregation() //

DROP PROCEDURE IF EXISTS run_old_ri_aggregation //

/* DROP VIEWS */
DROP VIEW IF EXISTS reserved_instance_coverage_hourly_ins_vw //
DROP VIEW IF EXISTS reserved_instance_coverage_daily_ins_vw //
DROP VIEW IF EXISTS reserved_instance_coverage_monthly_ins_vw //

DROP VIEW IF EXISTS reserved_instance_utilization_hourly_ins_vw //
DROP VIEW IF EXISTS reserved_instance_utilization_daily_ins_vw //
DROP VIEW IF EXISTS reserved_instance_utilization_monthly_ins_vw //

/* DROP UNUSED COLUMNS */

CREATE PROCEDURE drop_day_month_key_columns()
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SELECT "Error dropping day and month keys from RI aggregation tables" as '';
      END;

      ALTER TABLE reserved_instance_coverage_by_hour DROP COLUMN day_key;
      ALTER TABLE reserved_instance_coverage_by_hour DROP COLUMN month_key;
      ALTER TABLE reserved_instance_coverage_by_day DROP COLUMN month_key;

      ALTER TABLE reserved_instance_utilization_by_hour DROP COLUMN day_key;
      ALTER TABLE reserved_instance_utilization_by_hour DROP COLUMN month_key;
      ALTER TABLE reserved_instance_utilization_by_day DROP COLUMN month_key;

END //

CALL drop_day_month_key_columns() //

DROP PROCEDURE IF EXISTS drop_day_month_key_columns //

/* CREATE NEW ROLLUP PROCEDURES */

SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci' //

DROP PROCEDURE IF EXISTS aggregate_ric_topology //

CREATE PROCEDURE aggregate_ric_topology(IN rollup_key TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                        IN source_timestamp TIMESTAMP,
                                        IN snapshot_normalization_select TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                        IN destination_table TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci)
  aggregate_ric_topology_proc:BEGIN

    /* Create a view containing the reserved_instance_coverage rows which have not been aggregated to the hourly table */
    SET @sql='CREATE OR REPLACE VIEW reserved_instance_coverage_aggregate_vw AS
    SELECT rollup_key,
    snapshot_normalization_select as normalized_snapshot_time,
    snapshot_time,
    entity_id,
    region_id,
    availability_zone_id,
    business_account_id,
    avg(total_coupons) as total_coupons,
    avg(used_coupons) as used_coupons,
    count(*) as samples
    FROM reserved_instance_coverage_latest
    WHERE snapshot_time = \'source_timestamp\' GROUP BY rollup_key';

    SET @sql=REPLACE(@sql, 'rollup_key', rollup_key);
    SET @sql=REPLACE(@sql, 'snapshot_normalization_select', snapshot_normalization_select);
    SET @sql=REPLACE(@sql, 'source_timestamp', source_timestamp);

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    START TRANSACTION;

    set @sql='INSERT INTO destination_table (snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,samples, rollup_key)
    SELECT normalized_snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons, samples, rollup_key
    FROM reserved_instance_coverage_aggregate_vw b
    ON DUPLICATE KEY UPDATE destination_table.total_coupons =
    ((destination_table.total_coupons * destination_table.samples)+(b.total_coupons*b.samples))/(destination_table.samples+b.samples),
    destination_table.used_coupons =
    ((destination_table.used_coupons * destination_table.samples)+(b.used_coupons*b.samples))/(destination_table.samples+b.samples),
    destination_table.samples=destination_table.samples+b.samples';

    SET @sql=REPLACE(@sql, 'destination_table', destination_table);
    SET @sql=REPLACE(@sql, 'rollup_key', rollup_key);

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    COMMIT;

    DROP VIEW IF EXISTS reserved_instance_coverage_aggregate_vw;

END //

DROP PROCEDURE IF EXISTS aggregate_ric_table //

CREATE PROCEDURE aggregate_ric_table(IN aggregation_column TINYTEXT,
                                     IN rollup_key TINYTEXT,
                                     IN snapshot_normalization_select TINYTEXT,
                                     IN destination_table TINYTEXT)
  aggregate_ric_table_proc:BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE topology_time TIMESTAMP;
    DECLARE last_topology_rollup_time TIMESTAMP;
    DECLARE cur1 CURSOR FOR (SELECT creation_time FROM ingested_live_topology WHERE creation_time > last_topology_rollup_time);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET @last_aggregation_time='1970-01-01 12:00:00';
    SET @aggregation_query=CONCAT('SELECT ',aggregation_column,' INTO @last_aggregation_time FROM aggregation_meta_data where aggregate_table = \'reserved_instance_coverage_latest\'');

    PREPARE stmt from @aggregation_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET last_topology_rollup_time=@last_aggregation_time;
    if (last_topology_rollup_time='0000-00-00 00:00:00' OR last_topology_rollup_time IS NULL) THEN
      set last_topology_rollup_time = '1970-01-01 12:00:00';
    end if;

    OPEN cur1;
    read_loop: LOOP
      FETCH cur1 INTO topology_time;
      IF done THEN
        LEAVE read_loop;
      END IF;

      CALL aggregate_ric_topology(rollup_key, topology_time, snapshot_normalization_select, destination_table);

      SET @update_aggregation_metadata=CONCAT('UPDATE aggregation_meta_data
      SET last_aggregated=CURRENT_TIMESTAMP(), ',aggregation_column,' = \'',topology_time,'\'
      WHERE aggregate_table = \'reserved_instance_coverage_latest\'');

      PREPARE stmt from @update_aggregation_metadata;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur1;
END //

DROP PROCEDURE IF EXISTS aggregate_reserved_instance_coverage //

/* Procedure:  aggregate_reserved_instance_coverage */
/* Rolls up current RI coverage to hourly, daily and monthly tables simultaneous */
CREATE PROCEDURE aggregate_reserved_instance_coverage()
  aggregate_ric_proc:BEGIN

    /* HOURLY AGGREGATION */
    CALL aggregate_ric_table('last_aggregated_by_hour', 'hour_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00"))', 'reserved_instance_coverage_by_hour');

    /* DAILY AGGREGATION */
    CALL aggregate_ric_table('last_aggregated_by_day', 'day_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00"))', 'reserved_instance_coverage_by_day');

    /* MONTHLY AGGREGATION */
    CALL aggregate_ric_table('last_aggregated_by_month', 'month_key', 'TIMESTAMP(DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d 00:00:00"))', 'reserved_instance_coverage_by_month');

END //

DROP PROCEDURE IF EXISTS aggregate_riu_topology //

CREATE PROCEDURE aggregate_riu_topology(IN rollup_key TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                        IN source_timestamp TIMESTAMP,
                                        IN snapshot_normalization_select TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                        IN destination_table TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci)
  aggregate_riu_topology_proc:BEGIN

    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    START TRANSACTION;

    /* Create a view containing the reserved_instance_coverage rows which have not been aggregated to the hourly table */
    SET @sql='CREATE OR REPLACE VIEW reserved_instance_utilization_aggregate_vw AS
    SELECT rollup_key,
    snapshot_normalization_select as normalized_snapshot_time,
    snapshot_time,
    id,
    region_id,
    availability_zone_id,
    business_account_id,
    avg(total_coupons) as total_coupons,
    avg(used_coupons) as used_coupons,
    count(*) as samples
    FROM reserved_instance_utilization_latest
    WHERE snapshot_time = \'source_timestamp\' GROUP BY rollup_key';

    SET @sql=REPLACE(@sql, 'rollup_key', rollup_key);
    SET @sql=REPLACE(@sql, 'snapshot_normalization_select', snapshot_normalization_select);
    SET @sql=REPLACE(@sql, 'source_timestamp', source_timestamp);

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @sql='INSERT INTO destination_table (snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,samples, rollup_key)
    SELECT normalized_snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons, samples, rollup_key
    FROM reserved_instance_utilization_aggregate_vw b
    ON DUPLICATE KEY UPDATE destination_table.total_coupons =
    ((destination_table.total_coupons * destination_table.samples)+(b.total_coupons*b.samples))/(destination_table.samples+b.samples),
    destination_table.used_coupons =
    ((destination_table.used_coupons * destination_table.samples)+(b.used_coupons*b.samples))/(destination_table.samples+b.samples),
    destination_table.samples=destination_table.samples+b.samples';

    SET @sql=REPLACE(@sql, 'destination_table', destination_table);
    SET @sql=REPLACE(@sql, 'rollup_key', rollup_key);

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    DROP VIEW IF EXISTS reserved_instance_utilization_aggregate_vw;

    COMMIT;

END //

DROP PROCEDURE IF EXISTS aggregate_riu_table //

CREATE PROCEDURE aggregate_riu_table(IN aggregation_column TINYTEXT,
                                     IN rollup_key TINYTEXT,
                                     IN snapshot_normalization_select TINYTEXT,
                                     IN destination_table TINYTEXT)
  aggregate_riu_table_proc:BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE topology_time TIMESTAMP;
    DECLARE last_topology_rollup_time TIMESTAMP;
    DECLARE cur1 CURSOR FOR (SELECT creation_time FROM ingested_live_topology WHERE creation_time > last_topology_rollup_time);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET @last_aggregation_time='1970-01-01 12:00:00';
    SET @aggregation_query=CONCAT('SELECT ',aggregation_column,' INTO @last_aggregation_time FROM aggregation_meta_data where aggregate_table = \'reserved_instance_utilization_latest\'');

    PREPARE stmt from @aggregation_query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET last_topology_rollup_time=@last_aggregation_time;
    if (last_topology_rollup_time='0000-00-00 00:00:00' OR last_topology_rollup_time IS NULL) THEN
      set last_topology_rollup_time = '1970-01-01 12:00:00';
    end if;

    OPEN cur1;
    read_loop: LOOP
      FETCH cur1 INTO topology_time;
      IF done THEN
        LEAVE read_loop;
      END IF;

      CALL aggregate_riu_topology(rollup_key, topology_time, snapshot_normalization_select, destination_table);

      SET @update_aggregation_metadata=CONCAT('UPDATE aggregation_meta_data SET ',aggregation_column,' = \'',topology_time,'\' where aggregate_table = \'reserved_instance_utilization_latest\'');

      PREPARE stmt from @update_aggregation_metadata;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur1;
END //

DROP PROCEDURE IF EXISTS aggregate_reserved_instance_utilization //

/* Procedure:  aggregate_reserved_instance_utilization */
/* Rolls up current RI utilization to hourly, daily and monthly tables simultaneous */
CREATE PROCEDURE aggregate_reserved_instance_utilization()
  aggregate_riu_proc:BEGIN

    /* HOURLY AGGREGATION */
    CALL aggregate_riu_table('last_aggregated_by_hour', 'hour_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00"))', 'reserved_instance_utilization_by_hour');

    /* DAILY AGGREGATION */
    CALL aggregate_riu_table('last_aggregated_by_day', 'day_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00"))', 'reserved_instance_utilization_by_day');

    /* MONTHLY AGGREGATION */
    CALL aggregate_riu_table('last_aggregated_by_month', 'month_key', 'TIMESTAMP(DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d 00:00:00"))', 'reserved_instance_utilization_by_month');

END //

DROP EVENT IF EXISTS aggregate_ri;
//
CREATE
EVENT aggregate_ri
  ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    DO RELEASE_LOCK('aggregate_ri_lock');
  END;

  IF GET_LOCK('aggregate_ri_lock',0) THEN
    call aggregate_reserved_instance_coverage;
    call aggregate_reserved_instance_utilization;
  ELSE
    SET @msg = "RI aggregation lock is already held. Exiting.";
    SELECT @msg as '';
  END IF;
END //
