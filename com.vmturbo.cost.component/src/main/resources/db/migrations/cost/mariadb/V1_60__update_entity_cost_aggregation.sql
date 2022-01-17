DELIMITER //

/* INVOKE OLD AGGREGATION */

DROP PROCEDURE IF EXISTS run_old_entity_cost_aggregation //

CREATE PROCEDURE run_old_entity_cost_aggregation()
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SELECT "Error running old entity cost aggregation" as '';
      END;

      CALL aggregate_entity_cost;

END //

CALL run_old_entity_cost_aggregation() //

DROP PROCEDURE IF EXISTS run_old_entity_cost_aggregation //

/* DROP VIEWS */
DROP VIEW IF EXISTS entity_cost_hourly_snapshots_vw //
DROP VIEW IF EXISTS entity_cost_hourly_ins_vw //
DROP VIEW IF EXISTS entity_cost_daily_snapshots_vw //
DROP VIEW IF EXISTS entity_cost_daily_ins_vw //
DROP VIEW IF EXISTS entity_cost_monthly_snapshots_vw //
DROP VIEW IF EXISTS entity_cost_monthly_ins_vw //


/* CREATE NEW ROLLUP PROCEDURES */

SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci' //

DROP PROCEDURE IF EXISTS aggregate_entity_cost_topology //

CREATE PROCEDURE aggregate_entity_cost_topology(IN source_created_time TIMESTAMP,
                                                IN created_time_normalization_select TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                                IN destination_table TINYTEXT CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci)
  aggregate_entity_cost_topology_proc:BEGIN

    SET @entity_cost_vw_sql='CREATE OR REPLACE VIEW entity_cost_aggregation_vw AS
    SELECT created_time_normalization_select AS normalized_created_time,
    associated_entity_id,
    associated_entity_type,
    cost_type,
    currency,
    account_id,
    region_id,
    availability_zone_id,
    sum(amount) as amount
    FROM entity_cost
    WHERE created_time = \'source_created_time\'
    GROUP BY created_time, associated_entity_id, associated_entity_type,
    cost_type, currency, account_id, region_id, availability_zone_id';

    SET @entity_cost_vw_sql=REPLACE(@entity_cost_vw_sql, 'created_time_normalization_select', created_time_normalization_select);
    SET @entity_cost_vw_sql=REPLACE(@entity_cost_vw_sql, 'source_created_time', source_created_time);

    PREPARE stmt from @entity_cost_vw_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    START TRANSACTION;

    SET @rollup_sql='INSERT INTO destination_table (associated_entity_id,created_time,associated_entity_type,cost_type,currency,amount, samples, account_id, region_id, availability_zone_id)
    SELECT associated_entity_id, normalized_created_time,associated_entity_type,cost_type,currency,amount, 1, account_id, region_id, availability_zone_id
    FROM entity_cost_aggregation_vw b
    ON DUPLICATE KEY UPDATE destination_table.amount =
    ((destination_table.amount * destination_table.samples)+b.amount)/(destination_table.samples+1),
    destination_table.samples=destination_table.samples+1';

    SET @rollup_sql=REPLACE(@rollup_sql, 'destination_table', destination_table);

    PREPARE stmt from @rollup_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    COMMIT;

    DROP VIEW IF EXISTS entity_cost_aggregation_vw;

END //

DROP PROCEDURE IF EXISTS aggregate_entity_cost_table //

CREATE PROCEDURE aggregate_entity_cost_table(IN aggregation_column TINYTEXT,
                                             IN created_time_normalization_select TINYTEXT,
                                             IN destination_table TINYTEXT)
  aggregate_entity_cost_table_proc:BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE topology_time TIMESTAMP;
    DECLARE last_topology_rollup_time TIMESTAMP;
    DECLARE cur1 CURSOR FOR (SELECT creation_time FROM ingested_live_topology WHERE creation_time > last_topology_rollup_time);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET @last_aggregation_time='1970-01-01 12:00:00';
    SET @aggregation_query=CONCAT('SELECT ',aggregation_column,' INTO @last_aggregation_time FROM aggregation_meta_data where aggregate_table = \'entity_cost\'');

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

      CALL aggregate_entity_cost_topology(topology_time, created_time_normalization_select, destination_table);

      SET @update_aggregation_metadata=CONCAT('UPDATE aggregation_meta_data
      SET last_aggregated=CURRENT_TIMESTAMP(), ',aggregation_column,' = \'',topology_time,'\'
      WHERE aggregate_table = \'entity_cost\'');

      PREPARE stmt from @update_aggregation_metadata;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur1;
END //

DROP PROCEDURE IF EXISTS aggregate_entity_cost //

/* Procedure:  aggregate_entity_cost */
/* Rolls up current entity cost data to hourly, daily and monthly tables simultaneous */
CREATE PROCEDURE aggregate_entity_cost()
  aggregate_entity_cost_proc:BEGIN

    /* HOURLY AGGREGATION */
    CALL aggregate_entity_cost_table('last_aggregated_by_hour', 'TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d %H:00:00"))', 'entity_cost_by_hour');

    /* DAILY AGGREGATION */
    CALL aggregate_entity_cost_table('last_aggregated_by_day', 'TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d 00:00:00"))', 'entity_cost_by_day');

    /* MONTHLY AGGREGATION */
    CALL aggregate_entity_cost_table('last_aggregated_by_month', 'TIMESTAMP(DATE_FORMAT(last_day(created_time),"%Y-%m-%d 00:00:00"))', 'entity_cost_by_month');

END //

DROP EVENT IF EXISTS aggregate_entity_cost_event;
//
CREATE
EVENT aggregate_entity_cost_event
  ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
  BEGIN
    DO RELEASE_LOCK('aggregate_entity_cost_lock');
  END;

  IF GET_LOCK('aggregate_entity_cost_lock',0) THEN
    call aggregate_entity_cost;
  ELSE
    SET @msg = "Entity cost aggregation lock is already held. Exiting.";
    SELECT @msg as '';
  END IF;
END //