-- This migration drops and recreates all stored executable objects appearing in the
-- cost database in in conjunction with a change to database provisioning logic
-- so that initial migrations are no longer executed with root privileges. Recreation is
-- needed so that these objects, which may already be in the database with a DEFINER attribute
-- of 'root', will hereafter have a non-root definer and will no longer execute with root
-- privileges.

DELIMITER //

DROP EVENT IF EXISTS aggregate_entity_cost_event //
CREATE EVENT aggregate_entity_cost_event
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
END
//

DROP EVENT IF EXISTS aggregate_expenses_evt //
CREATE EVENT aggregate_expenses_evt
ON SCHEDULE EVERY 1 DAY STARTS ADDTIME(UTC_DATE, '01:00:00')
DO BEGIN
  CALL aggregate_account_expenses;
END
//

DROP EVENT IF EXISTS aggregate_ri //
CREATE EVENT aggregate_ri
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
END
//

DROP EVENT IF EXISTS purge_expired_account_expenses_data //
CREATE EVENT purge_expired_account_expenses_data
ON SCHEDULE EVERY 1 DAY STARTS ADDTIME(UTC_DATE, '01:00:00')
DO BEGIN
    CALL purge_expired_data('account_expenses', 'expense_date', 'retention_days', 'day');
    CALL purge_expired_data('account_expenses_by_month', 'expense_date', 'retention_months', 'month');
END
//

DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_MIG_1_45 //
CREATE PROCEDURE ADD_INDEX_IF_NOT_EXISTS_MIG_1_45(
        IN tableName VARCHAR(255),   -- name of table to add index to
        IN columnName VARCHAR(255),  -- name of column to add index on
        IN indexName VARCHAR(255)  -- name of index to add
)
BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SET @msg = (SELECT CONCAT("Index ", indexName, " is already in place in table ", tableName, "."));
          SELECT @msg as '';
      END;

      SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " ADD INDEX ", indexName, "(", columnName ,");"));
      PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;
END //

DROP PROCEDURE IF EXISTS aggregate_account_expenses //
CREATE PROCEDURE aggregate_account_expenses()
BEGIN
    DECLARE aggregation_date DATE;

    DECLARE EXIT HANDLER FOR sqlexception
    BEGIN
       ROLLBACK;
       RESIGNAL;
    END;

    SET @aggregation_date = UTC_DATE();

    /* Aggregation is done under a transaction to allow keeping the db in a consistent state in case
       of a failure in the aggregation process */
    START TRANSACTION;
    /* The monthly aggregation table stores the average expense on a service in a certain account
       during the month. We convert the expense date of each aggregated record to midnight on
       the last day of the expense date month to be able to group all the records of each month together */
    SET @sql=concat('CREATE OR REPLACE VIEW account_expenses_monthly_ins_vw AS
    SELECT
        associated_account_id,
        last_day(expense_date) as expense_date,
        associated_entity_id,
        entity_type,
        currency,
        avg(amount) as amount,
        count(*) as samples
    FROM account_expenses
    WHERE expense_date < \'', DATE_ADD(@aggregation_date, INTERVAL -1 DAY) ,'\' AND aggregated = 0
    GROUP BY last_day(expense_date), associated_account_id, associated_entity_id');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    INSERT INTO account_expenses_by_month(associated_account_id, expense_date, associated_entity_id, entity_type, currency, amount, samples)
    SELECT associated_account_id,
           expense_date,
           associated_entity_id,
           entity_type,
           currency,
           amount,
           samples
    FROM account_expenses_monthly_ins_vw a
    ON DUPLICATE KEY UPDATE
        account_expenses_by_month.amount =
        (account_expenses_by_month.amount * account_expenses_by_month.samples + a.amount * a.samples)/(account_expenses_by_month.samples + a.samples),
        account_expenses_by_month.samples = account_expenses_by_month.samples + a.samples;

    SET @sql=concat('UPDATE account_expenses SET aggregated = 1 WHERE expense_date < ''', DATE_ADD(@aggregation_date, INTERVAL -1 DAY),''' AND aggregated = 0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    COMMIT;
END //

DROP PROCEDURE IF EXISTS aggregate_entity_cost //
CREATE PROCEDURE aggregate_entity_cost()
aggregate_entity_cost_proc:BEGIN

    /* HOURLY AGGREGATION */
    CALL aggregate_entity_cost_table('last_aggregated_by_hour', 'TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d %H:00:00"))', 'entity_cost_by_hour');

    /* DAILY AGGREGATION */
    CALL aggregate_entity_cost_table('last_aggregated_by_day', 'TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d 00:00:00"))', 'entity_cost_by_day');

    /* MONTHLY AGGREGATION */
    CALL aggregate_entity_cost_table('last_aggregated_by_month', 'TIMESTAMP(DATE_FORMAT(last_day(created_time),"%Y-%m-%d 00:00:00"))', 'entity_cost_by_month');

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

DROP PROCEDURE IF EXISTS aggregate_reserved_instance_coverage //
CREATE PROCEDURE aggregate_reserved_instance_coverage()
aggregate_ric_proc:BEGIN

    /* HOURLY AGGREGATION */
    CALL aggregate_ric_table('last_aggregated_by_hour', 'hour_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00"))', 'reserved_instance_coverage_by_hour');

    /* DAILY AGGREGATION */
    CALL aggregate_ric_table('last_aggregated_by_day', 'day_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00"))', 'reserved_instance_coverage_by_day');

    /* MONTHLY AGGREGATION */
    CALL aggregate_ric_table('last_aggregated_by_month', 'month_key', 'TIMESTAMP(DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d 00:00:00"))', 'reserved_instance_coverage_by_month');

END //

DROP PROCEDURE IF EXISTS aggregate_reserved_instance_utilization //
CREATE PROCEDURE aggregate_reserved_instance_utilization()
aggregate_riu_proc:BEGIN

    /* HOURLY AGGREGATION */
    CALL aggregate_riu_table('last_aggregated_by_hour', 'hour_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00"))', 'reserved_instance_utilization_by_hour');

    /* DAILY AGGREGATION */
    CALL aggregate_riu_table('last_aggregated_by_day', 'day_key', 'TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00"))', 'reserved_instance_utilization_by_day');

    /* MONTHLY AGGREGATION */
    CALL aggregate_riu_table('last_aggregated_by_month', 'month_key', 'TIMESTAMP(DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d 00:00:00"))', 'reserved_instance_utilization_by_month');

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

DROP PROCEDURE IF EXISTS aggregate_riu_table //
CREATE PROCEDURE aggregate_riu_table(IN aggregation_column TINYTEXT,
                                     IN rollup_key TINYTEXT,
                                     IN snapshot_normalization_select TINYTEXT,
                                     IN destination_table TINYTEXT
)
aggregate_riu_table_proc:BEGIN

    DECLARE done INT DEFAULT FALSE;
    DECLARE topology_time TIMESTAMP;
    DECLARE last_topology_rollup_time TIMESTAMP;
    DECLARE cur1 CURSOR FOR (SELECT creation_time FROM ingested_live_topology WHERE creation_time > last_topology_rollup_time);
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    SET @last_aggregation_time='1970-01-01 12:00:00';
    SET @aggregation_query=CONCAT('SELECT ',aggregation_column,' INTO @last_aggregation_time FROM aggregation_meta_data where aggregate_table = ''reserved_instance_utilization_latest''');

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

      SET @update_aggregation_metadata=CONCAT('UPDATE aggregation_meta_data SET ',aggregation_column,' = ''',topology_time,''' where aggregate_table = ''reserved_instance_utilization_latest''');

      PREPARE stmt from @update_aggregation_metadata;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;
    END LOOP;

    CLOSE cur1;
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

DROP PROCEDURE IF EXISTS entity_savings_rollup //
CREATE PROCEDURE entity_savings_rollup(
    -- the table containing the records to be rolled up
    IN source_table CHAR(40),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(40),
    -- the snapshot time of the source records to be processed - must have zero milliseconds value
    IN snapshot_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME
)
BEGIN
    SET @sql = CONCAT(
        'INSERT INTO ', rollup_table,
        ' (entity_oid, stats_time, stats_type, stats_value, samples)',
        ' SELECT entity_oid, ', "'", rollup_time, "'", ', stats_type, sum(stats_value) as stats_value, 1',
        ' FROM ', source_table,
        ' WHERE stats_time = ', "'", snapshot_time, "'",
        ' GROUP BY entity_oid, stats_type',
        ' ON DUPLICATE KEY UPDATE stats_value=stats_value+values(stats_value), samples=samples+1'
    );

    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //

DROP PROCEDURE IF EXISTS purge_expired_data //
CREATE PROCEDURE purge_expired_data(IN tableName CHAR(40), IN dateColumn CHAR(40), IN policyName CHAR(50), IN periodName CHAR(20))
MODIFIES SQL DATA
BEGIN
    SET @currentDate = UTC_DATE();
    SET @purge_sql=concat('DELETE FROM ',tableName,'
             WHERE ',dateColumn,'< (SELECT date_sub("',@currentDate,'", INTERVAL retention_period ',periodName,')
                                         FROM account_expenses_retention_policies
                                         WHERE policy_name="',policyName,'") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
END //

DROP TRIGGER IF EXISTS reserved_instance_utilization_keys //
CREATE TRIGGER reserved_instance_utilization_keys BEFORE INSERT ON reserved_instance_utilization_latest
  FOR EACH ROW
  BEGIN
  /* Set hour, day, and month keys for existing databases */
  SET NEW.hour_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.day_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.month_key=md5(concat(
      ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));
  END//

DROP TRIGGER IF EXISTS reserved_instance_coverage_keys //
CREATE TRIGGER reserved_instance_coverage_keys BEFORE INSERT ON reserved_instance_coverage_latest
  FOR EACH ROW
  BEGIN
  /* Set hour, day, and month keys for existing databases */
  SET NEW.hour_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
      ifnull(NEW.entity_id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.day_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.entity_id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.month_key=md5(concat(
      ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.entity_id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));
  END//

DELIMITER ;
