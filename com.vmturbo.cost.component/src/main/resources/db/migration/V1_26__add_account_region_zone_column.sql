/**
 * This script does a number of things:
 * 1. it adds account_id, region_id, availability_zone_id to entity_cost,
 *    entity_cost_by_hour, entity_cost_by_day, and entity_cost_by_month tables. This columes
 *    respectively keep the account, region, availability zone associated with the entity
 *    that we keep the cost for. This change also define an index on these columns.
 * 2. it modifies the rollup mechanism so it rollups the stats considering their
 *    account information.
 */


-- add columns to entity_cost table
ALTER TABLE entity_cost ADD COLUMN account_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost ADD COLUMN region_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost ADD COLUMN availability_zone_id bigint(20) DEFAULT NULL;

-- add indices for newly added columns to entity_cost
ALTER TABLE entity_cost ADD INDEX entity_cost_account_id_index(account_id);
ALTER TABLE entity_cost ADD INDEX entity_cost_region_id_index(region_id);
ALTER TABLE entity_cost ADD INDEX entity_cost_availability_zone_id_index(availability_zone_id);


-- add columns to entity_cost_by_hour tablea
ALTER TABLE entity_cost_by_hour ADD COLUMN account_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost_by_hour ADD COLUMN region_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost_by_hour ADD COLUMN availability_zone_id bigint(20) DEFAULT NULL;

-- add indices for newly added columns to entity_cost_by_hour
ALTER TABLE entity_cost_by_hour ADD INDEX entity_cost_by_hour_account_id_index(account_id);
ALTER TABLE entity_cost_by_hour ADD INDEX entity_cost_by_hour_region_id_index(region_id);
ALTER TABLE entity_cost_by_hour ADD INDEX entity_cost_by_hour_availability_zone_id_index(availability_zone_id);


-- add columns to entity_cost_by_day table
ALTER TABLE entity_cost_by_day ADD COLUMN account_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost_by_day ADD COLUMN region_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost_by_day ADD COLUMN availability_zone_id bigint(20) DEFAULT NULL;

-- add indices for newly added columns to entity_cost_by_day
ALTER TABLE entity_cost_by_day ADD INDEX entity_cost_by_day_account_id_index(account_id);
ALTER TABLE entity_cost_by_day ADD INDEX entity_cost_by_day_region_id_index(region_id);
ALTER TABLE entity_cost_by_day ADD INDEX entity_cost_by_day_availability_zone_id_index(availability_zone_id);


-- add columns to entity_cost_by_month table
ALTER TABLE entity_cost_by_month ADD COLUMN account_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost_by_month ADD COLUMN region_id bigint(20) DEFAULT NULL;
ALTER TABLE entity_cost_by_month ADD COLUMN availability_zone_id bigint(20) DEFAULT NULL;

-- add indices for newly added columns to entity_cost_by_month
ALTER TABLE entity_cost_by_month ADD INDEX entity_cost_by_month_account_id_index(account_id);
ALTER TABLE entity_cost_by_month ADD INDEX entity_cost_by_month_region_id_index(region_id);
ALTER TABLE entity_cost_by_month ADD INDEX entity_cost_by_month_availability_zone_id_index(availability_zone_id);

/** We drop the trigger that sets the hour_key, day_key, month_key as the
 * rollup process can work without them. We also drop those columns
 */
DROP TRIGGER IF EXISTS entity_cost_keys;

ALTER TABLE entity_cost DROP COLUMN hour_key;
ALTER TABLE entity_cost DROP COLUMN day_key;
ALTER TABLE entity_cost DROP COLUMN month_key;

ALTER TABLE entity_cost_by_hour DROP COLUMN hour_key;
ALTER TABLE entity_cost_by_hour DROP COLUMN day_key;
ALTER TABLE entity_cost_by_hour DROP COLUMN month_key;

ALTER TABLE entity_cost_by_day DROP COLUMN day_key;
ALTER TABLE entity_cost_by_day DROP COLUMN month_key;

ALTER TABLE entity_cost_by_month DROP COLUMN month_key;

/**
 * When not default value sets for the first timestamp column
 * of a table the default value will be set as CURRENT_TIME ON UPDATE CURRENT TIME
 * which is undesirable for our case. THerefore, we set a default value for
 * create_time of entity_cost_by_hour, entity_cost_by_day, and entity_cost_by_month
 * table.
 */
ALTER TABLE entity_cost_by_hour ALTER created_time SET DEFAULT '0000-00-00 00:00:00';
ALTER TABLE entity_cost_by_day ALTER created_time SET DEFAULT '0000-00-00 00:00:00';
ALTER TABLE entity_cost_by_month ALTER created_time SET DEFAULT '0000-00-00 00:00:00';

/** Updating procedure that runs hourly to perform hourly/daily/monthly rollups.
 * This is identical to the changes in V1_8__create_entity_cost_aggregation
 * with the only difference that we populate account_id, region_id,
 * and availability_zone_id in the rollup table.
 */

DROP PROCEDURE IF EXISTS aggregate_entity_cost;

DELIMITER //
/* Procedure:  aggregate_entity_cost */
/* Rolls up current entity cost data to hourly, daily and monthly tables simultaneous */
CREATE PROCEDURE aggregate_entity_cost()
  BEGIN
      DECLARE last_aggregated_time TIMESTAMP;
      DECLARE min_created_time TIMESTAMP;
      DECLARE max_created_time TIMESTAMP;
      DECLARE last_aggregated_by_hour_time TIMESTAMP;
      DECLARE last_aggregated_by_day_time TIMESTAMP;
      DECLARE last_aggregated_by_month_time TIMESTAMP;

      /* Find oldest and newest records in the entity_cost table */
      SELECT min(created_time) INTO min_created_time from entity_cost;
      SELECT max(created_time) INTO max_created_time from entity_cost;

      /* The aggregation_meta_data table contains the details of the most recent rollup timestamps processed in the hourly, daily and monthly tables */
      /* Used as a starting point for future aggregations.*/
      /* Each aggregation cycle is defined by rows of data with between a starting time and an ending time. */
      /* The next aggregation of the hourly, daily, and monthly tables will be any data inserted with a timestamp greater than the last cycle processed */
      SELECT last_aggregated INTO last_aggregated_time FROM aggregation_meta_data where aggregate_table = 'entity_cost';
      SELECT last_aggregated_by_hour INTO last_aggregated_by_hour_time FROM aggregation_meta_data where aggregate_table = 'entity_cost';
      SELECT last_aggregated_by_day INTO last_aggregated_by_day_time FROM aggregation_meta_data where aggregate_table = 'entity_cost';
      SELECT last_aggregated_by_month INTO last_aggregated_by_month_time FROM aggregation_meta_data where aggregate_table = 'entity_cost';

      /* If an entry for entity_cost does not exist in the aggreggation_meta_data table, create a new entry with default values */
      if (concat(last_aggregated_time,last_aggregated_by_hour_time,last_aggregated_by_day_time, last_aggregated_by_month_time)) is null then
        SELECT 'NO META DATA FOUND.  CREATING...';
        DELETE FROM aggregation_meta_data WHERE aggregate_table='entity_cost';
        INSERT INTO aggregation_meta_data (aggregate_table) VALUES ('entity_cost');
        SET last_aggregated_time = min_created_time;
      end if;

      if (last_aggregated_by_hour_time='0000-00-00 00:00:00' OR last_aggregated_by_hour_time IS NULL) THEN
        SET last_aggregated_by_hour_time = '1970-01-01 00:00:01';
      end if;
      SET @last_hourly = last_aggregated_by_hour_time;


      SET @sql=concat('CREATE OR REPLACE VIEW entity_cost_hourly_snapshots_vw AS
      SELECT created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      account_id,
      region_id,
      availability_zone_id,
      sum(amount) as amount
      FROM entity_cost
      WHERE created_time > \'',@last_hourly,'\'
      GROUP BY created_time, associated_entity_id, associated_entity_type,
      cost_type, currency, account_id, region_id, availability_zone_id');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      SET @sql='CREATE OR REPLACE VIEW entity_cost_hourly_ins_vw AS
      SELECT TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d %H:00:00")) as created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      account_id,
      region_id,
      availability_zone_id,
      avg(amount) as amount,
      count(*) as samples
      FROM entity_cost_hourly_snapshots_vw
      GROUP BY created_time, associated_entity_id, associated_entity_type,
      cost_type, currency, account_id, region_id, availability_zone_id';

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      /* Rollup the rows in the view to the entity_cost_by_hour table */
      /* If an existing entry exists, the new amount is averaged into the existing amount, otherwise a new hourly entry is created */
      INSERT INTO entity_cost_by_hour (associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount, samples, account_id, region_id, availability_zone_id)
      SELECT associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount, samples, account_id, region_id, availability_zone_id
      FROM entity_cost_hourly_ins_vw b
      ON DUPLICATE KEY UPDATE entity_cost_by_hour.amount =
      ((entity_cost_by_hour.amount * entity_cost_by_hour.samples)+(b.amount*b.samples))/(entity_cost_by_hour.samples+b.samples),
      entity_cost_by_hour.samples=entity_cost_by_hour.samples+b.samples;

      /* When the hourly aggregation is complete, UPDATE the aggregation_meta_data table with the most recent created_time processed */
      /* This value is used as the starting point for the next scheduled aggregation cycel */
      UPDATE aggregation_meta_data SET last_aggregated_by_hour = max_created_time;

      /* Repeat the same process for daily and monthly rollups */
      /* DAILY AGGREGATION */

      if (last_aggregated_by_day_time='0000-00-00 00:00:00' OR last_aggregated_by_day_time IS NULL) THEN
        SET last_aggregated_by_day_time = '1970-01-01 00:00:01';
      end if;

      SET @last_daily = last_aggregated_by_day_time;

      SET @sql=concat('CREATE OR REPLACE VIEW entity_cost_daily_snapshots_vw AS
      SELECT created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      account_id,
      region_id,
      availability_zone_id,
      sum(amount) as amount
      FROM entity_cost
      WHERE created_time > \'',@last_daily,'\'
      GROUP BY created_time, associated_entity_id, associated_entity_type,
      cost_type, currency, account_id, region_id, availability_zone_id');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      SET @sql= 'CREATE OR REPLACE VIEW entity_cost_daily_ins_vw AS
      SELECT TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d 00:00:00")) as created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      account_id,
      region_id,
      availability_zone_id,
      avg(amount) as amount,
      count(*) as samples
      FROM entity_cost_daily_snapshots_vw
      GROUP BY created_time, associated_entity_id, associated_entity_type,
      cost_type, currency, account_id, region_id, availability_zone_id';

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      INSERT INTO entity_cost_by_day (associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount, samples, account_id, region_id, availability_zone_id)
      SELECT associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,samples, account_id, region_id, availability_zone_id
      FROM entity_cost_daily_ins_vw b
      ON DUPLICATE KEY UPDATE entity_cost_by_day.amount =
      ((entity_cost_by_day.amount * entity_cost_by_day.samples)+(b.amount*b.samples))/(entity_cost_by_day.samples+b.samples),
      entity_cost_by_day.samples=entity_cost_by_day.samples+b.samples;

      UPDATE aggregation_meta_data SET last_aggregated_by_day = max_created_time;


      /* MONTHLY AGGREGATION */

      if (last_aggregated_by_month_time='0000-00-00 00:00:00' OR last_aggregated_by_month_time IS NULL) THEN
        SET last_aggregated_by_month_time = '1970-01-01 00:00:01';
      end if;

      SET @last_monthly = last_aggregated_by_month_time;

      SET @sql=concat('CREATE OR REPLACE VIEW entity_cost_monthly_snapshots_vw AS
      SELECT created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      account_id,
      region_id,
      availability_zone_id,
      sum(amount) as amount
      FROM entity_cost
      WHERE created_time > \'',@last_daily,'\'
      GROUP BY created_time, associated_entity_id, associated_entity_type,
      cost_type, currency, account_id, region_id, availability_zone_id');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      SET @sql='CREATE OR REPLACE VIEW entity_cost_monthly_ins_vw AS
      SELECT TIMESTAMP(DATE_FORMAT(last_day(created_time),"%Y-%m-%d 00:00:00")) as created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      account_id,
      region_id,
      availability_zone_id,
      avg(amount) as amount,
      count(*) as samples
      FROM entity_cost_monthly_snapshots_vw
      GROUP BY created_time, associated_entity_id, associated_entity_type,
      cost_type, currency, account_id, region_id, availability_zone_id';

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      INSERT INTO entity_cost_by_month (associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,samples, account_id, region_id, availability_zone_id)
      SELECT associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,samples, account_id, region_id, availability_zone_id
      FROM entity_cost_monthly_ins_vw b
      ON DUPLICATE KEY UPDATE entity_cost_by_month.amount =
      ((entity_cost_by_month.amount * entity_cost_by_month.samples)+(b.amount*b.samples))/(entity_cost_by_month.samples+b.samples),
      entity_cost_by_month.samples=entity_cost_by_month.samples+b.samples;

      UPDATE aggregation_meta_data SET last_aggregated_by_month = max_created_time;
  END//

/* Recreate events to aggregate_cost scheduled event  */
/* Executes hourly, running the aggregate_reserved_instance_coverage, and aggregate_reserved_instance_utilization procedures */
DROP EVENT IF EXISTS aggregate_entity_cost_event
//
CREATE EVENT aggregate_entity_cost_event ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  call aggregate_entity_cost;
END
//