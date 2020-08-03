/* OM-53769. This migration contains a minor update on how we write last_aggregated values to aggregation_meta_data table */

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

      /* If an entry for entity_cost does not exist in the aggregation_meta_data table, create a new entry with default values */
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
      /* This value is used as the starting point for the next scheduled aggregation cycle */
      UPDATE aggregation_meta_data SET last_aggregated_by_hour = max_created_time where aggregate_table = 'entity_cost';

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

      UPDATE aggregation_meta_data SET last_aggregated_by_day = max_created_time  where aggregate_table = 'entity_cost';


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

      UPDATE aggregation_meta_data SET last_aggregated_by_month = max_created_time where aggregate_table = 'entity_cost';
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