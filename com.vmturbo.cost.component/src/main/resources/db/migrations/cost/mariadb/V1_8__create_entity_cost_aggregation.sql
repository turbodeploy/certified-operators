/* V1.8__create_entity_cost_aggregation.sql */

/* Aggregation Meta Data table creation */
/* This is a utility table used to store states of aggregation */
DROP TABLE IF EXISTS aggregation_meta_data;
CREATE TABLE aggregation_meta_data (
  aggregate_table varchar(64),
  last_aggregated timestamp,
  last_aggregated_by_hour timestamp,
  last_aggregated_by_day timestamp,
  last_aggregated_by_month timestamp,
  PRIMARY KEY(aggregate_table)
);

/* Create entity_cost_by_hour table */
DROP TABLE IF EXISTS entity_cost_by_hour;
CREATE TABLE entity_cost_by_hour (
  associated_entity_id bigint(20) NOT NULL,
  created_time timestamp,
  associated_entity_type int(11) NOT NULL,
  cost_type int(11) NOT NULL,
  currency int(11) NOT NULL,
  amount decimal(20,7) NOT NULL,
  hour_key varchar(32) NOT NULL,
  day_key varchar(32) DEFAULT NULL,
  month_key varchar(32) DEFAULT NULL,
  samples int(11),
  PRIMARY KEY (hour_key),
  INDEX ech_ct (created_time)
);

/* Create entity_cost_by_day table */
DROP TABLE IF EXISTS entity_cost_by_day;
CREATE TABLE entity_cost_by_day (
  associated_entity_id bigint(20) NOT NULL,
  created_time timestamp NOT NULL,
  associated_entity_type int(11) NOT NULL,
  cost_type int(11) NOT NULL,
  currency int(11) NOT NULL,
  amount decimal(20,7) NOT NULL,
  day_key varchar(32) NOT NULL,
  month_key varchar(32) NOT NULL,
  samples int(11),
  PRIMARY KEY (day_key),
  INDEX echd_ct(created_time)
);

/* Create entity_cost_by_month table */
DROP TABLE IF EXISTS entity_cost_by_month;
CREATE TABLE entity_cost_by_month (
  associated_entity_id bigint(20) NOT NULL,
  created_time timestamp NOT NULL,
  associated_entity_type int(11) NOT NULL,
  cost_type int(11) NOT NULL,
  currency int(11) NOT NULL,
  amount decimal(20,7) NOT NULL,
  month_key varchar(32) NOT NULL,
  samples int(11),
  PRIMARY KEY (month_key),
  INDEX echm(created_time)
);

/* Add primary keys to uniquely identify rows for aggregation */
/* These keys are a unique md5 checksum of all the non-value columns */
/* These keys uniquely identify an aggregate row in each of the hourly, daily and monthly tables */
/* They are used instead of creating a long composite key, which is comprised of all the columns in the table */
/* minus the value columns which are being aggregated.  A composite key index of this size will be essentially */
/* the zie of the original table */

ALTER TABLE entity_cost ADD hour_key varchar(32);
ALTER TABLE entity_cost ADD day_key varchar(32);
ALTER TABLE entity_cost ADD month_key varchar(32);
ALTER TABLE entity_cost DROP PRIMARY KEY;
CREATE INDEX ec_ct ON entity_cost(created_time);

/* Set hour, day, and month keys for existing databases */
UPDATE entity_cost SET hour_key=md5(concat(
    ifnull(date_format(created_time,"%Y-%m-%d %H:00:00"),'-'),
    ifnull(associated_entity_id,'-'),
    ifnull(associated_entity_type,'-'),
    ifnull(cost_type,'-'),
    ifnull(currency,'-')
));

UPDATE entity_cost SET day_key=md5(concat(
    ifnull(date_format(created_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(associated_entity_id,'-'),
    ifnull(associated_entity_type,'-'),
    ifnull(cost_type,'-'),
    ifnull(currency,'-')
));

UPDATE entity_cost SET month_key=md5(concat(
    ifnull(date_format(last_day(created_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(associated_entity_id,'-'),
    ifnull(associated_entity_type,'-'),
    ifnull(cost_type,'-'),
    ifnull(currency,'-')
));

/* Triggers to UPDATE hour, day and month keys on each new inserted row */
DROP TRIGGER IF EXISTS entity_cost_keys;
DELIMITER //
CREATE TRIGGER entity_cost_keys BEFORE INSERT ON entity_cost
  FOR EACH ROW
  BEGIN
      SET NEW.hour_key=md5(concat(
        ifnull(date_format(NEW.created_time,"%Y-%m-%d %H:00:00"),'-'),
        ifnull(NEW.associated_entity_id,'-'),
        ifnull(NEW.associated_entity_type,'-'),
        ifnull(NEW.cost_type,'-'),
        ifnull(NEW.currency,'-')
      ));

      SET NEW.day_key=md5(concat(
        ifnull(date_format(NEW.created_time,"%Y-%m-%d 00:00:00"),'-'),
        ifnull(NEW.associated_entity_id,'-'),
        ifnull(NEW.associated_entity_type,'-'),
        ifnull(NEW.cost_type,'-'),
        ifnull(NEW.currency,'-')
      ));

      SET NEW.month_key=md5(concat(
        ifnull(date_format(last_day(NEW.created_time),"%Y-%m-%d 00:00:00"),'-'),
        ifnull(NEW.associated_entity_id,'-'),
        ifnull(NEW.associated_entity_type,'-'),
        ifnull(NEW.cost_type,'-'),
        ifnull(NEW.currency,'-')
      ));
  END//

  DROP PROCEDURE IF EXISTS aggregate_entity_cost;
    //
  /* Procedure:  aggregate_entity_cost */
  /* Rolls up current entity cost data to hourly, daily and monthly tables simultaneous */
  CREATE PROCEDURE aggregate_entity_cost()
    aggregate_entity:BEGIN
      DECLARE running_aggregations INT;
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
        SELECT min(created_time) INTO last_aggregated_time FROM entity_cost;
      end if;

      /* HOURLY AGGREGATION */

      if (last_aggregated_by_hour_time='0000-00-00 00:00:00' OR last_aggregated_by_hour_time IS NULL) THEN
        SET last_aggregated_by_hour_time = '1970-01-01 00:00:01';
      end if;
      SET @last_hourly = last_aggregated_by_hour_time;

      /* Create a view containing the entity_cost rows which have not been aggregated to the hourly table */
      SET @sql=concat('CREATE OR REPLACE VIEW entity_cost_hourly_ins_vw AS
      SELECT hour_key, day_key, month_key,
      TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d %H:00:00")) as created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      avg(amount) as amount,
      count(*) as samples
      FROM entity_cost
      WHERE created_time > \'',@last_hourly,'\' GROUP BY hour_key');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      /* Rollup the rows in the view to the entity_cost_by_hour table */
      /* If an existing entry exists, the new amount is averaged into the existing amount, otherwise a new hourly entry is created */
      INSERT INTO entity_cost_by_hour (associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,hour_key,day_key,month_key,samples)
      SELECT associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,hour_key,day_key,month_key,samples
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

      SET @sql=concat('CREATE OR REPLACE VIEW entity_cost_daily_ins_vw AS
      SELECT day_key, month_key,
      TIMESTAMP(DATE_FORMAT(created_time,"%Y-%m-%d 00:00:00")) as created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      avg(amount) as amount,
      count(*) as samples
      FROM entity_cost
      WHERE created_time > \'',@last_daily,'\' GROUP BY day_key');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      INSERT INTO entity_cost_by_day (associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,day_key,month_key,samples)
      SELECT associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,day_key,month_key,samples
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

      SET @sql=concat('CREATE OR REPLACE VIEW entity_cost_monthly_ins_vw AS
      SELECT month_key,
      TIMESTAMP(DATE_FORMAT(last_day(created_time),"%Y-%m-%d 00:00:00")) as created_time,
      associated_entity_id,
      associated_entity_type,
      cost_type,
      currency,
      avg(amount) as amount,
      count(*) as samples
      FROM entity_cost
      WHERE created_time > \'',@last_monthly,'\' GROUP BY month_key');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      INSERT INTO entity_cost_by_month (associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,month_key,samples)
      SELECT associated_entity_id, created_time,associated_entity_type,cost_type,currency,amount,month_key,samples
      FROM entity_cost_monthly_ins_vw b
      ON DUPLICATE KEY UPDATE entity_cost_by_month.amount =
      ((entity_cost_by_month.amount * entity_cost_by_month.samples)+(b.amount*b.samples))/(entity_cost_by_month.samples+b.samples),
      entity_cost_by_month.samples=entity_cost_by_month.samples+b.samples;

      UPDATE aggregation_meta_data SET last_aggregated_by_month = max_created_time;

    END//

    /* Recreate events to aggregate_cost scheduled event  */
    /* Executes hourly, running the aggregate_reserved_instance_coverage, and aggregate_reserved_instance_utilization procedures */
    DROP EVENT IF EXISTS aggregate_entity_cost_event;
    //
    CREATE
    EVENT aggregate_entity_cost_event
      ON SCHEDULE EVERY 1 HOUR
    DO BEGIN
      call aggregate_entity_cost;
    END //
