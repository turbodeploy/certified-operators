/* Add primary keys to uniquely identify rows for aggregation */
ALTER TABLE reserved_instance_utilization_latest ADD hour_key varchar(32);
ALTER TABLE reserved_instance_utilization_latest ADD day_key varchar(32);
ALTER TABLE reserved_instance_utilization_latest ADD month_key varchar(32);

/* Set hour, day, and month keys for existing databases */
UPDATE reserved_instance_utilization_latest SET hour_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
    ifnull(id,'-'),
    ifnull(region_id,'-'),
    ifnull(availability_zone_id,'-'),
    ifnull(business_account_id,'-')
));

UPDATE reserved_instance_utilization_latest SET day_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(id,'-'),
    ifnull(region_id,'-'),
    ifnull(availability_zone_id,'-'),
    ifnull(business_account_id,'-')
));

UPDATE reserved_instance_utilization_latest SET month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d %H:00:00"),'-'),
    ifnull(id,'-'),
    ifnull(region_id,'-'),
    ifnull(availability_zone_id,'-'),
    ifnull(business_account_id,'-')
));

/* Add primary keys to uniquely identify rows for aggregation */
ALTER TABLE reserved_instance_coverage_latest ADD hour_key varchar(32);
ALTER TABLE reserved_instance_coverage_latest ADD day_key varchar(32);
ALTER TABLE reserved_instance_coverage_latest ADD month_key varchar(32);

/* Set hour, day, and month keys for existing databases */
UPDATE reserved_instance_coverage_latest SET hour_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
    ifnull(entity_id,'-'),
    ifnull(region_id,'-'),
    ifnull(availability_zone_id,'-'),
    ifnull(business_account_id,'-')
));

UPDATE reserved_instance_coverage_latest SET day_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(entity_id,'-'),
    ifnull(region_id,'-'),
    ifnull(availability_zone_id,'-'),
    ifnull(business_account_id,'-')
));

UPDATE reserved_instance_coverage_latest SET month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d %H:00:00"),'-'),
    ifnull(entity_id,'-'),
    ifnull(region_id,'-'),
    ifnull(availability_zone_id,'-'),
    ifnull(business_account_id,'-')
));

/* Triggers to update hour, day and month keys on each new inserted row */
DROP TRIGGER IF EXISTS reserved_instance_utilization_keys;
DELIMITER //
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
      ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d %H:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));
  END//
  DELIMITER ;

  /* Triggers to update hour, day and month keys on each new inserted row */
  DROP TRIGGER IF EXISTS reserved_instance_coverage_keys;
  DELIMITER //
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
        ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d %H:00:00"),'-'),
        ifnull(NEW.entity_id,'-'),
        ifnull(NEW.region_id,'-'),
        ifnull(NEW.availability_zone_id,'-'),
        ifnull(NEW.business_account_id,'-')
    ));
    END//
    DELIMITER ;



-- This table store the utilization of reserved instance, which means for each reserved instance
-- what is the total coupons it has, and what is the amount of coupons which used by other entities.
DROP TABLE IF EXISTS reserved_instance_utilization_by_hour;
CREATE TABLE reserved_instance_utilization_by_hour (
    snapshot_time                      TIMESTAMP       NOT NULL,
    id                                 BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      FLOAT           NOT NULL,
    used_coupons                       FLOAT           NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (hour_key),
    INDEX riuh_idst (id,snapshot_time)
);

DROP TABLE IF EXISTS reserved_instance_utilization_by_day;
CREATE TABLE reserved_instance_utilization_by_day (
    snapshot_time                      TIMESTAMP       NOT NULL,
    id                                 BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      FLOAT           NOT NULL,
    used_coupons                       FLOAT           NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (day_key),
    INDEX riud_idst (id,snapshot_time)
);

DROP TABLE IF EXISTS reserved_instance_utilization_by_month;
CREATE TABLE reserved_instance_utilization_by_month (
    snapshot_time                      TIMESTAMP       NOT NULL,
    id                                 BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      FLOAT           NOT NULL,
    used_coupons                       FLOAT           NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (month_key),
    INDEX rium_idst (id,snapshot_time)
);

-- This table store the coverage information of reserved instance, which means for each entity,
-- what is the total coupons it has, and what is the amount of coupons covered by reserved instance.
DROP TABLE IF EXISTS reserved_instance_coverage_by_hour;
CREATE TABLE reserved_instance_coverage_by_hour (
    snapshot_time                      TIMESTAMP       NOT NULL,
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      FLOAT           NOT NULL,
    used_coupons                       FLOAT           NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (hour_key),
    INDEX rich_eidst (entity_id, snapshot_time)
);

DROP TABLE IF EXISTS reserved_instance_coverage_by_day;
CREATE TABLE reserved_instance_coverage_by_day (
    snapshot_time                      TIMESTAMP       NOT NULL,
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      FLOAT           NOT NULL,
    used_coupons                       FLOAT           NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (day_key),
    INDEX ricd_eidst (entity_id, snapshot_time)
);

DROP TABLE IF EXISTS reserved_instance_coverage_by_month;
CREATE TABLE reserved_instance_coverage_by_month (
    snapshot_time                      TIMESTAMP       NOT NULL,
    entity_id                          BIGINT          NOT NULL,
    region_id                          BIGINT          NOT NULL,
    availability_zone_id               BIGINT          NOT NULL,
    business_account_id                BIGINT          NOT NULL,
    total_coupons                      FLOAT           NOT NULL,
    used_coupons                       FLOAT           NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (month_key),
    INDEX ricm_eidst (entity_id, snapshot_time)
);


/* Aggregation Procedures */

DROP PROCEDURE IF EXISTS aggregate_reserved_instance_coverage;
/* Procedure:  aggregate_reserved_instance_coverage */
/* Rolls up current entity cost data to hourly, daily and monthly tables simultaneous */
DELIMITER //
CREATE PROCEDURE aggregate_reserved_instance_coverage()
  aggregate_ric:BEGIN
    DECLARE running_aggregations INT;
    DECLARE last_aggregated_time TIMESTAMP;
    DECLARE min_created_time TIMESTAMP;
    DECLARE max_created_time TIMESTAMP;
    DECLARE last_aggregated_by_hour_time TIMESTAMP;
    DECLARE last_aggregated_by_day_time TIMESTAMP;
    DECLARE last_aggregated_by_month_time TIMESTAMP;

    /* Find oldest and newest records in the reserved_instance_coverage table */
    SELECT min(snapshot_time) INTO min_created_time from reserved_instance_coverage_latest;
    SELECT max(snapshot_time) INTO max_created_time from reserved_instance_coverage_latest;

    /* The aggregation_meta_data table contains the details of the most recent rollup timestamps processed in the hourly, daily and monthly tables */
    /* Used as a starting point for future aggregations.                                                                                           */
    SELECT last_aggregated INTO last_aggregated_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_coverage_latest';
    SELECT last_aggregated_by_hour INTO last_aggregated_by_hour_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_coverage_latest';
    SELECT last_aggregated_by_day INTO last_aggregated_by_day_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_coverage_latest';
    SELECT last_aggregated_by_month INTO last_aggregated_by_month_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_coverage_latest';

    /* If an entry for reserved_instance_coverage does not exist in the aggreggation_meta_data table, create a new entry with default values */
    if (concat(last_aggregated_time,last_aggregated_by_hour_time,last_aggregated_by_day_time, last_aggregated_by_month_time)) is null then
      SELECT 'NO META DATA FOUND.  CREATING...';
      DELETE FROM aggregation_meta_data WHERE aggregate_table='reserved_instance_coverage_latest';
      INSERT INTO aggregation_meta_data (aggregate_table) VALUES ('reserved_instance_coverage_latest');
      SELECT min(snapshot_time) INTO last_aggregated_time FROM reserved_instance_coverage_latest;
    end if;

    /* HOURLY AGGREGATION */

    if (last_aggregated_by_hour_time='0000-00-00 00:00:00' OR last_aggregated_by_hour_time IS NULL) THEN
      set last_aggregated_by_hour_time = '1970-01-01 00:00:00';
    end if;
    set @last_hourly = last_aggregated_by_hour_time;

    /* Create a view containing the reserved_instance_coverage rows which have not been aggregated to the hourly table */
    set @sql=concat('CREATE OR REPLACE VIEW reserved_instance_coverage_hourly_ins_vw AS
    SELECT hour_key, day_key, month_key,
    DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time,
    entity_id,
    region_id,
    availability_zone_id,
    business_account_id,
    avg(total_coupons) as total_coupons,
    avg(used_coupons) as used_coupons,
    count(*) as samples
    FROM reserved_instance_coverage_latest
    WHERE snapshot_time > \'',@last_hourly,'\' GROUP BY hour_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* Rollup the rows in the view to the reserved_instance_coverage_by_hour table */
    /* If an existing entry exists, the new amount is averaged into the existing amount, otherwise a new hourly entry is created */
    INSERT INTO reserved_instance_coverage_by_hour (snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,hour_key,day_key,month_key,samples)
    SELECT snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,hour_key,day_key,month_key,samples
    FROM reserved_instance_coverage_hourly_ins_vw b
    ON DUPLICATE KEY UPDATE reserved_instance_coverage_by_hour.total_coupons =
    ((reserved_instance_coverage_by_hour.total_coupons * reserved_instance_coverage_by_hour.samples)+(b.total_coupons*b.samples))/(reserved_instance_coverage_by_hour.samples+b.samples),
    reserved_instance_coverage_by_hour.used_coupons =
    ((reserved_instance_coverage_by_hour.used_coupons * reserved_instance_coverage_by_hour.samples)+(b.used_coupons*b.samples))/(reserved_instance_coverage_by_hour.samples+b.samples),
    reserved_instance_coverage_by_hour.samples=reserved_instance_coverage_by_hour.samples+b.samples;

    /* When the hourly aggregation is complete, update the aggregation_meta_data table with the most recent snapshot_time processed */
    /* This value is used as the starting point for the next scheduled aggregation cycel */
    update aggregation_meta_data set last_aggregated_by_hour = max_created_time;


    /* Repeat the same process for daily and monthly rollups */
    /* DAILY AGGREGATION */

    if (last_aggregated_by_day_time='0000-00-00 00:00:00' OR last_aggregated_by_day_time IS NULL) THEN
      set last_aggregated_by_day_time = '1970-01-01 00:00:00';
    end if;

    set @last_daily = last_aggregated_by_day_time;

    set @sql=concat('CREATE OR REPLACE VIEW reserved_instance_coverage_daily_ins_vw AS
    SELECT day_key, month_key,
    DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time,
    entity_id,
    region_id,
    availability_zone_id,
    business_account_id,
    avg(total_coupons) as total_coupons,
    avg(used_coupons) as used_coupons,
    count(*) as samples
    FROM reserved_instance_coverage_latest
    WHERE snapshot_time > \'',@last_hourly,'\' GROUP BY day_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    INSERT INTO reserved_instance_coverage_by_day (snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,day_key,month_key,samples)
    SELECT snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,day_key,month_key,samples
    FROM reserved_instance_coverage_daily_ins_vw b
    ON DUPLICATE KEY UPDATE reserved_instance_coverage_by_day.total_coupons =
    ((reserved_instance_coverage_by_day.total_coupons * reserved_instance_coverage_by_day.samples)+(b.total_coupons*b.samples))/(reserved_instance_coverage_by_day.samples+b.samples),
    reserved_instance_coverage_by_day.used_coupons =
    ((reserved_instance_coverage_by_day.used_coupons * reserved_instance_coverage_by_day.samples)+(b.used_coupons*b.samples))/(reserved_instance_coverage_by_day.samples+b.samples),
    reserved_instance_coverage_by_day.samples=reserved_instance_coverage_by_day.samples+b.samples;

    update aggregation_meta_data set last_aggregated_by_day = max_created_time;


    /* MONTHLY AGGREGATION */

    if (last_aggregated_by_month_time='0000-00-00 00:00:00' OR last_aggregated_by_month_time IS NULL) THEN
      set last_aggregated_by_month_time = '1970-01-01 00:00:00';
    end if;

    set @last_monthly = last_aggregated_by_month_time;

    set @sql=concat('CREATE OR REPLACE VIEW reserved_instance_coverage_monthly_ins_vw AS
    SELECT month_key,
    DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
    entity_id,
    region_id,
    availability_zone_id,
    business_account_id,
    avg(total_coupons) as total_coupons,
    avg(used_coupons) as used_coupons,
    count(*) as samples
    FROM reserved_instance_coverage_latest WHERE snapshot_time > \'',@last_monthly,'\' GROUP BY month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    INSERT INTO reserved_instance_coverage_by_month (snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,month_key,samples)
    SELECT snapshot_time, entity_id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,month_key,samples
    FROM reserved_instance_coverage_monthly_ins_vw b
    ON DUPLICATE KEY UPDATE reserved_instance_coverage_by_month.total_coupons =
    ((reserved_instance_coverage_by_month.total_coupons * reserved_instance_coverage_by_month.samples)+(b.total_coupons*b.samples))/(reserved_instance_coverage_by_month.samples+b.samples),
    reserved_instance_coverage_by_month.used_coupons =
    ((reserved_instance_coverage_by_month.used_coupons * reserved_instance_coverage_by_month.samples)+(b.used_coupons*b.samples))/(reserved_instance_coverage_by_month.samples+b.samples),
    reserved_instance_coverage_by_month.samples=reserved_instance_coverage_by_month.samples+b.samples;

    update aggregation_meta_data set last_aggregated_by_month = max_created_time;

  END//
  DELIMITER ;



  DROP PROCEDURE IF EXISTS aggregate_reserved_instance_utilization;
  /* Procedure:  aggregate_reserved_instance_utilization */
  /* Rolls up current entity cost data to hourly, daily and monthly tables simultaneous */
  DELIMITER //
  CREATE PROCEDURE aggregate_reserved_instance_utilization()
    aggregate_ric:BEGIN
      DECLARE running_aggregations INT;
      DECLARE last_aggregated_time TIMESTAMP;
      DECLARE min_created_time TIMESTAMP;
      DECLARE max_created_time TIMESTAMP;
      DECLARE last_aggregated_by_hour_time TIMESTAMP;
      DECLARE last_aggregated_by_day_time TIMESTAMP;
      DECLARE last_aggregated_by_month_time TIMESTAMP;

      /* Find oldest and newest records in the reserved_instance_utilization table */
      SELECT min(snapshot_time) INTO min_created_time from reserved_instance_utilization_latest;
      SELECT max(snapshot_time) INTO max_created_time from reserved_instance_utilization_latest;

      /* The aggregation_meta_data table contains the details of the most recent rollup timestamps processed in the hourly, daily and monthly tables */
      /* Used as a starting point for future aggregations.                                                                                           */
      SELECT last_aggregated INTO last_aggregated_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_utilization_latest';
      SELECT last_aggregated_by_hour INTO last_aggregated_by_hour_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_utilization_latest';
      SELECT last_aggregated_by_day INTO last_aggregated_by_day_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_utilization_latest';
      SELECT last_aggregated_by_month INTO last_aggregated_by_month_time FROM aggregation_meta_data where aggregate_table = 'reserved_instance_utilization_latest';

      /* If an entry for reserved_instance_utilization does not exist in the aggreggation_meta_data table, create a new entry with default values */
      if (concat(last_aggregated_time,last_aggregated_by_hour_time,last_aggregated_by_day_time, last_aggregated_by_month_time)) is null then
        SELECT 'NO META DATA FOUND.  CREATING...';
        DELETE FROM aggregation_meta_data WHERE aggregate_table='reserved_instance_utilization_latest';
        INSERT INTO aggregation_meta_data (aggregate_table) VALUES ('reserved_instance_utilization_latest');
        SELECT min(snapshot_time) INTO last_aggregated_time FROM reserved_instance_utilization_latest;
      end if;

      /* HOURLY AGGREGATION */

      if (last_aggregated_by_hour_time='0000-00-00 00:00:00' OR last_aggregated_by_hour_time IS NULL) THEN
        set last_aggregated_by_hour_time = '1970-01-01 00:00:00';
      end if;
      set @last_hourly = last_aggregated_by_hour_time;

      /* Create a view containing the reserved_instance_utilization rows which have not been aggregated to the hourly table */
      set @sql=concat('CREATE OR REPLACE VIEW reserved_instance_utilization_hourly_ins_vw AS
      SELECT hour_key, day_key, month_key,
      DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time,
      id,
      region_id,
      availability_zone_id,
      business_account_id,
      avg(total_coupons) as total_coupons,
      avg(used_coupons) as used_coupons,
      count(*) as samples
      FROM reserved_instance_utilization_latest
      WHERE snapshot_time > \'',@last_hourly,'\' GROUP BY hour_key');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      /* Rollup the rows in the view to the reserved_instance_utilization_by_hour table */
      /* If an existing entry exists, the new amount is averaged into the existing amount, otherwise a new hourly entry is created */
      INSERT INTO reserved_instance_utilization_by_hour (snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,hour_key,day_key,month_key,samples)
      SELECT snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,hour_key,day_key,month_key,samples
      FROM reserved_instance_utilization_hourly_ins_vw b
      ON DUPLICATE KEY UPDATE reserved_instance_utilization_by_hour.total_coupons =
      ((reserved_instance_utilization_by_hour.total_coupons * reserved_instance_utilization_by_hour.samples)+(b.total_coupons*b.samples))/(reserved_instance_utilization_by_hour.samples+b.samples),
      reserved_instance_utilization_by_hour.used_coupons =
      ((reserved_instance_utilization_by_hour.used_coupons * reserved_instance_utilization_by_hour.samples)+(b.used_coupons*b.samples))/(reserved_instance_utilization_by_hour.samples+b.samples),
      reserved_instance_utilization_by_hour.samples=reserved_instance_utilization_by_hour.samples+b.samples;

      /* When the hourly aggregation is complete, update the aggregation_meta_data table with the most recent snapshot_time processed */
      /* This value is used as the starting point for the next scheduled aggregation cycel */
      update aggregation_meta_data set last_aggregated_by_hour = max_created_time;


      /* Repeat the same process for daily and monthly rollups */
      /* DAILY AGGREGATION */

      if (last_aggregated_by_day_time='0000-00-00 00:00:00' OR last_aggregated_by_day_time IS NULL) THEN
        set last_aggregated_by_day_time = '1970-01-01 00:00:00';
      end if;

      set @last_daily = last_aggregated_by_day_time;

      set @sql=concat('CREATE OR REPLACE VIEW reserved_instance_utilization_daily_ins_vw AS
      SELECT day_key, month_key,
      DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time,
      id,
      region_id,
      availability_zone_id,
      business_account_id,
      avg(total_coupons) as total_coupons,
      avg(used_coupons) as used_coupons,
      count(*) as samples
      FROM reserved_instance_utilization_latest
      WHERE snapshot_time > \'',@last_hourly,'\' GROUP BY day_key');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      INSERT INTO reserved_instance_utilization_by_day (snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,day_key,month_key,samples)
      SELECT snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,day_key,month_key,samples
      FROM reserved_instance_utilization_daily_ins_vw b
      ON DUPLICATE KEY UPDATE reserved_instance_utilization_by_day.total_coupons =
      ((reserved_instance_utilization_by_day.total_coupons * reserved_instance_utilization_by_day.samples)+(b.total_coupons*b.samples))/(reserved_instance_utilization_by_day.samples+b.samples),
      reserved_instance_utilization_by_day.used_coupons =
      ((reserved_instance_utilization_by_day.used_coupons * reserved_instance_utilization_by_day.samples)+(b.used_coupons*b.samples))/(reserved_instance_utilization_by_day.samples+b.samples),
      reserved_instance_utilization_by_day.samples=reserved_instance_utilization_by_day.samples+b.samples;

      update aggregation_meta_data set last_aggregated_by_day = max_created_time;


      /* MONTHLY AGGREGATION */

      if (last_aggregated_by_month_time='0000-00-00 00:00:00' OR last_aggregated_by_month_time IS NULL) THEN
        set last_aggregated_by_month_time = '1970-01-01 00:00:00';
      end if;

      set @last_monthly = last_aggregated_by_month_time;

      set @sql=concat('CREATE OR REPLACE VIEW reserved_instance_utilization_monthly_ins_vw AS
      SELECT month_key,
      DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
      id,
      region_id,
      availability_zone_id,
      business_account_id,
      avg(total_coupons) as total_coupons,
      avg(used_coupons) as used_coupons,
      count(*) as samples
      FROM reserved_instance_utilization_latest WHERE snapshot_time > \'',@last_monthly,'\' GROUP BY month_key');

      PREPARE stmt from @sql;
      EXECUTE stmt;
      DEALLOCATE PREPARE stmt;

      INSERT INTO reserved_instance_utilization_by_month (snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,month_key,samples)
      SELECT snapshot_time, id, region_id, availability_zone_id, business_account_id, total_coupons, used_coupons,month_key,samples
      FROM reserved_instance_utilization_monthly_ins_vw b
      ON DUPLICATE KEY UPDATE reserved_instance_utilization_by_month.total_coupons =
      ((reserved_instance_utilization_by_month.total_coupons * reserved_instance_utilization_by_month.samples)+(b.total_coupons*b.samples))/(reserved_instance_utilization_by_month.samples+b.samples),
      reserved_instance_utilization_by_month.used_coupons =
      ((reserved_instance_utilization_by_month.used_coupons * reserved_instance_utilization_by_month.samples)+(b.used_coupons*b.samples))/(reserved_instance_utilization_by_month.samples+b.samples),
      reserved_instance_utilization_by_month.samples=reserved_instance_utilization_by_month.samples+b.samples;

      update aggregation_meta_data set last_aggregated_by_month = max_created_time;

    END//
    DELIMITER ;

/* Recreate events to aggregate_ri scheduled event  */
/* Executes hourly, running the aggregate_reserved_instance_coverage, and aggregate_reserved_instance_utilization procedures */
DROP EVENT IF EXISTS aggregate_ri;
DELIMITER //
CREATE
EVENT aggregate_ri
  ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  call aggregate_reserved_instance_coverage;
  call aggregate_reserved_instance_utilization;
END //
DELIMITER ;

UPDATE version_info SET version=76.0 WHERE id=1;


DROP EVENT IF EXISTS aggregate_ri;
DELIMITER //
CREATE
EVENT aggregate_ri
  ON SCHEDULE EVERY 1 MINUTE
DO BEGIN
  call aggregate_reserved_instance_coverage;
  call aggregate_reserved_instance_utilization;
END //
DELIMITER ;

update version_info set version=1.9 where id=1;
