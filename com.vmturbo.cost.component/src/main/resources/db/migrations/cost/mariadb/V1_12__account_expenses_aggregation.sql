/* DROP VIEWS */
DROP VIEW IF EXISTS account_expenses_hourly_ins_vw;
DROP VIEW IF EXISTS account_expenses_daily_ins_vw;
DROP VIEW IF EXISTS account_expenses_monthly_ins_vw;


DELIMITER //

/* Add primary keys to uniquely identify rows for aggregation */
/* These keys are a unique md5 checksum of all the non-value columns */
/* These keys uniquely identify an aggregate row in each of the hourly, daily and monthly tables */
/* They are used instead of creating a long composite key, which is comprised of all the columns in the table */
/* minus the value columns which are being aggregated.  A composite key index of this size will be essentially */
/* the zie of the original table */

ALTER TABLE account_expenses ADD hour_key varchar(32);
//
ALTER TABLE account_expenses ADD day_key varchar(32);
//
ALTER TABLE account_expenses ADD month_key varchar(32);
//
ALTER TABLE account_expenses DROP PRIMARY KEY;
//

CREATE INDEX aa_st_ae ON account_expenses(associated_account_id,snapshot_time,associated_entity_id);
//

/* Set hour, day, and month keys for existing databases */
UPDATE account_expenses SET hour_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
    ifnull(associated_account_id,'-'),
    ifnull(associated_entity_id,'-'),
    ifnull(entity_type,'-'),
    ifnull(currency,'-')
));
//
UPDATE account_expenses SET day_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(associated_account_id,'-'),
    ifnull(associated_entity_id,'-'),
    ifnull(entity_type,'-'),
    ifnull(currency,'-')
));
//
UPDATE account_expenses SET month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(associated_account_id,'-'),
    ifnull(associated_entity_id,'-'),
    ifnull(entity_type,'-'),
    ifnull(currency,'-')
));
//

/* Triggers to UPDATE hour, day and month keys on each new inserted row */
DROP TRIGGER IF EXISTS account_expenses_keys;
//
CREATE TRIGGER account_expenses_keys BEFORE INSERT ON account_expenses
  FOR EACH ROW
  BEGIN
      SET NEW.hour_key=md5(concat(
        ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
        ifnull(NEW.associated_account_id,'-'),
        ifnull(NEW.associated_entity_id,'-'),
        ifnull(NEW.entity_type,'-'),
        ifnull(NEW.currency,'-')
      ));

      SET NEW.day_key=md5(concat(
        ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
        ifnull(NEW.associated_account_id,'-'),
        ifnull(NEW.associated_entity_id,'-'),
        ifnull(NEW.entity_type,'-'),
        ifnull(NEW.currency,'-')
      ));

      SET NEW.month_key=md5(concat(
        ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
        ifnull(NEW.associated_account_id,'-'),
        ifnull(NEW.associated_entity_id,'-'),
        ifnull(NEW.entity_type,'-'),
        ifnull(NEW.currency,'-')
      ));
  END//

DROP TABLE IF EXISTS account_expenses_by_hour;
//
CREATE TABLE account_expenses_by_hour (
    associated_account_id              BIGINT          NOT NULL,
    snapshot_time                      TIMESTAMP       NOT NULL,
    associated_entity_id               BIGINT          NOT NULL,
    entity_type                        INT(11)         NOT NULL,
    currency                           INT(11)         NOT NULL,
    amount                             DECIMAL(20,7)   NOT NULL,
    hour_key                           VARCHAR(32)     NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (hour_key),
    INDEX exh_aist (associated_account_id, snapshot_time)
);
//


DROP TABLE IF EXISTS account_expenses_by_day;
//
CREATE TABLE account_expenses_by_day (
    associated_account_id              BIGINT          NOT NULL,
    snapshot_time                      TIMESTAMP       NOT NULL,
    associated_entity_id               BIGINT          NOT NULL,
    entity_type                        INT(11)         NOT NULL,
    currency                           INT(11)         NOT NULL,
    amount                             DECIMAL(20,7)   NOT NULL,
    day_key                            VARCHAR(32)     NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (day_key),
    INDEX exd_aist (associated_account_id, snapshot_time)
);
//

DROP TABLE IF EXISTS account_expenses_by_month;
//
CREATE TABLE account_expenses_by_month (
    associated_account_id              BIGINT          NOT NULL,
    snapshot_time                      TIMESTAMP       NOT NULL,
    associated_entity_id               BIGINT          NOT NULL,
    entity_type                        INT(11)         NOT NULL,
    currency                           INT(11)         NOT NULL,
    amount                             DECIMAL(20,7)   NOT NULL,
    month_key                          VARCHAR(32)     NOT NULL,
    samples                            INT(11)         NOT NULL,
    PRIMARY KEY (month_key),
    INDEX exm_aist (associated_account_id, snapshot_time)
);
//

/* Aggregation Procedures */

DROP PROCEDURE IF EXISTS aggregate_account_expenses;
//
/* Procedure:  aggregate_account_expenses */
/* Rolls up current entity cost data to hourly, daily and monthly tables simultaneous */
CREATE PROCEDURE aggregate_account_expenses()
  aggregate_exp:BEGIN
    DECLARE running_aggregations INT;
    DECLARE last_aggregated_time TIMESTAMP;
    DECLARE min_snapshot_time TIMESTAMP;
    DECLARE max_snapshot_time TIMESTAMP;
    DECLARE last_aggregated_by_hour_time TIMESTAMP;
    DECLARE last_aggregated_by_day_time TIMESTAMP;
    DECLARE last_aggregated_by_month_time TIMESTAMP;

    /* Find oldest and newest records in the account_expenses table */
    SELECT min(snapshot_time) INTO min_snapshot_time from account_expenses;
    SELECT max(snapshot_time) INTO max_snapshot_time from account_expenses;

    /* The aggregation_meta_data table contains the details of the most recent rollup timestamps processed in the hourly, daily and monthly tables */
    /* Used as a starting point for future aggregations.                                                                                           */
    SELECT last_aggregated INTO last_aggregated_time FROM aggregation_meta_data where aggregate_table = 'account_expenses';
    SELECT last_aggregated_by_hour INTO last_aggregated_by_hour_time FROM aggregation_meta_data where aggregate_table = 'account_expenses';
    SELECT last_aggregated_by_day INTO last_aggregated_by_day_time FROM aggregation_meta_data where aggregate_table = 'account_expenses';
    SELECT last_aggregated_by_month INTO last_aggregated_by_month_time FROM aggregation_meta_data where aggregate_table = 'account_expenses';

    /* If an entry for account_expenses does not exist in the aggreggation_meta_data table, create a new entry with default values */
    if (concat(last_aggregated_time,last_aggregated_by_hour_time,last_aggregated_by_day_time, last_aggregated_by_month_time)) is null then
      SELECT 'NO META DATA FOUND.  CREATING...';
      DELETE FROM aggregation_meta_data WHERE aggregate_table='account_expenses';
      INSERT INTO aggregation_meta_data (aggregate_table) VALUES ('account_expenses');
      SELECT min(snapshot_time) INTO last_aggregated_time FROM account_expenses;
    end if;

    /* HOURLY AGGREGATION */

    if (last_aggregated_by_hour_time='0000-00-00 00:00:00' OR last_aggregated_by_hour_time IS NULL) THEN
      set last_aggregated_by_hour_time = '1970-01-01 12:00:00';
    end if;
    set @last_hourly = last_aggregated_by_hour_time;

    /* Create a view containing the account_expenses rows which have not been aggregated to the hourly table */
    set @sql=concat('CREATE OR REPLACE VIEW account_expenses_hourly_ins_vw AS
    SELECT hour_key, day_key, month_key,
    TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d %H:00:00")) as snapshot_time,
    associated_account_id,
    associated_entity_id,
    entity_type,
    currency,
    avg(amount) as amount,
    count(*) as samples
    FROM account_expenses
    WHERE snapshot_time > \'',@last_hourly,'\' GROUP BY hour_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* Rollup the rows in the view to the account_expenses_by_hour table */
    /* If an existing entry exists, the new amount is averaged into the existing amount, otherwise a new hourly entry is created */
    INSERT INTO account_expenses_by_hour (snapshot_time, associated_account_id, associated_entity_id, entity_type, currency, amount,hour_key,day_key,month_key,samples)
    SELECT snapshot_time, associated_account_id, associated_entity_id, entity_type, currency, amount,hour_key,day_key,month_key,samples
    FROM account_expenses_hourly_ins_vw b
    ON DUPLICATE KEY UPDATE account_expenses_by_hour.amount =
    ((account_expenses_by_hour.amount * account_expenses_by_hour.samples)+(b.amount*b.samples))/(account_expenses_by_hour.samples+b.samples),
    account_expenses_by_hour.samples=account_expenses_by_hour.samples+b.samples;

    /* When the hourly aggregation is complete, update the aggregation_meta_data table with the most recent snapshot_time processed */
    /* This value is used as the starting point for the next scheduled aggregation cycel */
    update aggregation_meta_data set last_aggregated_by_hour = max_snapshot_time where aggregate_table = 'account_expenses';


    /* Repeat the same process for daily and monthly rollups */
    /* DAILY AGGREGATION */

    if (last_aggregated_by_day_time='0000-00-00 00:00:00' OR last_aggregated_by_day_time IS NULL) THEN
      set last_aggregated_by_day_time = '1970-01-01 12:00:00';
    end if;

    set @last_daily = last_aggregated_by_day_time;

    set @sql=concat('CREATE OR REPLACE VIEW account_expenses_daily_ins_vw AS
    SELECT day_key, month_key,
    TIMESTAMP(DATE_FORMAT(snapshot_time,"%Y-%m-%d 00:00:00")) as snapshot_time,
    associated_account_id,
    associated_entity_id,
    entity_type,
    currency,
    avg(amount) as amount,
    count(*) as samples
    FROM account_expenses
    WHERE snapshot_time > \'',@last_daily,'\' GROUP BY day_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    INSERT INTO account_expenses_by_day (snapshot_time, associated_account_id, associated_entity_id, entity_type, currency, amount,day_key,month_key,samples)
    SELECT snapshot_time, associated_account_id, associated_entity_id, entity_type, currency, amount,day_key,month_key,samples
    FROM account_expenses_daily_ins_vw b
    ON DUPLICATE KEY UPDATE account_expenses_by_day.amount =
    ((account_expenses_by_day.amount * account_expenses_by_day.samples)+(b.amount*b.samples))/(account_expenses_by_day.samples+b.samples),
    account_expenses_by_day.samples=account_expenses_by_day.samples+b.samples;

    update aggregation_meta_data set last_aggregated_by_day = max_snapshot_time where aggregate_table = 'account_expenses';


    /* MONTHLY AGGREGATION */

    if (last_aggregated_by_month_time='0000-00-00 00:00:00' OR last_aggregated_by_month_time IS NULL) THEN
      set last_aggregated_by_month_time = '1970-01-01 12:00:00';
    end if;

    set @last_monthly = last_aggregated_by_month_time;

    set @sql=concat('CREATE OR REPLACE VIEW account_expenses_monthly_ins_vw AS
    SELECT month_key,
    TIMESTAMP(DATE_FORMAT(last_day(snapshot_time),"%Y-%m-%d %H:00:00")) as snapshot_time,
    associated_account_id,
    associated_entity_id,
    entity_type,
    currency,
    avg(amount) as amount,
    count(*) as samples
    FROM account_expenses WHERE snapshot_time > \'',@last_monthly,'\' GROUP BY month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    INSERT INTO account_expenses_by_month (snapshot_time, associated_account_id, associated_entity_id, entity_type, currency, amount,month_key,samples)
    SELECT snapshot_time, associated_account_id, associated_entity_id, entity_type, currency, amount,month_key,samples
    FROM account_expenses_monthly_ins_vw b
    ON DUPLICATE KEY UPDATE account_expenses_by_month.amount =
    ((account_expenses_by_month.amount * account_expenses_by_month.samples)+(b.amount*b.samples))/(account_expenses_by_month.samples+b.samples),
    account_expenses_by_month.samples=account_expenses_by_month.samples+b.samples;

    update aggregation_meta_data set last_aggregated_by_month = max_snapshot_time where aggregate_table = 'account_expenses';

  END//


/* Recreate events to aggregate_expenses scheduled event  */
/* Executes hourly, running the aggregate_account_expenses procedures */

DROP EVENT IF EXISTS aggregate_expenses_evt;
//
CREATE
EVENT aggregate_expenses_evt
  ON SCHEDULE EVERY 1 HOUR
DO BEGIN
  call aggregate_account_expenses;
END //
