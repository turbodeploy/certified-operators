/** Create a table to keep track of available timestamps for history data.
  *
  * There's a corresponding Java migration to initialize this table from existing stats data.
  */

DROP TABLE IF EXISTS available_timestamps;
CREATE TABLE available_timestamps (
    history_variety VARCHAR(20),
    time_frame CHAR(10),
    time_stamp TIMESTAMP NOT NULL DEFAULT 0, -- default 0 to suppress all automatic setting to current
    expires_at TIMESTAMP NOT NULL,
    PRIMARY KEY (history_variety, time_frame, time_stamp)
);

/** Also add chronological unit to retention policies data, rather than just embedding it in the policy name. */

-- first add the column, and ignore the error we'll get if it already exists, using a throw-away proc
DROP PROCEDURE IF EXISTS _;
DELIMITER //
CREATE PROCEDURE _()
BEGIN
  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
  ALTER TABLE retention_policies ADD COLUMN unit VARCHAR(20);
END //
DELIMITER ;
CALL _();
DROP PROCEDURE _;

-- now provide values for existing records
UPDATE retention_policies SET unit='HOURS' WHERE policy_name = 'retention_latest_hours';
UPDATE retention_policies SET unit='HOURS' WHERE policy_name = 'retention_hours';
UPDATE retention_policies SET unit='DAYS' WHERE policy_name = 'retention_days';
UPDATE retention_policies SET unit='MONTHS' WHERE policy_name = 'retention_months';
UPDATE retention_policies SET unit='DAYS' WHERE policy_name = 'systemload_retention_days';
UPDATE retention_policies SET unit='DAYS' WHERE policy_name = 'percentile_retention_days';
