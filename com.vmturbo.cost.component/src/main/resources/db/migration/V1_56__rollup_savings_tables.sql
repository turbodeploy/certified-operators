-- Hourly savings data for each applicable entity.
RENAME TABLE entity_savings_stats_hourly to entity_savings_by_hour;
ALTER TABLE entity_savings_by_hour CHANGE entity_id entity_oid BIGINT(20) NOT NULL;
ALTER TABLE entity_savings_by_hour MODIFY COLUMN stats_value DOUBLE NOT NULL;

-- Daily rolled up savings data (from hourly tables) for each applicable entity.
DROP TABLE IF EXISTS entity_savings_by_day;
CREATE TABLE entity_savings_by_day LIKE entity_savings_by_hour;
ALTER TABLE entity_savings_by_day ADD COLUMN samples INT(11) NOT NULL;

-- Corresponding monthly rollup table.
DROP TABLE IF EXISTS entity_savings_by_month;
CREATE TABLE entity_savings_by_month LIKE entity_savings_by_day;

-- Audit log for changes to entity states affected by various events (actions, power state etc.)
DROP TABLE IF EXISTS entity_savings_audit_events;
CREATE TABLE entity_savings_audit_events (
  -- VM/DB oid whose state we are tracking now, or have ever tracked before, even deleted ones.
  entity_oid BIGINT(20) NOT NULL,

  -- Type of entity event to track. Action recommendations/executions, topology events
  -- (creation/deletion/powerToggles etc.). Other possible events are group membership changes.
  event_type INT NOT NULL,

  -- Action OID if action event type, or Vendor event id for topology events if applicable, or ''.
  event_id VARCHAR(255) NOT NULL,

  -- Time when the event happened. Action event time, or when topology event (power down) happened.
  event_time DATETIME NOT NULL,

  -- Additional info ('' if not applicable) about event where applicable, stored as a JSON string.
  event_info MEDIUMTEXT NOT NULL CHECK (json_valid(`event_info`)),

  PRIMARY KEY (entity_oid, event_type, event_id, event_time)
);

-- Rollup convenience procedure for entity savings tables.
DROP procedure if exists entity_savings_rollup;

DELIMITER //
create procedure entity_savings_rollup (
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
DELIMITER ;

