/*
 * Various changes to the cluster stats tables:
 * - The `recorded_on` TIMESTAMP columns now have a default 0 to avoid the automatic and very
 *   disruptive automatic defaulting to current_time on insert and update operations
 * - The `value` column is converted to DOUBLE type
 * - Added _latest and _by_hour tables
 * - All rollups have `samples` columns (_latest does not)
 * - `aggregated` column is dropped
 * Also replaced the existing stored procs that purge cluster_stats day/month tables
 * with a single stored proc that purges all the tables, using the purge_expired_data
 * utility proc.
 */

DROP PROCEDURE IF EXISTS _fix_existing_tables;
DELIMITER //
CREATE PROCEDURE _fix_existing_tables()
FIX_TABLES:BEGIN

    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        SELECT CONCAT('Failed to ', @operation, ' on table ', @table);

    -- Fix existing _by_day table
    SET @table='cluster_stats_by_day';
    SET @operation='add `samples` column';
    ALTER TABLE cluster_stats_by_day ADD COLUMN (samples INT(11));
    SET @operation='modify `recorded_on` column';
    ALTER TABLE cluster_stats_by_day MODIFY COLUMN recorded_on TIMESTAMP NOT NULL DEFAULT 0;
    SET @operation='modify `value` column';
    ALTER TABLE cluster_stats_by_day MODIFY COLUMN value DOUBLE NOT NULL;
    SET @operation='drop `aggregated` column';
    ALTER TABLE cluster_stats_by_day DROP COLUMN aggregated;
    -- Fix existing _by_month table
    SET @table='cluster_stats_by_month';
    SET @operation='modify `recorded_on` column';
    ALTER TABLE cluster_stats_by_month MODIFY COLUMN recorded_on TIMESTAMP NOT NULL DEFAULT 0;
    SET @operation='modify `value` column';
    ALTER TABLE cluster_stats_by_month MODIFY COLUMN value DOUBLE NOT NULL;
END //
CALL _fix_existing_tables() //
DROP PROCEDURE _fix_existing_tables //
DELIMITER ;


-- Create new _latest table patterned after _by_day, minus the `samples` column
DROP TABLE IF EXISTS cluster_stats_latest;
CREATE TABLE cluster_stats_latest LIKE cluster_stats_by_day;
ALTER TABLE cluster_stats_latest DROP COLUMN samples;

-- Create new _by_hour table patterned after _by_day
DROP TABLE IF EXISTS cluster_stats_by_hour;

CREATE TABLE cluster_stats_by_hour LIKE cluster_stats_by_day;

DROP EVENT IF EXISTS purge_expired_days_cluster;
DROP PROCEDURE IF EXISTS purge_expired_days_cluster;
DROP EVENT IF EXISTS purge_expired_months_cluster;
DROP PROCEDURE IF EXISTS purge_expired_months_cluster;
DROP EVENT IF EXISTS aggregate_cluster_event;
DROP PROCEDURE IF EXISTS aggregateClusterStats;

DROP PROCEDURE IF EXISTS purge_expired_cluster_stats;

DELIMITER //
CREATE PROCEDURE purge_expired_cluster_stats()
BEGIN
    CALL purge_expired_data('cluster_stats_latest', 'recorded_on', 'retention_latest_hours', 'hour');
    CALL purge_expired_data('cluster_stats_by_hour', 'recorded_on', 'retention_hours', 'hour');
    CALL purge_expired_data('cluster_stats_by_day', 'recorded_on', 'retention_days', 'day');
    CALL purge_expired_data('cluster_stats_by_month', 'recorded_on', 'retention_months', 'month');
END //

DELIMITER ;
