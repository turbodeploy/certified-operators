-- NOTE: This migration is a copy of the migration V1.24.4, repeated here so that it will be
-- executed by clients upgrading from a prior 7.21 branch release will execute the migration
-- when upgrading to the 7.21.0 release where 7.17 and 7.21 branches of history component were
-- unified.

/**
 * This migration redefines the stored procedure that performs rollups (aka aggregations) for the
 * entity stats tables.
 *
 * Major changes are:
 * * The entity_stats_rollup proc can be used to perform hourly, daily, or monthly rollups, whereas the
 *   former proc always performed all three.
 * * In order to meet the needs of all three in a single proc, a nubmer of parameters are used to
 *   guide choices made by the proc that depend on the rollup interval being processed.
 * * The proc is invoked from Java code in the history component not by a DB event.
 * * There is no attempt to prevent multiple concurrent executions. In fact, the Java code now
 *   carefulliy manages concurrent executions in order to improve performance.
 * * The proc no longer operates on a whole stats table at a time. Instead, it operats on a "shard"
 *   that is defined by a range of prefixes of the hour_key column.
 * * Rollups operates on records from the _latest table for hourly rollups, but from _by_hour table
 *   for daily and monthly rollups.
 */

DELIMITER //

DROP PROCEDURE IF EXISTS `aggregate`//
DROP PROCEDURE IF EXISTS entity_stats_rollup //
CREATE DEFINER=CURRENT_USER PROCEDURE `entity_stats_rollup` (
    -- the table containing the records to be rolled up
    IN source_table CHAR(30),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(30),
    -- the snapshot time of the source records to be processed
    IN snapshot_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME,
    -- exclusive lower bound on hour_key values for this shard (or null for the lowest shard)
    IN hour_key_low CHAR(32),
    -- exclusive upper bound on hour_key values for this shard (or null for highest shard)
    IN hour_key_high CHAR(32),
    -- whether the hour_key column should be copied from source recrods to rollup records
    IN copy_hour_key TINYINT,
    -- whether the day_key column should be copied from source records to rollup records
    IN copy_day_key TINYINT,
    -- whether the month_key column should be copied from source recors to rollup records
    IN copy_month_key TINYINT,
    -- whether the source table has a "samples" column (constant 1 is used in avg calculations
    -- if not)
    IN source_has_samples TINYINT,
    -- record count reported after upsert operation, as an output parameter
    OUT record_count INT
)

ENTITY_STATS_ROLLUP_PROC:BEGIN

	SET @source_table=source_table;
	SET @rollup_table=rollup_table;
	SET @rollup_time=rollup_time;
	SET @copy_hour_key=copy_hour_key;
	SET @copy_day_key=copy_day_key;
	SET @copy_month_key=copy_month_key;
	SET @source_has_samples=source_has_samples;
	SET @hour_cond = IF(hour_key_low IS NOT NULL,
	    IF(hour_key_high IS NOT NULL,
	        CONCAT('hour_key BETWEEN \'', hour_key_low, '\' AND \'', hour_key_high, '\''),
	        CONCAT('hour_key > \'', hour_key_low, '\'')),
	    CONCAT('hour_key < \'', hour_key_high, '\''));

	SET sql_mode='';

	SET @sql=CONCAT('INSERT INTO ',@rollup_table,'(
	    snapshot_time,
	    uuid,
	    producer_uuid,
	    property_type,
	    property_subtype,
	    relation,
	    commodity_key,
	    capacity,
	    effective_capacity,
	    max_value,
	    min_value,
	    avg_value,
	    samples',
	    IF(@copy_hour_key, ', hour_key', ''),
	    IF(@copy_day_key, ', day_key', ''),
	    IF(@copy_month_key, ', month_key', ''),'
	)
	SELECT
	    @rollup_time,
	    uuid,
	    producer_uuid,
	    property_type,
	    property_subtype,
	    relation,
	    commodity_key,
	    capacity,
	    effective_capacity,
	    max_value,
	    min_value,
	    avg_value,',
	    IF(@source_has_samples, 'samples', '1'),
	    IF (@copy_hour_key, ', hour_key', ''),
	    IF (@copy_day_key, ', day_key', ''),
	    IF (@copy_month_key, ', month_key', ''), '
	FROM ',@source_table,'
	WHERE snapshot_time = \'', snapshot_time, '\' AND ', @hour_cond, '
	ON DUPLICATE KEY UPDATE ',
	    'min_value = IF(VALUES(min_value)<',@rollup_table,'.min_value, VALUES(min_value),',@rollup_table,'.min_value),',
	    'max_value = IF(VALUES(max_value)>',@rollup_table,'.max_value,VALUES(max_value),',@rollup_table,'.max_value),',
	    'avg_value = ((',@rollup_table,'.avg_value * ',@rollup_table,'.samples) + VALUES(avg_value)+VALUES(samples)) / (',@rollup_table,'.samples + VALUES(samples)),',
	    'samples = ',@rollup_table,'.samples + VALUES(samples)');
	SELECT @sql;

	PREPARE stmt FROM @sql;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

	SET record_count = ROW_COUNT();

END //

DELIMITER ;
