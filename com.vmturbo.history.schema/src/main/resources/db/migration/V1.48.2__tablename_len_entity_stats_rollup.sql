/*
 * Here we update procedures: 'entity_stats_rollup' to support table with long names: 40 chars.
 */
DROP PROCEDURE IF EXISTS entity_stats_rollup;

DELIMITER //
CREATE DEFINER=CURRENT_USER PROCEDURE `entity_stats_rollup` (
    -- the table containing the records to be rolled up
    IN source_table CHAR(40),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(40),
    -- the snapshot time of the source records to be processed - must have zero milliseconds value
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
BEGIN
    -- Our extra condition... only include records that match our hour_key range
	SET @hour_cond = IF(hour_key_low IS NOT NULL,
	    IF(hour_key_high IS NOT NULL,
	        CONCAT('hour_key BETWEEN \'', hour_key_low, '\' AND \'', hour_key_high, '\''),
	        CONCAT('hour_key > \'', hour_key_low, '\'')),
	    CONCAT('hour_key < \'', hour_key_high, '\''));


    -- All the columns to participate in the INSERT phase
	SET @insert_columns=CONCAT_WS('#', '',
	    'snapshot_time', 'uuid', 'producer_uuid', 'property_type', 'property_subtype',
	    'relation', 'commodity_key', 'capacity', 'effective_capacity',
	    'max_value', 'min_value', 'avg_value', 'samples',
	    -- omit unwanted rollup key columns
	    IF(copy_hour_key, 'hour_key', NULL),
	    IF(copy_day_key, 'day_key', NULL),
	    IF(copy_month_key, 'month_key', NULL),
	    '');
	-- Values for all those columns. Most just come from the same columns in source table
	SET @insert_values=CONCAT_WS('#', '',
	    -- the `snapshot_time` is always our rollup time
	    CONCAT("'", rollup_time, "'"),
	    'uuid', 'producer_uuid', 'property_type', 'property_subtype',
	    'relation', 'commodity_key', 'capacity', 'effective_capacity',
	    'max_value', 'min_value', 'avg_value',
        -- `samples` is literal 1 if source doesn't have samples
	    IF(source_has_samples, 'samples', '1'),
	    -- omit unwanted rollup keys
	    IF(copy_hour_key, 'hour_key', NULL),
	    IF(copy_day_key, 'day_key', NULL),
	    IF(copy_month_key, 'month_key', NULL),
	    '');
    -- columns to receive updated values if in UPDATE phase, whenever it happens
    SET @update_columns = CONCAT_WS('#', '',
        'max_value', 'min_value', 'avg_value', 'samples',
        '');
    -- values for the updated columns
    SET @update_values = CONCAT_WS('#', '',
       rollup_max('max_value', rollup_table),
       rollup_min('min_value', rollup_table),
       rollup_avg('avg_value', 'samples', rollup_table),
       rollup_incr('samples', rollup_table),
       '');

    -- perform the upsert, and transmit record count back to caller
    CALL generic_rollup(source_table, rollup_table, snapshot_time, rollup_time, 'snapshot_time',
        @insert_columns, @insert_values, @update_columns, @update_values, @hour_cond, record_count);
END //
DELIMITER ;
