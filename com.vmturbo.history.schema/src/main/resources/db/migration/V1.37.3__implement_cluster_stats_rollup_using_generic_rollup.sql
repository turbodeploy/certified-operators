/*
 * Implement a procedure to rollup cluster_stats tables, using the generic_rollup stored proc.
 */
DROP procedure if exists cluster_stats_rollup;

DELIMITER //
create procedure cluster_stats_rollup(
    -- the table containing the records to be rolled up
    IN source_table CHAR(30),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(30),
    -- the snapshot time of the source records to be processed - must have zero milliseconds value
    IN snapshot_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME,
    -- whether the source table has a "samples" column (constant 1 is used in avg calculations
    -- if not)
    IN source_has_samples TINYINT,
    -- record count reported after upsert operation, as an output parameter
    OUT record_count INT
)
BEGIN
    -- columns to be set in the INSERT phase of the upsert
	SET @insert_columns=CONCAT_WS('#', '',
	    'recorded_on', 'internal_name', 'property_type', 'property_subtype', 'value', 'samples',
	    '');
	-- values for those columns
	SET @insert_values=CONCAT_WS('#', '',
        -- `recorded_on` in rollup table is always the provided rollup time
	    CONCAT("'", rollup_time, "'"),
	    'internal_name', 'property_type', 'property_subtype', 'value',
	    -- if source table has no `samples` column, use literal 1
	    IF(source_has_samples, 'samples', '1'),
	    '');
    -- columns to be updated in UPDATE phase, whenever it happens
    SET @update_columns = CONCAT_WS('#', '', 'value', 'samples',
        '');
    -- values for update columns
    SET @update_values = CONCAT_WS('#', '',
       rollup_avg('value', 'samples', rollup_table),
       rollup_incr('samples', rollup_table),
       '');

    CALL generic_rollup(source_table, rollup_table, snapshot_time, rollup_time, 'recorded_on',
        @insert_columns, @insert_values, @update_columns, @update_values, NULL, record_count);
END //
DELIMITER ;
