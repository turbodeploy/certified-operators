/*
 * Create a generic rollup stored proc from which others can be easily implemented for specific
 * compatible rollup scenarios.
 *
 * The proc essentially executes an "upsert" (INSERT... SELECT... ON DUPLICATE KEY UPDATE...)
 * on a pair of tables: the source table, and the rollup table. The SELECT clause applies to the
 * source table to retrieve records with a particular timestamp value, and the selected records
 * are inserted or updated into corresponding records in the rollup table, with a timestamp that
 * represents the entire rolled up period.
 *
 * The caller specifies the following sets of information, each in the form of a single
 * string parameter:
 * - The list of rollup-table columns for the INSERT part of the upsert, i.e. the columns of the
 *   rollup table that will be populated when a new rollup record must be created.
 * - The list of expressions that will be used for the respective columns in the INSERT part of the
 *   upsert. Usually these are just corresponding source table column names, but they can also be
 *   literals, as is typical for the rollup timestamp and a column like `samples` to keep track of
 *   how many base records are represented in the rollup record, for averaging.
 * - The list of rollup record columns to be updated in the UPDATE part of the upsert - generally
 *   these columns that represent time series data, not time series identity.
 * - The list of expressions to which the respective update columns will be set in the UPDATE part
 *   of the upsert. We provide functions that can be used to construct expressions for common
 *
 *   aggregations like min, max, and avg, removing an easy source of errors in these procs.
 * The caller must also provide the name of the column that contains the record timestamp in the
 * source table, as this is used separately in constructing the primary WHERE clause condition for
 * the SELECT. The caller may also supply an additional WHERE condition that will be conjoined
 * (ANDed) with the primary condition.
 *
 * The `source_time` parameter must represent an exact second (i.e. fractional second part = 0).
 * The source records that will participate are those that fall in the one-second range starting
 * at that instant: `source_time` <= t < (`source_time` + 1).
 *
 * Column  and value lists are all single strings, each starting and ending with '#' and with '#'
 * separating individual entries. E.g. columns `a`, `b`, and `c` would be coded as '#a#b#c#'.
 */
DROP PROCEDURE IF EXISTS generic_rollup;

DELIMITER //
CREATE PROCEDURE generic_rollup(
    -- the table containing the records to be rolled up
    IN source_table CHAR(30),
    -- the table into which those records should be rolled up
    IN rollup_table CHAR(30),
    -- the time of the source records to be processed - must have zero milliseconds value
    IN source_time DATETIME,
    -- the rollup time for rollup records (i.e. their snapshot_time column values)
    IN rollup_time DATETIME,
    -- name of column in source table that represents the source record's timestamp
    IN source_time_column CHAR(30),
    -- hash-separated names of columns participating in the INSERT side of the upsert
    IN insert_columns VARCHAR(1000),
    -- hash-separated values for INSERT columns
    IN insert_values VARCHAR(1000),
    -- hash-separated names of columns updated in UPDATE side of upsert
    IN update_columns VARCHAR(1000),
    -- hash-separated values for update columns.
    IN update_values VARCHAR(1000),
    -- WHERE clause condition to be joined (by AND) with the timestamp field condition, or NULL
    IN cond VARCHAR(1000),
    -- output variable where record count will be recorded
    OUT record_count INT
)
BEGIN
	-- source_time param value will always be at 0 msec, but values in source table may not be
	SET @time_cond = CONCAT_WS(' ', source_time_column,
	    'BETWEEN', CONCAT("'", source_time, "'"),
	    'AND',  CONCAT("'", DATE_ADD(source_time, INTERVAL 1 SECOND), "'"));
    SET @updates = rollup_updates(update_columns, update_values, rollup_table);
    set @sql=CONCAT_WS(' ',
        'INSERT INTO', rollup_table, '(', REPLACE(TRIM('#' FROM insert_columns), '#', ', '), ')',
        'SELECT', REPLACE(TRIM('#' FROM insert_values), '#', ', '),
        'FROM', source_table,
        'WHERE', @time_cond, IF(cond IS NOT NULL, CONCAT_WS(' ', 'AND', cond), ''),
        'ON DUPLICATE KEY UPDATE',
        @updates);
    SELECT @sql as 'Rolllup SQL';
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    SET record_count = ROW_COUNT();
END //
DELIMITER ;


/*
 * Procedure to compute the column=value list for the UPDATE part of the upsert statement.
 */
DROP FUNCTION IF EXISTS rollup_updates;

DELIMITER //
CREATE FUNCTION rollup_updates(cols VARCHAR(1000), vals VARCHAR(1000), tbl VARCHAR(30))
RETURNS VARCHAR(1000)
BEGIN
    DECLARE update_string VARCHAR(1000);
    DECLARE col VARCHAR(30);
    DECLARE colspos INT;
    DECLARE val VARCHAR(1000);
    DECLARE valspos INT;
    SET update_string = '';
    SET cols = TRIM(LEADING '#' FROM cols);
    SET vals = TRIM(LEADING '#' FROM vals);
    SET colspos = LOCATE('#', cols);
    -- loop through all the columns
    WHILE colspos > 0 DO
        -- extract the next column name
        SET col = LEFT(cols, colspos - 1);
        -- and its corresponding expression
        SET valspos = LOCATE('#', vals);
        SET val = LEFT(vals, valspos - 1);
        -- compute the "column=value" string for this pair and append it our accumulating list
        SET update_string = CONCAT(update_string, CONCAT_WS(' ', col, '=', val), ', ');
        SET cols = SUBSTRING(cols, colspos + 1);
        SET vals = SUBSTRING(vals, valspos + 1);
        SET colspos = LOCATE('#', cols);
    END WHILE;
    -- drop the initial ", " from the first pair we added
    RETURN TRIM(', ' FROM update_string);
END //
DELIMITER ;

/*
 * Function to provide an expression that will calculate the minimum value between an existing
 * rollup table column value and the value presented for that column for INSERT.
 */
DROP FUNCTION IF EXISTS rollup_min;

DELIMITER //
CREATE FUNCTION rollup_min(name VARCHAR(30), rollup_table VARCHAR(30)) RETURNS VARCHAR(1000)
BEGIN
    -- CONCAT(rollup_table, '.', name) will yield existing value, while
    -- CONCAT('VALUES(', name, ')' will yield the new incoming value from the source table
    RETURN CONCAT('IF(', rollup_table, '.', name, '<VALUES(', name, '),',
        rollup_table, '.', name, ',VALUES(', name, '))');
END //
DELIMITER ;

/*
 * Function to provide an expression that will calculate the minimum value between an existing
 * rollup table column value and the value presented for that column for INSERT.
 */
DROP FUNCTION IF EXISTS rollup_max;

DELIMITER //
CREATE FUNCTION rollup_max(name VARCHAR(30), rollup_table VARCHAR(30)) RETURNS VARCHAR(1000)
BEGIN
    RETURN CONCAT('IF(', rollup_table, '.', name, '>VALUES(', name, '),',
        rollup_table, '.', name, ',VALUES(', name, '))');
END //
DELIMITER ;

/*
 * Function to provide an expression that will calculate the average of an existing rollup table
 * column value and an incoming source table value for the same column, each weighted by a
 * corresponding sample count.
 */
DROP FUNCTION IF EXISTS rollup_avg;

DELIMITER //
CREATE FUNCTION rollup_avg(name VARCHAR(30), samples_name VARCHAR(30), rollup_table VARCHAR(30))
RETURNS VARCHAR(1000)
BEGIN
    RETURN CONCAT(
        '(  (', rollup_table, '.', name, ' * ', rollup_table, '.', samples_name, ') + ',
        '   (VALUES(', name, ') * VALUES(', samples_name, '))) ',
        '/ (', rollup_table, '.', samples_name, ' + VALUES(', samples_name, '))');
END //
DELIMITER ;
DROP FUNCTION IF EXISTS rollup_incr;

/*
 * Function to provide an expression that will calculate add an incoming source table value to
 * the existing rollup table column of the same name.
 */
DELIMITER //
CREATE FUNCTION rollup_incr(name VARCHAR(30), rollup_table VARCHAR(30)) RETURNS VARCHAR(1000)
BEGIN
    RETURN CONCAT(rollup_table, '.', name, ' + VALUES(', name, ')');
END //
DELIMITER ;
