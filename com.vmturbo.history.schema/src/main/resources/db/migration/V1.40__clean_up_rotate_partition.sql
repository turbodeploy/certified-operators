/*
 * In the context of OM-53378, I put the rotate_partition stored proc under close scrutiny
 * to see whether I could find a way for the observed error to occur (duplicate partition
 * names in the same table). I didn't find a way, and my best guess is that this can arise
 * when multiple simultaneous executions end up running in parallel.
 *
 * To prevent that, the Java code that executes this stored proc now acquires an exclusive
 * lock on the table first, and only releases it after execution has completed.
 *
 * Aside from that, some minor cleanups have been performed, along with a fair bit of
 * debugging output that can be enabled by setting the `debug` variable to `TRUE` near
 * the top of the procedure definition. Also, to support testing, the caller can pass in
 * a datetime value, which will be used instead of current-time as a basis for partition
 * calculation.
 */
DROP FUNCTION IF EXISTS format_14_digit_datetime;

DELIMITER $$
# function to compute YYYYMMDDHHmmSS integer value for a given timestamp
CREATE FUNCTION format_14_digit_datetime(boundary DATETIME) RETURNS CHAR(20) DETERMINISTIC
BEGIN
    RETURN year(boundary)*10000000000 + month(boundary)*100000000 + day(boundary)*1000000
        + hour(boundary)*10000 + minute(boundary)*100 + second(boundary);
END $$
DELIMITER ;


DROP PROCEDURE IF EXISTS rotate_partition;

DELIMITER $$
CREATE DEFINER=CURRENT_USER PROCEDURE rotate_partition(IN stats_table CHAR(30), asOfTime DATETIME)
BEGIN
    DECLARE debug INT DEFAULT FALSE;

    DECLARE sql_statement varchar (1000);

    DECLARE done INT DEFAULT FALSE;

    # formatted datetime of the boundary for that we are iterating through
    # partitions name follow this pattern: beforeYYYYMMDDHHmmSS
    DECLARE part_fmt CHAR(20);
    # number of seconds into the past to be covered by active partitions - based on retention policies
    DECLARE num_seconds INT;
    # number of seconds into the future to be covered by active partitions other than the 'future' partition.
    # these partitions will receive new records until they are rotated into the past by a later execution of
    # this stored proc. This should be long enough to avoid putting new data in danger of being dropped, since
    # the future partition is always truncated by this stored proc.
    DECLARE num_seconds_for_future  INT;
    DECLARE retention_type CHAR(20);
    DECLARE idle_timeout_secs INT;

    # used in the continue handler in case we want to print out the error
    DECLARE E INT DEFAULT 0;
    DECLARE M TEXT DEFAULT NULL;

    # cursor for iterating over existing partitions, returning formatted partition boundary times
    # partitions with names that _do not_ begin with 'before' (including start and future) are excluded
    DECLARE cur1 CURSOR FOR (SELECT substring(partition_name, 7) FROM information_schema.partitions
        WHERE table_name = stats_table COLLATE utf8_unicode_ci AND table_schema = database()
            AND substring(PARTITION_NAME, 1, 6) = 'before' ORDER BY partition_name ASC);


    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    # we do not want to exit from this procedure in case we are dropping a partition that is not
    # existing (for whatever reason), specially when dropping and recreating the future partition
    DECLARE CONTINUE HANDLER FOR 1507 SET E='1507', M="Error in list of partitions to %s";
    DECLARE CONTINUE HANDLER FOR 1508 SET E='1508', M="Cannot remove all partitions, use DROP TABLE instead";

    /*
     On some testbeds, the app transactions(from history) were in idle
     state(from show processlist output) but were still holding the table locks.
     The "show engine innodb status" showed the transaction as Active and
     in "Cleaning up" state. The rotate partitions calls pile up as they are
     all waiting for the table locks to be released. As not partition is pruned,
     disk fills up. To get out of this situation, we have to kill the idle threads.
    */
    select timeout_seconds into idle_timeout_secs from idle_threads_policy;
    CALL kill_idle_threads(idle_timeout_secs);

    # capture start time for partitioning performance measurement
    set @partitioning_id = md5(now());
    set @start_of_partitioning=now();

    # check which table we need to rotate, and set variables for it
    set retention_type = substring_index(stats_table, '_', -1);
    CASE retention_type
      WHEN 'latest' then
        select retention_period into num_seconds from retention_policies where policy_name='retention_latest_hours';
        set num_seconds = num_seconds*(60*60);
        # set future to 3 hours
        set num_seconds_for_future = 3*(60*60);

      WHEN 'hour' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_hours';
        set num_seconds = num_seconds*(60*60);
        # set future to 8 hours
        set num_seconds_for_future = 8*(60*60);

      WHEN 'day' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_days';
        set num_seconds = num_seconds*(24*60*60);
        # set future to 3 days
        set num_seconds_for_future = 3*(24*60*60);

      WHEN 'month' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_months';
        set num_seconds = num_seconds*(31*24*60*60);
        # set future to 3 months
        set num_seconds_for_future = 3*(31*24*60*60);
    END CASE;



    # calculate what should be the most distant partition boundary in the past, and a formatted version of it
    # we normally do everything based on current time, but for testing we permit an alternative time to be specified
    set @current_utc := IFNULL(asOfTime, utc_timestamp);
    set @oldest_part := date_sub(@current_utc, INTERVAL num_seconds SECOND);
    set @oldest_part_fmt := format_14_digit_datetime(@oldest_part);

    # create future partitions for next X hours/days
    set @future_part := date_add(@current_utc, INTERVAL num_seconds_for_future SECOND);
    set @future_part_fmt := format_14_digit_datetime(@future_part);

    IF debug THEN
        SELECT concat('Retention range at ', utc_timestamp, ': [', @oldest_part_fmt, ' => ', @future_part_fmt, ']') AS '';
    END IF;

    # var to store the maximum partition date existing right now
    SET @max_part_fmt := 0;

    # iterate over the cursor and drop all the old partitions not needed anymore
    OPEN cur1;
    read_loop: LOOP
      FETCH cur1 INTO part_fmt;
      IF done THEN
        LEAVE read_loop;
      END IF;

      IF debug THEN
          SELECT concat('Existing Partition: ', part_fmt, ': ', IF(part_fmt < @oldest_part_fmt, 'drop', 'keep')) AS '';
      END IF;

      # if current partition is older than the last partition, drop it
      IF part_fmt < @oldest_part_fmt THEN
        set @sql_statement = concat('ALTER TABLE ', stats_table, ' DROP PARTITION before', part_fmt);
        PREPARE stmt FROM @sql_statement;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
      END IF;

      # set the current partition as the partition encountered with max date
      set @max_part_fmt := part_fmt;
    END LOOP;
    CLOSE cur1;

    # as a safety measure in order to not pile up data, and fill up the disk (which will result in stopping the whole platform)
    # we want to make sure that the future partition is empty, so that when we will trigger the repartition to create new ones,
    # the future will not contain any data, and hence we will not copy any data over to the new partitions (which will take
    # additional time). this means that if for some reason (we are slow, and cannot keep up with incoming data/aggregation)
    # the future partition contains some data, those data will be dropped. This choice has been done to maintain the platform
    # alive, even if that means to drop data, instead of keep all the data, but then die because of disk full.
    set @sql_statement = concat('ALTER TABLE ', stats_table, ' TRUNCATE PARTITION future;');
    PREPARE stmt from @sql_statement;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    # now we'll start adding partitions as needed, starting with the boundary time of the last existing partition encountered
    # we don't mess with existing partitions that were not dropped above, so in the normal case we'll be creating new partitions
    # with boundaries that are beyond that of the latest existing parittion. However, stats tables newly created during migration
    # start with only a 'start' partition at 0, and a 'future' partition at MAX_VALUE. In that case, we'll be creating a whole
    # set of new partitions.

    # check if our current starting point is earlier than the oldest partition that we want to have, and
    # if so, case use the oldest partition boundary as a starting point instead of any current paritition
    # this normally happens when we had no prior partitions, so @max_part_fmt = 0
    IF @max_part_fmt < @oldest_part_fmt THEN
        SET @max_part_fmt := @oldest_part_fmt;
    END IF;

    # calculate the time period between partitions that will yield 40 partitions from oldest to
    # future boundaries
    set @delta := (to_seconds(@future_part) - to_seconds(@oldest_part)) DIV 40;
    IF debug THEN SELECT concat('Partition delta: ', @delta) AS ''; END IF;

    # begin a sql stmt to reorganize the "future" partition by adding new partitions to it
    set @sql_statement = concat('alter table ', stats_table, ' REORGANIZE PARTITION future into (');

    # add delta to find earliest partition to be created
    set @add_part := date_add(@max_part_fmt, INTERVAL @delta SECOND);
    set @add_part_fmt := format_14_digit_datetime(@add_part);

    # continue adding the delta until we reach the future date
    WHILE @add_part_fmt <= @future_part_fmt DO

      IF debug THEN SELECT concat('Adding partition at boundary ', @add_part_fmt) AS ''; END IF;
      # append another partition
      set @sql_statement = concat(@sql_statement, '\n  partition before', @add_part_fmt, ' VALUES LESS THAN (to_seconds(\'', @add_part, '\')), ');

      # increase the date by another delta
      set @add_part := date_add(@add_part, INTERVAL @delta SECOND);
      set @add_part_fmt := format_14_digit_datetime(@add_part);

    END WHILE;

    # finish the alter partition statement
    set @sql_statement = concat(@sql_statement, '\n  partition future VALUES LESS THAN MAXVALUE);');
    IF debug THEN SELECT concat('Reorganize future: ',  @sql_statement) AS ''; END IF;
    # execute it
    PREPARE stmt from @sql_statement;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    IF debug THEN
        call summarize_stats_partitions(stats_table);
    END IF;

    # capture end time of partitioning.  Log timings to standard out, and appl_performance table
    SET @end_of_partitioning=now();
    SELECT concat(now(),'   INFO: PERFORMANCE: Partitioning ID: ',@partitioning_id, ', Partitioning of: ,',stats_table,',
        Start time: ',@start_of_partitioning,', End time: ',@end_of_partitioning,',
        Total Time: ', time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)),' seconds') AS '';

    INSERT INTO appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds)
     VALUES (@partitioning_id, 'REPARTITION', stats_table, 0, @start_of_partitioning, @end_of_partitioning,
        time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)));

END$$
DELIMITER ;

DROP PROCEDURE IF EXISTS summarize_stats_partitions;

DELIMITER $$
CREATE PROCEDURE summarize_stats_partitions(IN stats_table VARCHAR(30))
BEGIN
    # dump a handy table showing the time overall time range, and the now-current partition boundaries
    DROP TABLE IF EXISTS _summary;
    CREATE TEMPORARY TABLE _summary (row INTEGER AUTO_INCREMENT PRIMARY KEY, time DATETIME);
    INSERT INTO _summary SELECT NULL, str_to_date(substring(partition_name, 7), '%Y%m%d%H%i%s')
        FROM information_schema.partitions
        WHERE table_name = stats_table COLLATE utf8_unicode_ci AND table_schema = database()
        AND substring(partition_name, 1, 6) = 'before';

    SET @previous_time = NULL;
    SELECT time as 'boundary time', delta FROM (
        SELECT time, timediff(time, @previous_time) as delta,
             @previous_time := time as _junk
         FROM _summary ORDER BY row
    ) as x;

    SELECT count(*)-2 AS 'partition count' FROM _summary;

    DROP TABLE _summary;
END$$
DELIMITER ;
