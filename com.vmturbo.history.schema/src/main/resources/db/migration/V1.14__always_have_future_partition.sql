/*
V1.7 migration added to the rotate_migrations procedure to ensure that the future partition is
always empty before we attempt to regorganize it, by dropping the future partition and then
immediately recreating it. This has the unfortunate effect of leaving the table temporarily
with nowhere to place records that do not fall into any other partitions, and this can lead to
insertion failures.

Here we address the problem by, instead, truncating the future partition. That achieves the
goal of ensuring the partition is empty, but it does not change the partition definitions on the
the table and so does not open us up to the race condition.

Changes in this migration are relative to the definition in V1.11 migration (which fixed a bug
in the original V1.7 migration) and is limited to the code between the "V1.14 START/END" comments
*/

DROP procedure IF EXISTS `rotate_partition`;

DELIMITER $$
CREATE DEFINER=CURRENT_USER PROCEDURE `rotate_partition`(IN stats_table CHAR(30))
BEGIN

    DECLARE sql_statement varchar (1000);

    DECLARE done INT DEFAULT FALSE;

    # name of the partition that we are iterating through
    # partitions name follow this pattern: beforeYYYYMMDDHHmmSS
    DECLARE part_name CHAR(22);
    DECLARE num_seconds INT;
    DECLARE num_seconds_for_future  INT;
    DECLARE retention_type CHAR(20);
    DECLARE idle_timeout_secs INT;

    # used in the continue handler in case we want to print out the error
    DECLARE E INT DEFAULT 0;
    DECLARE M TEXT DEFAULT NULL;

    # cursor for iterating over existing partitions
    # the select will return only the numeric part, removing the 'before' string
    # it will also remove the start and future partition from the result
    # the cursor will iterate over numbers/dates in increasing order
    DECLARE cur1 CURSOR FOR (select substring(PARTITION_NAME, 7) from information_schema.partitions
        where table_name=stats_table COLLATE utf8_unicode_ci and TABLE_SCHEMA=database()
        and substring(PARTITION_NAME, 7) <> '' order by PARTITION_NAME asc);

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
        set num_seconds = num_seconds *60*60;
        # set future to 2 hours
        set num_seconds_for_future = 2*60*60;

      WHEN 'hour' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_hours';
        set num_seconds = num_seconds *60*60;
        # set future to 8 hours
        set num_seconds_for_future = 8*60*60;

      WHEN 'day' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_days';
        set num_seconds = num_seconds *24*60*60;
        # set future to 3 days
        set num_seconds_for_future = 3*24*60*60;

      WHEN 'month' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_months';
        set num_seconds = num_seconds * 31*24*60*60;
        # set future to 3 months
        set num_seconds_for_future = 3*31*24*60*60;
    END CASE;



    #calculate what should be the last partition from the past
    set @last_part := date_sub(current_timestamp, INTERVAL num_seconds SECOND);
    set @last_part_compact := YEAR(@last_part)*10000000000 + MONTH(@last_part)*100000000 + DAY(@last_part)*1000000 + hour(@last_part)*10000 + minute(@last_part)*100 + second(@last_part);

    # create future partitions for next X hours/days
    set @future_part := date_add(current_timestamp, INTERVAL num_seconds_for_future SECOND);
    set @future_part_compact := YEAR(@future_part)*10000000000 + MONTH(@future_part)*100000000 + DAY(@future_part)*1000000 + hour(@future_part)*10000 + minute(@future_part)*100 + second(@future_part);

    # var to store the maximum partition date existing right now
    set @max_part := 0;

    # iterate over the cursor and drop all the old partitions not needed anymore
    OPEN cur1;
    read_loop: LOOP
      FETCH cur1 INTO part_name;
      IF done THEN
        LEAVE read_loop;
      END IF;

      # if current partition is older than the last partition, drop it
      IF part_name < @last_part_compact THEN
        set @sql_statement = concat('alter table ', stats_table, ' DROP PARTITION before',part_name);
        PREPARE stmt from @sql_statement;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
      END IF;

      # set the current partition as the partition encountered with max date
      set @max_part := part_name;
    END LOOP;
    CLOSE cur1;

    # V1.14 START
    # as a safety measure in order to not pile up data, and fill up the disk (which will result in stopping the whole platform)
    # we want to make sure that the future partition is empty, so that when we will trigger the repartition to create new ones,
    # the future will not contain any data, and hence we will not copy any data over to the new partitions (which will take
    # additional time). this means that if for some reason (we are slow, and cannot keep up with incoming data/aggregation)
    # the future partition contains some data, those data will be dropped. This choice has been done to maintain the platform
    # alive, even if that means to drop data, instead of keep all the data, but then die because of disk full.
    #
    set @sql_statement = concat('alter table ', stats_table, ' TRUNCATE PARTITION future;');
	PREPARE stmt from @sql_statement;
	EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    # V1.14 END

    # check if the maximum existing partition is even before the last partition that we need to have
    # in this case use the last partition as a starting point
    IF @max_part < @last_part_compact THEN
      set @max_part := @last_part_compact;
    END IF;

    # calculate the time period between partitions, given the number of total partitions
    # right now we are always trying to generate 40 partitions
    set @delta := (to_seconds(@future_part) - to_seconds(@last_part)) DIV 40;

    # reorganize the "future" partition by adding new partitions to it
    set @sql_statement = concat('alter table ', stats_table, ' REORGANIZE PARTITION future into (');

    # add the delta once
    set @add_part := date_add(@max_part, INTERVAL @delta SECOND);
    set @add_part_compact := YEAR(@add_part)*10000000000 + MONTH(@add_part)*100000000 + DAY(@add_part)*1000000 + hour(@add_part)*10000 + minute(@add_part)*100 + second(@add_part);

    # continue adding the delta until we reach the future date
    WHILE @add_part_compact <= @future_part_compact DO

      # append another partition
      set @sql_statement = concat(@sql_statement, 'partition before', @add_part_compact, ' VALUES LESS THAN (to_seconds(\'', @add_part, '\')), ');

      # increase the date by another delta
      set @add_part := date_add(@add_part, INTERVAL @delta SECOND);
      set @add_part_compact := YEAR(@add_part)*10000000000 + MONTH(@add_part)*100000000 + DAY(@add_part)*1000000 + hour(@add_part)*10000 + minute(@add_part)*100 + second(@add_part);

    END WHILE;

    # finish the alter partition statement
    set @sql_statement = concat(@sql_statement, ' partition future VALUES LESS THAN MAXVALUE);');
    # (debug only) and print it out
    #select @sql_statement;
    # execute it
    PREPARE stmt from @sql_statement;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    # capture end time of partitioning.  Log timings to standard out, and appl_performance table
    set @end_of_partitioning=now();
    select concat(now(),'   INFO: PERFORMANCE: Partitioning ID: ',@partitioning_id, ', Partitioning of: ,',stats_table,',
        Start time: ',@start_of_partitioning,', End time: ',@end_of_partitioning,',
        Total Time: ', time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)),' seconds') as '';

    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds)
     values (@partitioning_id, 'REPARTITION', stats_table, 0, @start_of_partitioning, @end_of_partitioning,
        time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)));

  END$$

DELIMITER ;
