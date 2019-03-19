/*
  - Create a new stored procedure called kill_idle_threads which kills
    any application threads which are in "Sleep" state but
    have an active outstanding transaction(typically due to not
    explicitly ending the transction via commit/rollback).
  - Add a new table to set the idle thread kill threshold.
  - The kill thread procedure is called in rotate_partition.
*/

DROP TABLE IF EXISTS `idle_threads_policy`;

CREATE TABLE `idle_threads_policy` (
  `id` enum("1") NOT NULL, # Enum to ensure only one entry in the table.
  `timeout_seconds` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

# Set defaul Idle transaction timeout to 15 minutes(900 seconds).
INSERT INTO `idle_threads_policy` VALUES ("1", 900);

DROP PROCEDURE IF EXISTS `kill_idle_threads`;

DELIMITER ;;
-- Kill any long running app transactions which are in "stuck" state
CREATE DEFINER=CURRENT_USER PROCEDURE `kill_idle_threads`(IN idle_timeout_secs INTEGER)
 BEGIN

    DECLARE tid CHAR(22);
    DECLARE done INT DEFAULT FALSE;

    DECLARE tid_cursor CURSOR FOR (select information_schema.processlist.ID from information_schema.processlist
        JOIN information_schema.innodb_trx ON information_schema.processlist.ID=information_schema.innodb_trx.trx_mysql_thread_id
        where information_schema.processlist.User='vmtplatform' and information_schema.processlist.Command='Sleep'
        and (to_seconds(current_timestamp)-to_seconds(information_schema.innodb_trx.trx_started)) > idle_timeout_secs);

    # This declaration is to keep mysql from printing an error message when there are no results
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    select concat(now(),' INFO:  Idle timeout: ', idle_timeout_secs) as '';
    OPEN tid_cursor;
    read_loop: LOOP
          FETCH tid_cursor INTO tid;
          IF done THEN
            LEAVE read_loop;
          END IF;
        select concat(now(),' INFO:  killing thread ', tid) as '';
        kill tid;
    END LOOP;
    CLOSE tid_cursor;
 END;;
DELIMITER ;

DROP PROCEDURE IF EXISTS `rotate_partition`;

/*
 Drop partitions with data which are older than the retention period.
 Create new partitions.
*/
DELIMITER ;;
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

    # cursor for iterating over existing partitions
    # the select will return only the numeric part, removing the 'before' string
    # it will also remove the start and future partition from the result
    # the cursor will iterate over numbers/dates in increasing order
    DECLARE cur1 CURSOR FOR (select substring(PARTITION_NAME, 7) from information_schema.partitions
        where table_name=stats_table COLLATE utf8_unicode_ci and TABLE_SCHEMA=database()
        and substring(PARTITION_NAME, 7) <> '' order by PARTITION_NAME asc);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

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
        # set future to 3 days
        set num_seconds_for_future = 3*24*60*60;
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
    # and print it out
    select @sql_statement;
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

  END ;;

DELIMITER ;
