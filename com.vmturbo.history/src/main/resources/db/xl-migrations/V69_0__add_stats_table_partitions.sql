/* Alter the entity tables to add partitioning */

# market
ALTER TABLE market_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE market_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE market_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE market_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# app
ALTER TABLE app_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE app_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE app_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE app_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# ch
ALTER TABLE ch_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE ch_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE ch_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE ch_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# cnt
ALTER TABLE cnt_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE cnt_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE cnt_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE cnt_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# dpod
ALTER TABLE dpod_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE dpod_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE dpod_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE dpod_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# ds
ALTER TABLE ds_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE ds_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE ds_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE ds_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# iom
ALTER TABLE iom_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE iom_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE iom_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE iom_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# pm
ALTER TABLE pm_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE pm_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE pm_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE pm_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# sc
ALTER TABLE sc_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE sc_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE sc_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE sc_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# sw
ALTER TABLE sw_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE sw_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE sw_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE sw_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# vdc
ALTER TABLE vdc_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vdc_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vdc_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vdc_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# vm
ALTER TABLE vm_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vm_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vm_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vm_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

# vpod
ALTER TABLE vpod_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vpod_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vpod_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE vpod_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE
);


/* Create a stored procedure for dropping old partitions and creating new ones */

DELIMITER //

DROP PROCEDURE IF EXISTS rotate_partition;
//

CREATE PROCEDURE rotate_partition(IN stats_table CHAR(30))
BEGIN

    # sql statement to be executed
    DECLARE sql_statement varchar (1000);

    DECLARE done INT DEFAULT FALSE;

    # name of the partition that we are iterating through
    # partitions name follow this pattern: beforeYYYYMMDDHHmmSS
    DECLARE part_name CHAR(22);

    # cursor for iterating over existing partitions
    # the select will return only the numeric part, removing the 'before' string
    # it will also remove the start and future partition from the result
    # the cursor will iterate over numbers/dates in increasing order
    DECLARE cur1 CURSOR FOR (select substring(PARTITION_NAME, 7) from information_schema.partitions where table_name=stats_table COLLATE utf8_general_ci and TABLE_SCHEMA=database() and substring(PARTITION_NAME, 7) <> '' order by PARTITION_NAME asc);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    # capture start time for partitioning performance measurement
    set @partitioning_id = md5(now());
    set @start_of_partitioning=now();

    # check which table we need to rotate, and set variables for it
    set @num_seconds = NULL;
    set @num_seconds_for_future = NULL;
    set @retention_type := (select substring_index(stats_table, '_', -1));
    CASE @retention_type
      WHEN 'latest' then
        set @num_seconds := (select retention_period from retention_policies where policy_name='retention_latest_hours')*60*60;
        # set future to 2 hours
        set @num_seconds_for_future := 2*60*60;

      WHEN 'hour' THEN
        set @num_seconds := (select retention_period from retention_policies where policy_name='retention_hours')*60*60;
        # set future to 8 hours
        set @num_seconds_for_future := 8*60*60;

      when 'day' then
        set @num_seconds := (select retention_period from retention_policies where policy_name='retention_days')*24*60*60;
        # set future to 3 days
        set @num_seconds_for_future := 3*24*60*60;

      when 'month' then
        set @num_seconds := (select retention_period from retention_policies where policy_name='retention_months')*31*24*60*60;
        # set future to 3 days
        set @num_seconds_for_future := 3*24*60*60;
    END CASE;



    #calculate what should be the last partition from the past
    set @last_part := (select date_sub(current_timestamp, INTERVAL @num_seconds SECOND));
    set @last_part_compact := YEAR(@last_part)*10000000000 + MONTH(@last_part)*100000000 + DAY(@last_part)*1000000 + hour(@last_part)*10000 + minute(@last_part)*100 + second(@last_part);

    # create future partitions for next X hours/days
    set @future_part := (select date_add(current_timestamp, INTERVAL @num_seconds_for_future SECOND));
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
    set @add_part := (select date_add(@max_part, INTERVAL @delta SECOND));
    set @add_part_compact := YEAR(@add_part)*10000000000 + MONTH(@add_part)*100000000 + DAY(@add_part)*1000000 + hour(@add_part)*10000 + minute(@add_part)*100 + second(@add_part);

    # continue adding the delta until we reach the future date
    WHILE @add_part_compact <= @future_part_compact DO

        # append another partition
        set @sql_statement = concat(@sql_statement, 'partition before', @add_part_compact, ' VALUES LESS THAN (to_seconds(\'', @add_part, '\')), ');

        # increase the date by another delta
        set @add_part := (select date_add(@add_part, INTERVAL @delta SECOND));
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
    select concat(now(),'   INFO: PERFORMANCE: Partitioning ID: ',@partitioning_id, ', Partitioning of: ,',stats_table,', Start time: ',@start_of_partitioning,', End time: ',@end_of_partitioning,', Total Time: ', time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@partitioning_id, 'REPARTITION',stats_table,0,@start_of_partitioning,@end_of_partitioning,time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)));

END //
DELIMITER ;


/* Create trigger_rotate_partition procedure
   Calls rotate_partition() procedure for each entity stats table */

DELIMITER //

DROP PROCEDURE IF EXISTS trigger_rotate_partition;
//

CREATE PROCEDURE trigger_rotate_partition()
BEGIN

  # market
  CALL rotate_partition('market_stats_latest');
  CALL rotate_partition('market_stats_by_day');
  CALL rotate_partition('market_stats_by_hour');
  CALL rotate_partition('market_stats_by_month');

  # app
  CALL rotate_partition('app_stats_latest');
  CALL rotate_partition('app_stats_by_day');
  CALL rotate_partition('app_stats_by_hour');
  CALL rotate_partition('app_stats_by_month');

  # ch
  CALL rotate_partition('ch_stats_latest');
  CALL rotate_partition('ch_stats_by_day');
  CALL rotate_partition('ch_stats_by_hour');
  CALL rotate_partition('ch_stats_by_month');

  # cnt
  CALL rotate_partition('cnt_stats_latest');
  CALL rotate_partition('cnt_stats_by_day');
  CALL rotate_partition('cnt_stats_by_hour');
  CALL rotate_partition('cnt_stats_by_month');

  # dpod
  CALL rotate_partition('dpod_stats_latest');
  CALL rotate_partition('dpod_stats_by_day');
  CALL rotate_partition('dpod_stats_by_hour');
  CALL rotate_partition('dpod_stats_by_month');

  # ds
  CALL rotate_partition('ds_stats_latest');
  CALL rotate_partition('ds_stats_by_day');
  CALL rotate_partition('ds_stats_by_hour');
  CALL rotate_partition('ds_stats_by_month');

  # iom
  CALL rotate_partition('iom_stats_latest');
  CALL rotate_partition('iom_stats_by_day');
  CALL rotate_partition('iom_stats_by_hour');
  CALL rotate_partition('iom_stats_by_month');

  # pm
  CALL rotate_partition('pm_stats_latest');
  CALL rotate_partition('pm_stats_by_day');
  CALL rotate_partition('pm_stats_by_hour');
  CALL rotate_partition('pm_stats_by_month');

  # sc
  CALL rotate_partition('sc_stats_latest');
  CALL rotate_partition('sc_stats_by_day');
  CALL rotate_partition('sc_stats_by_hour');
  CALL rotate_partition('sc_stats_by_month');

  # sw
  CALL rotate_partition('sw_stats_latest');
  CALL rotate_partition('sw_stats_by_day');
  CALL rotate_partition('sw_stats_by_hour');
  CALL rotate_partition('sw_stats_by_month');

  # vdc
  CALL rotate_partition('vdc_stats_latest');
  CALL rotate_partition('vdc_stats_by_day');
  CALL rotate_partition('vdc_stats_by_hour');
  CALL rotate_partition('vdc_stats_by_month');

  # vm
  CALL rotate_partition('vm_stats_latest');
  CALL rotate_partition('vm_stats_by_day');
  CALL rotate_partition('vm_stats_by_hour');
  CALL rotate_partition('vm_stats_by_month');

  # vpod
  CALL rotate_partition('vpod_stats_latest');
  CALL rotate_partition('vpod_stats_by_day');
  CALL rotate_partition('vpod_stats_by_hour');
  CALL rotate_partition('vpod_stats_by_month');

END //
DELIMITER ;


/* Trigger the partition creation right now */
CALL trigger_rotate_partition();


/* Drop the previous events for purging the stats tables
   this is done now by dropping the expired partitions  */

DROP EVENT IF EXISTS purge_expired_latest;
DROP EVENT IF EXISTS purge_expired_hours;
DROP EVENT IF EXISTS purge_expired_days;
DROP EVENT IF EXISTS purge_expired_months;


/* Create trigger_purge_expired procedure
   Calls purge_expired procedure for each entity stats table
   This is not used in any event, but is kept for backward compatibility, in case the
   partition drop will have problems */

DELIMITER //

DROP PROCEDURE IF EXISTS trigger_purge_expired;
//

CREATE PROCEDURE trigger_purge_expired()
BEGIN
  -- purge latest table records
  call purge_expired_latest('app');
  call purge_expired_latest('ch');
  call purge_expired_latest('cnt');
  call purge_expired_latest('dpod');
  call purge_expired_latest('ds');
  call purge_expired_latest('iom');
  call purge_expired_latest('pm');
  call purge_expired_latest('sc');
  call purge_expired_latest('sw');
  call purge_expired_latest('vdc');
  call purge_expired_latest('vm');
  call purge_expired_latest('vpod');

  -- purge _by_hour records
  call purge_expired_hours('app');
  call purge_expired_hours('ch');
  call purge_expired_hours('cnt');
  call purge_expired_hours('dpod');
  call purge_expired_hours('ds');
  call purge_expired_hours('iom');
  call purge_expired_hours('pm');
  call purge_expired_hours('sc');
  call purge_expired_hours('sw');
  call purge_expired_hours('vdc');
  call purge_expired_hours('vm');
  call purge_expired_hours('vpod');

  -- purge _by_days records
  call purge_expired_days('app');
  call purge_expired_days('ch');
  call purge_expired_days('cnt');
  call purge_expired_days('dpod');
  call purge_expired_days('ds');
  call purge_expired_days('iom');
  call purge_expired_days('pm');
  call purge_expired_days('sc');
  call purge_expired_days('sw');
  call purge_expired_days('vdc');
  call purge_expired_days('vm');
  call purge_expired_days('vpod');

  -- purge _by_months records
  call purge_expired_months('app');
  call purge_expired_months('ch');
  call purge_expired_months('cnt');
  call purge_expired_months('dpod');
  call purge_expired_months('ds');
  call purge_expired_months('iom');
  call purge_expired_months('pm');
  call purge_expired_months('sc');
  call purge_expired_months('sw');
  call purge_expired_months('vdc');
  call purge_expired_months('vm');
  call purge_expired_months('vpod');
END //
DELIMITER ;


/* Create aggregate_stats_event MySQL event                      */
/* Triggers every 5 minutes                                      */
/* Calls aggregate() procedure for each entity stats table group */
/* In addition, at the end call the rotate partition procedure   */

DELIMITER //

DROP EVENT IF EXISTS aggregate_stats_event;
//

CREATE
EVENT aggregate_stats_event
  ON SCHEDULE EVERY 10 MINUTE
DO BEGIN
  call market_aggregate('market');
  call aggregate('app');
  call aggregate('ch');
  call aggregate('cnt');
  call aggregate('dpod');
  call aggregate('ds');
  call aggregate('iom');
  call aggregate('pm');
  call aggregate('sc');
  call aggregate('sw');
  call aggregate('vdc');
  call aggregate('vm');
  call aggregate('vpod');

  CALL trigger_rotate_partition();
END //
DELIMITER ;


UPDATE version_info SET version=69.0 WHERE id=1;