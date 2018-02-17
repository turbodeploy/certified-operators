-- add an 'update_time' column to aggregation_status to help determine stuck aggregations
ALTER TABLE aggregation_status ADD COLUMN created TIMESTAMP DEFAULT CURRENT_TIMESTAMP;


-- add roll-up colums for 'da' - Disk Array - tables
alter table da_stats_latest add aggregated boolean not null default false;
alter table da_stats_by_hour add aggregated boolean not null default false;
alter table da_stats_by_day add aggregated boolean not null default false;
alter table da_stats_by_month add aggregated boolean not null default false;

alter table da_stats_by_hour add samples integer;
alter table da_stats_by_day add samples integer;
alter table da_stats_by_month add samples integer;
alter table da_stats_by_hour add new_samples integer;
alter table da_stats_by_day add new_samples integer;
alter table da_stats_by_month add new_samples integer;



/* Add new app primary key columns.  */
alter table da_stats_latest add hour_key varchar(32);
alter table da_stats_latest add day_key varchar(32);
alter table da_stats_latest add month_key varchar(32);

alter table da_stats_by_hour add hour_key varchar(32);
alter table da_stats_by_hour add day_key varchar(32);
alter table da_stats_by_hour add month_key varchar(32);

alter table da_stats_by_day add day_key varchar(32);
alter table da_stats_by_day add month_key varchar(32);

alter table da_stats_by_month add month_key varchar(32);

/* Create primary keys. */
alter table da_stats_by_hour add primary key (hour_key,snapshot_time);
alter table da_stats_by_day add primary key (day_key,snapshot_time);
alter table da_stats_by_month add primary key (month_key,snapshot_time);

ALTER TABLE da_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE da_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE da_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE da_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

/* Create da trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
DELIMITER //

CREATE TRIGGER set_da_primary_keys BEFORE INSERT ON da_stats_latest
  FOR EACH ROW BEGIN
  set NEW.hour_key=md5(concat(
                           ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                           ifnull(NEW.uuid,'-'),
                           ifnull(NEW.producer_uuid,'-'),
                           ifnull(NEW.property_type,'-'),
                           ifnull(NEW.property_subtype,'-'),
                           ifnull(NEW.relation,'-'),
                           ifnull(NEW.commodity_key,'-')
                       ));

  SET NEW.day_key=md5(concat(
                          ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                          ifnull(NEW.uuid,'-'),
                          ifnull(NEW.producer_uuid,'-'),
                          ifnull(NEW.property_type,'-'),
                          ifnull(NEW.property_subtype,'-'),
                          ifnull(NEW.relation,'-'),
                          ifnull(NEW.commodity_key,'-')
                      ));

  SET NEW.month_key=md5(concat(
                            ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                            ifnull(NEW.uuid,'-'),
                            ifnull(NEW.producer_uuid,'-'),
                            ifnull(NEW.property_type,'-'),
                            ifnull(NEW.property_subtype,'-'),
                            ifnull(NEW.relation,'-'),
                            ifnull(NEW.commodity_key,'-')
                        ));
END//
DELIMITER ;


/* Initialize hour_key, day_key, month_key for da_ history tables where existing keys are null */
update da_stats_latest
set hour_key=md5(concat(
                     ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                     ifnull(uuid,'-'),
                     ifnull(producer_uuid,'-'),
                     ifnull(property_type,'-'),
                     ifnull(property_subtype,'-'),
                     ifnull(relation,'-'),
                     ifnull(commodity_key,'-')
                 )), day_key=md5(concat(
                                     ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                                     ifnull(uuid,'-'),
                                     ifnull(producer_uuid,'-'),
                                     ifnull(property_type,'-'),
                                     ifnull(property_subtype,'-'),
                                     ifnull(relation,'-'),
                                     ifnull(commodity_key,'-')
                                 )), month_key=md5(concat(
                                                       ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                                       ifnull(uuid,'-'),
                                                       ifnull(producer_uuid,'-'),
                                                       ifnull(property_type,'-'),
                                                       ifnull(property_subtype,'-'),
                                                       ifnull(relation,'-'),
                                                       ifnull(commodity_key,'-')
                                                   )) where hour_key is null;

update da_stats_by_hour
set hour_key=md5(concat(
                     ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                     ifnull(uuid,'-'),
                     ifnull(producer_uuid,'-'),
                     ifnull(property_type,'-'),
                     ifnull(property_subtype,'-'),
                     ifnull(relation,'-'),
                     ifnull(commodity_key,'-')
                 )), day_key=md5(concat(
                                     ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                                     ifnull(uuid,'-'),
                                     ifnull(producer_uuid,'-'),
                                     ifnull(property_type,'-'),
                                     ifnull(property_subtype,'-'),
                                     ifnull(relation,'-'),
                                     ifnull(commodity_key,'-')
                                 )), month_key=md5(concat(
                                                       ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                                       ifnull(uuid,'-'),
                                                       ifnull(producer_uuid,'-'),
                                                       ifnull(property_type,'-'),
                                                       ifnull(property_subtype,'-'),
                                                       ifnull(relation,'-'),
                                                       ifnull(commodity_key,'-')
                                                   )) where hour_key is null;

update da_stats_by_day
set day_key=md5(concat(
                    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                    ifnull(uuid,'-'),
                    ifnull(producer_uuid,'-'),
                    ifnull(property_type,'-'),
                    ifnull(property_subtype,'-'),
                    ifnull(relation,'-'),
                    ifnull(commodity_key,'-')
                )), month_key=md5(concat(
                                      ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                      ifnull(uuid,'-'),
                                      ifnull(producer_uuid,'-'),
                                      ifnull(property_type,'-'),
                                      ifnull(property_subtype,'-'),
                                      ifnull(relation,'-'),
                                      ifnull(commodity_key,'-')
                                  )) where day_key is null;

update da_stats_by_month
set month_key=md5(concat(
                      ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                      ifnull(uuid,'-'),
                      ifnull(producer_uuid,'-'),
                      ifnull(property_type,'-'),
                      ifnull(property_subtype,'-'),
                      ifnull(relation,'-'),
                      ifnull(commodity_key,'-')
                  )) where month_key is null;


-- add roll-up colums for 'lp' - Logical Pool - tables
alter table lp_stats_latest add aggregated boolean not null default false;
alter table lp_stats_by_hour add aggregated boolean not null default false;
alter table lp_stats_by_day add aggregated boolean not null default false;
alter table lp_stats_by_month add aggregated boolean not null default false;

alter table lp_stats_by_hour add samples integer;
alter table lp_stats_by_day add samples integer;
alter table lp_stats_by_month add samples integer;
alter table lp_stats_by_hour add new_samples integer;
alter table lp_stats_by_day add new_samples integer;
alter table lp_stats_by_month add new_samples integer;


/* Add new app primary key columns.  */
alter table lp_stats_latest add hour_key varchar(32);
alter table lp_stats_latest add day_key varchar(32);
alter table lp_stats_latest add month_key varchar(32);

alter table lp_stats_by_hour add hour_key varchar(32);
alter table lp_stats_by_hour add day_key varchar(32);
alter table lp_stats_by_hour add month_key varchar(32);

alter table lp_stats_by_day add day_key varchar(32);
alter table lp_stats_by_day add month_key varchar(32);

alter table lp_stats_by_month add month_key varchar(32);

/* Create lp primary keys. */
alter table lp_stats_by_hour add primary key (hour_key,snapshot_time);
alter table lp_stats_by_day add primary key (day_key,snapshot_time);
alter table lp_stats_by_month add primary key (month_key,snapshot_time);

ALTER TABLE lp_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE lp_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE lp_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE lp_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);


/* Create lp trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
DELIMITER //

CREATE TRIGGER set_lp_primary_keys BEFORE INSERT ON lp_stats_latest
  FOR EACH ROW BEGIN
  set NEW.hour_key=md5(concat(
                           ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                           ifnull(NEW.uuid,'-'),
                           ifnull(NEW.producer_uuid,'-'),
                           ifnull(NEW.property_type,'-'),
                           ifnull(NEW.property_subtype,'-'),
                           ifnull(NEW.relation,'-'),
                           ifnull(NEW.commodity_key,'-')
                       ));

  SET NEW.day_key=md5(concat(
                          ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                          ifnull(NEW.uuid,'-'),
                          ifnull(NEW.producer_uuid,'-'),
                          ifnull(NEW.property_type,'-'),
                          ifnull(NEW.property_subtype,'-'),
                          ifnull(NEW.relation,'-'),
                          ifnull(NEW.commodity_key,'-')
                      ));

  SET NEW.month_key=md5(concat(
                            ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                            ifnull(NEW.uuid,'-'),
                            ifnull(NEW.producer_uuid,'-'),
                            ifnull(NEW.property_type,'-'),
                            ifnull(NEW.property_subtype,'-'),
                            ifnull(NEW.relation,'-'),
                            ifnull(NEW.commodity_key,'-')
                        ));
END//
DELIMITER ;


/* Initialize hour_key, day_key, month_key for da_ history tables where existing keys are null */
update lp_stats_latest
set hour_key=md5(concat(
                     ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                     ifnull(uuid,'-'),
                     ifnull(producer_uuid,'-'),
                     ifnull(property_type,'-'),
                     ifnull(property_subtype,'-'),
                     ifnull(relation,'-'),
                     ifnull(commodity_key,'-')
                 )), day_key=md5(concat(
                                     ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                                     ifnull(uuid,'-'),
                                     ifnull(producer_uuid,'-'),
                                     ifnull(property_type,'-'),
                                     ifnull(property_subtype,'-'),
                                     ifnull(relation,'-'),
                                     ifnull(commodity_key,'-')
                                 )), month_key=md5(concat(
                                                       ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                                       ifnull(uuid,'-'),
                                                       ifnull(producer_uuid,'-'),
                                                       ifnull(property_type,'-'),
                                                       ifnull(property_subtype,'-'),
                                                       ifnull(relation,'-'),
                                                       ifnull(commodity_key,'-')
                                                   )) where hour_key is null;

update lp_stats_by_hour
set hour_key=md5(concat(
                     ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                     ifnull(uuid,'-'),
                     ifnull(producer_uuid,'-'),
                     ifnull(property_type,'-'),
                     ifnull(property_subtype,'-'),
                     ifnull(relation,'-'),
                     ifnull(commodity_key,'-')
                 )), day_key=md5(concat(
                                     ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                                     ifnull(uuid,'-'),
                                     ifnull(producer_uuid,'-'),
                                     ifnull(property_type,'-'),
                                     ifnull(property_subtype,'-'),
                                     ifnull(relation,'-'),
                                     ifnull(commodity_key,'-')
                                 )), month_key=md5(concat(
                                                       ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                                       ifnull(uuid,'-'),
                                                       ifnull(producer_uuid,'-'),
                                                       ifnull(property_type,'-'),
                                                       ifnull(property_subtype,'-'),
                                                       ifnull(relation,'-'),
                                                       ifnull(commodity_key,'-')
                                                   )) where hour_key is null;

update lp_stats_by_day
set day_key=md5(concat(
                    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                    ifnull(uuid,'-'),
                    ifnull(producer_uuid,'-'),
                    ifnull(property_type,'-'),
                    ifnull(property_subtype,'-'),
                    ifnull(relation,'-'),
                    ifnull(commodity_key,'-')
                )), month_key=md5(concat(
                                      ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                      ifnull(uuid,'-'),
                                      ifnull(producer_uuid,'-'),
                                      ifnull(property_type,'-'),
                                      ifnull(property_subtype,'-'),
                                      ifnull(relation,'-'),
                                      ifnull(commodity_key,'-')
                                  )) where day_key is null;

update lp_stats_by_month
set month_key=md5(concat(
                      ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                      ifnull(uuid,'-'),
                      ifnull(producer_uuid,'-'),
                      ifnull(property_type,'-'),
                      ifnull(property_subtype,'-'),
                      ifnull(relation,'-'),
                      ifnull(commodity_key,'-')
                  )) where month_key is null;


-- add roll-up colums for 'cpod' - Container Pod - tables
alter table cpod_stats_latest add aggregated boolean not null default false;
alter table cpod_stats_by_hour add aggregated boolean not null default false;
alter table cpod_stats_by_day add aggregated boolean not null default false;
alter table cpod_stats_by_month add aggregated boolean not null default false;

alter table cpod_stats_by_hour add samples integer;
alter table cpod_stats_by_day add samples integer;
alter table cpod_stats_by_month add samples integer;
alter table cpod_stats_by_hour add new_samples integer;
alter table cpod_stats_by_day add new_samples integer;
alter table cpod_stats_by_month add new_samples integer;

/* Add new app primary key columns.  */
alter table cpod_stats_latest add hour_key varchar(32);
alter table cpod_stats_latest add day_key varchar(32);
alter table cpod_stats_latest add month_key varchar(32);

alter table cpod_stats_by_hour add hour_key varchar(32);
alter table cpod_stats_by_hour add day_key varchar(32);
alter table cpod_stats_by_hour add month_key varchar(32);

alter table cpod_stats_by_day add day_key varchar(32);
alter table cpod_stats_by_day add month_key varchar(32);

alter table cpod_stats_by_month add month_key varchar(32);

/* Create app primary keys. */
alter table cpod_stats_by_hour add primary key (hour_key,snapshot_time);
alter table cpod_stats_by_day add primary key (day_key,snapshot_time);
alter table cpod_stats_by_month add primary key (month_key,snapshot_time);

ALTER TABLE cpod_stats_latest
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE cpod_stats_by_hour
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE cpod_stats_by_day
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

ALTER TABLE cpod_stats_by_month
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE
);

/* Create cpod trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
DELIMITER //

CREATE TRIGGER set_cpod_primary_keys BEFORE INSERT ON cpod_stats_latest
  FOR EACH ROW BEGIN
  set NEW.hour_key=md5(concat(
                           ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                           ifnull(NEW.uuid,'-'),
                           ifnull(NEW.producer_uuid,'-'),
                           ifnull(NEW.property_type,'-'),
                           ifnull(NEW.property_subtype,'-'),
                           ifnull(NEW.relation,'-'),
                           ifnull(NEW.commodity_key,'-')
                       ));

  SET NEW.day_key=md5(concat(
                          ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                          ifnull(NEW.uuid,'-'),
                          ifnull(NEW.producer_uuid,'-'),
                          ifnull(NEW.property_type,'-'),
                          ifnull(NEW.property_subtype,'-'),
                          ifnull(NEW.relation,'-'),
                          ifnull(NEW.commodity_key,'-')
                      ));

  SET NEW.month_key=md5(concat(
                            ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                            ifnull(NEW.uuid,'-'),
                            ifnull(NEW.producer_uuid,'-'),
                            ifnull(NEW.property_type,'-'),
                            ifnull(NEW.property_subtype,'-'),
                            ifnull(NEW.relation,'-'),
                            ifnull(NEW.commodity_key,'-')
                        ));
END//
DELIMITER ;


/* Initialize hour_key, day_key, month_key for da_ history tables where existing keys are null */
update cpod_stats_latest
set hour_key=md5(concat(
                     ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                     ifnull(uuid,'-'),
                     ifnull(producer_uuid,'-'),
                     ifnull(property_type,'-'),
                     ifnull(property_subtype,'-'),
                     ifnull(relation,'-'),
                     ifnull(commodity_key,'-')
                 )), day_key=md5(concat(
                                     ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                                     ifnull(uuid,'-'),
                                     ifnull(producer_uuid,'-'),
                                     ifnull(property_type,'-'),
                                     ifnull(property_subtype,'-'),
                                     ifnull(relation,'-'),
                                     ifnull(commodity_key,'-')
                                 )), month_key=md5(concat(
                                                       ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                                       ifnull(uuid,'-'),
                                                       ifnull(producer_uuid,'-'),
                                                       ifnull(property_type,'-'),
                                                       ifnull(property_subtype,'-'),
                                                       ifnull(relation,'-'),
                                                       ifnull(commodity_key,'-')
                                                   )) where hour_key is null;

update cpod_stats_by_hour
set hour_key=md5(concat(
                     ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
                     ifnull(uuid,'-'),
                     ifnull(producer_uuid,'-'),
                     ifnull(property_type,'-'),
                     ifnull(property_subtype,'-'),
                     ifnull(relation,'-'),
                     ifnull(commodity_key,'-')
                 )), day_key=md5(concat(
                                     ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                                     ifnull(uuid,'-'),
                                     ifnull(producer_uuid,'-'),
                                     ifnull(property_type,'-'),
                                     ifnull(property_subtype,'-'),
                                     ifnull(relation,'-'),
                                     ifnull(commodity_key,'-')
                                 )), month_key=md5(concat(
                                                       ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                                       ifnull(uuid,'-'),
                                                       ifnull(producer_uuid,'-'),
                                                       ifnull(property_type,'-'),
                                                       ifnull(property_subtype,'-'),
                                                       ifnull(relation,'-'),
                                                       ifnull(commodity_key,'-')
                                                   )) where hour_key is null;

update cpod_stats_by_day
set day_key=md5(concat(
                    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
                    ifnull(uuid,'-'),
                    ifnull(producer_uuid,'-'),
                    ifnull(property_type,'-'),
                    ifnull(property_subtype,'-'),
                    ifnull(relation,'-'),
                    ifnull(commodity_key,'-')
                )), month_key=md5(concat(
                                      ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                                      ifnull(uuid,'-'),
                                      ifnull(producer_uuid,'-'),
                                      ifnull(property_type,'-'),
                                      ifnull(property_subtype,'-'),
                                      ifnull(relation,'-'),
                                      ifnull(commodity_key,'-')
                                  )) where day_key is null;

update cpod_stats_by_month
set month_key=md5(concat(
                      ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
                      ifnull(uuid,'-'),
                      ifnull(producer_uuid,'-'),
                      ifnull(property_type,'-'),
                      ifnull(property_subtype,'-'),
                      ifnull(relation,'-'),
                      ifnull(commodity_key,'-')
                  )) where month_key is null;



-- add a function to compute the number of aggregation status tasks currently running
DELIMITER //
DROP FUNCTION IF EXISTS CHECKAGGR //
CREATE FUNCTION CHECKAGGR() RETURNS INTEGER
  BEGIN
    DECLARE AGGREGATING INTEGER DEFAULT 0;

    SELECT COUNT(*) INTO AGGREGATING FROM INFORMATION_SCHEMA.PROCESSLIST WHERE
      COMMAND != 'Sleep' AND INFO like '%aggreg%' and info not like '%INFORMATION_SCHEMA%';
    RETURN AGGREGATING;
  END;//

-- update the aggregation worker function to use this new check
/* AGGREGATE PROCEDURE */
DROP PROCEDURE IF EXISTS aggregate;
//
CREATE PROCEDURE aggregate(IN statspref CHAR(10))
  aggregate_proc:BEGIN
    DECLARE running_aggregations INT;
    DECLARE number_of_unaggregated_rows INT;
    DECLARE number_of_unaggregated_rows_hour INT;
    DECLARE number_of_unaggregated_rows_day INT;

    set @aggregation_id = md5(now());

    set sql_mode='';

    set running_aggregations = CHECKAGGR();
    if running_aggregations > 0 then
      select 'Aggregations already running... exiting rollup function.' as '';
      leave aggregate_proc;
    end if;

    insert into aggregation_status values ('Running', null, null);

    set @start_of_aggregation=now();

    /* HOURLY AGGREAGATION BEGIN */
    select concat(now(),' INFO:  Starting hourly aggregation ',statspref) as '';
    set @start_of_aggregation_hourly=now();

    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    set @sql=concat('update ',statspref,'_stats_latest a set a.aggregated=2 where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    /* select number_of_unaggregated_rows; */


    if number_of_unaggregated_rows = 0 then
      select 'Nothing to aggregate...' as '';
      delete from aggregation_status;
      leave aggregate_proc;
    end if;


    /* create view returning only unaggregated rows */
    set @sql=concat('create or replace view ',statspref,'_hourly_ins_vw as
      select date_format(a.snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time,
      a.uuid,
      a.producer_uuid,
      a.property_type,
      a.property_subtype,
      a.relation,
      a.commodity_key,
      a.hour_key,
      a.day_key,
      a.month_key,
      max(a.capacity) as capacity,
      min(a.min_value) as min_value,
      max(a.max_value) as max_value,
      avg(a.avg_value) as avg_value,
      count(*) as samples,
      count(*) as new_samples
      from ',statspref,'_stats_latest a where a.aggregated=2 group by hour_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    /* Aggregate hourly data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */

    set @sql=concat('insert into ',statspref,'_stats_by_hour
         (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,
         max_value,min_value,avg_value,samples,aggregated,new_samples,hour_key,day_key,month_key)
           select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,
           max_value,min_value,avg_value,samples,0,new_samples,hour_key,day_key,month_key from ',statspref,'_hourly_ins_vw b
      on duplicate key update ',
                    statspref,'_stats_by_hour.snapshot_time=b.snapshot_time,',
                    statspref,'_stats_by_hour.uuid=b.uuid,',
                    statspref,'_stats_by_hour.producer_uuid=b.producer_uuid,',
                    statspref,'_stats_by_hour.property_type=b.property_type,',
                    statspref,'_stats_by_hour.property_subtype=b.property_subtype,',
                    statspref,'_stats_by_hour.relation=b.relation,',
                    statspref,'_stats_by_hour.commodity_key=b.commodity_key,',
                    statspref,'_stats_by_hour.min_value=if(b.min_value<',statspref,'_stats_by_hour.min_value, b.min_value, ',statspref,'_stats_by_hour.min_value),',
                    statspref,'_stats_by_hour.max_value=if(b.max_value>',statspref,'_stats_by_hour.max_value,b.max_value,',statspref,'_stats_by_hour.max_value),',
                    statspref,'_stats_by_hour.avg_value=((',statspref,'_stats_by_hour.avg_value*',statspref,'_stats_by_hour.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_hour.samples+b.new_samples),',
                    statspref,'_stats_by_hour.samples=',statspref,'_stats_by_hour.samples+b.new_samples,',
                    statspref,'_stats_by_hour.new_samples=b.new_samples,',
                    statspref,'_stats_by_hour.hour_key=b.hour_key,',
                    statspref,'_stats_by_hour.day_key=b.day_key,',
                    statspref,'_stats_by_hour.month_key=b.month_key,',
                    statspref,'_stats_by_hour.aggregated=0');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('update ',statspref,'_stats_latest set aggregated=1 where aggregated=2');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation_hourly=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Hourly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows,', Start time: ',@start_of_aggregation_hourly,', End time: ',@end_of_aggregation_hourly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION HOURLY',statspref,number_of_unaggregated_rows,@start_of_aggregation_hourly,@end_of_aggregation_hourly,time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)));
    /* END  HOURLY */


    /* DAILY AGGREGATION BEGIN */

    set @start_of_aggregation_daily=now();

    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    select concat(now(),' INFO:  Starting daily aggregation ',statspref) as '';

    set @sql=concat('update ',statspref,'_stats_by_hour a
set a.aggregated=2
where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows_hour = ROW_COUNT();

    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @sql=concat('create or replace view ',statspref,'_daily_ins_vw as
select date_format(a.snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time,
a.uuid,
a.producer_uuid,
a.property_type,
a.property_subtype,
a.relation,
a.commodity_key,
a.day_key,
a.month_key,
max(a.capacity) as capacity,
min(a.min_value) as min_value,
max(a.max_value) as max_value,
avg(a.avg_value) as avg_value,
sum(samples) as samples,
sum(new_samples) as new_samples
from ',statspref,'_stats_by_hour a where a.aggregated=2 group by day_key');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    /* Aggregate daily data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */

    set @sql=concat('insert into ',statspref,'_stats_by_day
 (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,
 max_value,min_value,avg_value,samples,aggregated,new_samples,day_key,month_key)
   select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,
   max_value,min_value,avg_value,samples,0,new_samples,day_key,month_key from ',statspref,'_daily_ins_vw b
on duplicate key update
',statspref,'_stats_by_day.snapshot_time=b.snapshot_time,
',statspref,'_stats_by_day.uuid=b.uuid,
',statspref,'_stats_by_day.producer_uuid=b.producer_uuid,
',statspref,'_stats_by_day.property_type=b.property_type,
',statspref,'_stats_by_day.property_subtype=b.property_subtype,
',statspref,'_stats_by_day.relation=b.relation,
',statspref,'_stats_by_day.commodity_key=b.commodity_key,
',statspref,'_stats_by_day.min_value=if(b.min_value<',statspref,'_stats_by_day.min_value, b.min_value, ',statspref,'_stats_by_day.min_value),
',statspref,'_stats_by_day.max_value=if(b.max_value>',statspref,'_stats_by_day.max_value,b.max_value,',statspref,'_stats_by_day.max_value),
',statspref,'_stats_by_day.avg_value=((',statspref,'_stats_by_day.avg_value*',statspref,'_stats_by_day.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_day.samples+b.new_samples),
',statspref,'_stats_by_day.samples=',statspref,'_stats_by_day.samples+b.new_samples,
',statspref,'_stats_by_day.new_samples=b.new_samples,
',statspref,'_stats_by_day.day_key=b.day_key,
',statspref,'_stats_by_day.month_key=b.month_key,
',statspref,'_stats_by_day.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @sql=concat('update ',statspref,'_stats_by_hour set aggregated=1 where aggregated=2');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* select @sql; */
    set @end_of_aggregation_daily=now();

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Daily: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_hour,', Start time: ',@start_of_aggregation_daily,', End time: ',@end_of_aggregation_daily,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION DAILY',statspref,number_of_unaggregated_rows_hour,@start_of_aggregation_daily,@end_of_aggregation_daily,time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)));


    /* END DAILY AGGREGATION */


    /* MONTHLY AGGREGATION BEGIN */
    set @start_of_aggregation_monthly=now();
    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    select concat(now(),' INFO:  Starting monthly aggregation ',statspref) as '';

    set @sql=concat('update ',statspref,'_stats_by_day a
set a.aggregated=2
where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows_day = ROW_COUNT();

    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('create or replace view ',statspref,'_monthly_ins_vw as
select date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
a.uuid,
a.producer_uuid,
a.property_type,
a.property_subtype,
a.relation,
a.commodity_key,
a.month_key,
max(a.capacity) as capacity,
min(a.min_value) as min_value,
max(a.max_value) as max_value,
avg(a.avg_value) as avg_value,
sum(samples) as samples,
sum(new_samples) as new_samples
from ',statspref,'_stats_by_day a where a.aggregated=2 group by month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    /* Aggregate monthly data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */


    set @sql=concat('insert into ',statspref,'_stats_by_month
 (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,
 max_value,min_value,avg_value,samples,new_samples,month_key)
   select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,
   max_value,min_value,avg_value,samples,new_samples,month_key from ',statspref,'_monthly_ins_vw b
on duplicate key update
',statspref,'_stats_by_month.snapshot_time=b.snapshot_time,
',statspref,'_stats_by_month.uuid=b.uuid,
',statspref,'_stats_by_month.producer_uuid=b.producer_uuid,
',statspref,'_stats_by_month.property_type=b.property_type,
',statspref,'_stats_by_month.property_subtype=b.property_subtype,
',statspref,'_stats_by_month.relation=b.relation,
',statspref,'_stats_by_month.commodity_key=b.commodity_key,
',statspref,'_stats_by_month.min_value=if(b.min_value<',statspref,'_stats_by_month.min_value, b.min_value, ',statspref,'_stats_by_month.min_value),
',statspref,'_stats_by_month.max_value=if(b.max_value>',statspref,'_stats_by_month.max_value,b.max_value,',statspref,'_stats_by_month.max_value),
',statspref,'_stats_by_month.avg_value=((',statspref,'_stats_by_month.avg_value*',statspref,'_stats_by_month.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_month.samples+b.new_samples),
',statspref,'_stats_by_month.samples=',statspref,'_stats_by_month.samples+b.new_samples,
',statspref,'_stats_by_month.new_samples=b.new_samples,
',statspref,'_stats_by_month.month_key=b.month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('update ',statspref,'_stats_by_day set aggregated=1 where aggregated=2');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */
    set @end_of_aggregation_monthly=now();

    /* END MONTHLY AGGREGATION */


    set @sql=concat('delete from aggregation_status');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Monthly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation_monthly,', End time: ',@end_of_aggregation_monthly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION MONTHLY',statspref,number_of_unaggregated_rows_day,@start_of_aggregation_monthly,@end_of_aggregation_monthly,time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)));

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Total: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation,', End time: ',@end_of_aggregation,', Total Time: ', time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION TOTAL',statspref,number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,@start_of_aggregation,@end_of_aggregation,time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)));

  END
//

-- update trigger_rotate_partition procedure to skip rotation (pruning) if aggregation is running

DROP PROCEDURE IF EXISTS trigger_rotate_partition;
//

CREATE PROCEDURE trigger_rotate_partition()
  rotation_block: BEGIN

    SET @aggregation_in_progress = CHECKAGGR();
    IF @aggregation_in_progress > 0 THEN
      SELECT 'Aggregation is already running: skip rotation.' as '';
      LEAVE rotation_block;
    END IF;


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

    # cpod
    CALL rotate_partition('cpod_stats_latest');
    CALL rotate_partition('cpod_stats_by_day');
    CALL rotate_partition('cpod_stats_by_hour');
    CALL rotate_partition('cpod_stats_by_month');

    # dpod
    CALL rotate_partition('dpod_stats_latest');
    CALL rotate_partition('dpod_stats_by_day');
    CALL rotate_partition('dpod_stats_by_hour');
    CALL rotate_partition('dpod_stats_by_month');

    # da
    CALL rotate_partition('da_stats_latest');
    CALL rotate_partition('da_stats_by_day');
    CALL rotate_partition('da_stats_by_hour');
    CALL rotate_partition('da_stats_by_month');

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

    # lp
    CALL rotate_partition('lp_stats_latest');
    CALL rotate_partition('lp_stats_by_day');
    CALL rotate_partition('lp_stats_by_hour');
    CALL rotate_partition('lp_stats_by_month');

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



/* Redefine the  aggregate_stats_event MySQL event               */
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
  call aggregate('cpod');
  call aggregate('dpod');
  call aggregate('da');
  call aggregate('ds');
  call aggregate('iom');
  call aggregate('lp');
  call aggregate('pm');
  call aggregate('sc');
  call aggregate('sw');
  call aggregate('vdc');
  call aggregate('vm');
  call aggregate('vpod');

  CALL trigger_rotate_partition();
END //
DELIMITER ;

