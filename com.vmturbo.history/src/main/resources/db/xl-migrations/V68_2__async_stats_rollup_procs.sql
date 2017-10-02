DELIMITER //

DROP PROCEDURE IF EXISTS aggregate;

//

CREATE PROCEDURE aggregate(IN statspref CHAR(10))
  BEGIN

    /* HOURLY AGGREGATE */


    set @hourly_insert_sql=concat('update ',statspref,'_stats_latest a left join ',statspref,'_stats_by_hour b
on date_format(a.snapshot_time,"%Y-%m-%d %H:00:00")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.producer_uuid<=>b.producer_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
and a.relation<=>b.relation
and a.commodity_key<=>b.commodity_key
set a.aggregated=2
where b.snapshot_time is null
and b.uuid is null
and b.producer_uuid is null
and b.property_type is null
and b.property_subtype is null
and b.relation is null
and b.commodity_key is null
and a.aggregated=0');

    PREPARE stmt from @hourly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @hourly_update_sql=concat('update ',statspref,'_stats_latest a left join ',statspref,'_stats_by_hour b
on date_format(a.snapshot_time,"%Y-%m-%d %H:00:00")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.producer_uuid<=>b.producer_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
and a.relation<=>b.relation
and a.commodity_key<=>b.commodity_key
set a.aggregated=3
where a.aggregated=0');


    PREPARE stmt from @hourly_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @view_delete=concat('drop view if exists ',statspref,'_hourly_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @hourly_insert_view=concat('create view if not exists ',statspref,'_hourly_ins_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time,
  a.uuid,
  a.producer_uuid,
  a.property_type,
  a.property_subtype,
  a.relation,
  a.commodity_key,
  max(a.capacity) as capacity,
  min(a.min_value) as min_value,
  max(a.max_value) as max_value,
  avg(a.avg_value) as avg_value,
  count(*) as samples,
  count(*) as new_samples
  from ',statspref,'_stats_latest a left join ',statspref,'_stats_by_hour b
  on date_format(a.snapshot_time,"%Y-%m-%d %H:00:00")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.producer_uuid<=>b.producer_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  and a.relation<=>b.relation
  and a.commodity_key<=>b.commodity_key
  where b.snapshot_time is null
  and b.uuid is null
  and b.producer_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and b.relation is null
  and b.commodity_key is null
  and a.aggregated=2 group by 1,2,3,4,5,6,7');

    PREPARE stmt from @hourly_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_hourly_inserts_sql=concat('insert into ',statspref,'_stats_by_hour
     (snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,max_value,min_value,avg_value,samples,aggregated,new_samples)
     select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,max_value,min_value,avg_value,samples,0,new_samples from ',statspref,'_hourly_ins_vw');

    PREPARE stmt from @perform_hourly_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',statspref,'_stats_latest set aggregated=1 where aggregated=2');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @view_delete=concat('drop view if exists ',statspref,'_hourly_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @hourly_update_view=concat('create view if not exists ',statspref,'_hourly_upd_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time, a.uuid,a.producer_uuid,a.property_type,
  a.property_subtype,a.relation, a.commodity_key,max(a.capacity) as capacity,
  min(a.min_value) as min_value, max(a.max_value) as max_value, avg(a.avg_value) as avg_value, count(*) as samples, count(*) as new_samples
  from ',statspref,'_stats_latest a left join ',statspref,'_stats_by_hour b on date_format(a.snapshot_time,"%Y-%m-%d %H:00:00")<=>b.snapshot_time
  and a.uuid<=>b.uuid and a.producer_uuid<=>b.producer_uuid and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype and a.relation<=>b.relation and a.commodity_key<=>b.commodity_key
  where b.snapshot_time is not null and b.uuid is not null and b.producer_uuid is not null and b.property_type is not null
  and b.property_subtype is not null and b.relation is not null and b.commodity_key is not null and a.aggregated=3 group by 1,2,3,4,5,6,7');

    PREPARE stmt from @hourly_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_hourly_updates_sql=concat('update ',statspref,'_stats_by_hour a, ',statspref,'_hourly_upd_vw b
  set a.snapshot_time=b.snapshot_time,a.uuid=b.uuid,a.producer_uuid=b.producer_uuid, a.property_type=b.property_type,
  a.property_subtype=b.property_subtype, a.relation=b.relation, a.commodity_key=b.commodity_key,
  a.min_value=if(b.min_value<a.min_value, b.min_value, a.min_value),
  a.max_value=if(b.max_value>a.max_value,b.max_value,a.max_value),
  a.avg_value=((a.avg_value*a.samples)+(b.avg_value*b.new_samples))/(a.samples+b.new_samples),
  a.samples=a.samples+b.new_samples,
  a.new_samples=b.new_samples,
  a.aggregated=0
  where a.snapshot_time<=>b.snapshot_time and a.uuid<=>b.uuid
  and a.producer_uuid<=>b.producer_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  and a.relation<=>b.relation
  and a.commodity_key<=>b.commodity_key');

    PREPARE stmt from @perform_hourly_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',statspref,'_stats_latest set aggregated=1 where aggregated=3');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    /*
    drop view if exists ',statspref,'_ins_vw;
    drop view if exists ',statspref,'_upd_vw;
    */

    /* DAILY AGGREGATE */
    set @daily_insert_sql=concat('update ',statspref,'_stats_by_hour a left join ',statspref,'_stats_by_day b
on date_format(a.snapshot_time,"%Y-%m-%d 00:00:00")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.producer_uuid<=>b.producer_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
and a.relation<=>b.relation
and a.commodity_key<=>b.commodity_key
set a.aggregated=2
where b.snapshot_time is null
and b.uuid is null
and b.producer_uuid is null
and b.property_type is null
and b.property_subtype is null
and b.relation is null
and b.commodity_key is null
and a.aggregated=0');

    PREPARE stmt from @daily_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @daily_update_sql=concat('update ',statspref,'_stats_by_hour a left join ',statspref,'_stats_by_day b
on date_format(a.snapshot_time,"%Y-%m-%d 00:00:00")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.producer_uuid<=>b.producer_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
and a.relation<=>b.relation
and a.commodity_key<=>b.commodity_key
set a.aggregated=3
where a.aggregated=0');


    PREPARE stmt from @daily_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists ',statspref,'_daily_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @daily_insert_view=concat('create view if not exists ',statspref,'_daily_ins_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time,
  a.uuid,
  a.producer_uuid,
  a.property_type,
  a.property_subtype,
  a.relation,
  a.commodity_key,
  max(a.capacity) as capacity,
  min(a.min_value) as min_value,
  max(a.max_value) as max_value,
  avg(a.avg_value) as avg_value,
  sum(a.samples) as samples,
  sum(a.new_samples) as new_samples
  from ',statspref,'_stats_by_hour a left join ',statspref,'_stats_by_day b
  on date_format(a.snapshot_time,"%Y-%m-%d 00:00:00")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.producer_uuid<=>b.producer_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  and a.relation<=>b.relation
  and a.commodity_key<=>b.commodity_key
  where b.snapshot_time is null
  and b.uuid is null
  and b.producer_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and b.relation is null
  and b.commodity_key is null
  and a.aggregated=2 group by 1,2,3,4,5,6,7');

    PREPARE stmt from @daily_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_daily_inserts_sql=concat('insert into ',statspref,'_stats_by_day (snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,max_value,min_value,avg_value,samples,aggregated,new_samples)
     select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,max_value,min_value,avg_value,samples,0,new_samples from ',statspref,'_daily_ins_vw');

    PREPARE stmt from @perform_daily_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',statspref,'_stats_by_hour set aggregated=1 where aggregated=2');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @view_delete=concat('drop view if exists ',statspref,'_daily_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @daily_update_view=concat('create view if not exists ',statspref,'_daily_upd_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time, a.uuid,a.producer_uuid,a.property_type,a.property_subtype,a.relation, a.commodity_key,max(a.capacity) as capacity,
  min(a.min_value) as min_value, max(a.max_value) as max_value, avg(a.avg_value) as avg_value, sum(a.samples) as samples, sum(a.new_samples) as new_samples
  from ',statspref,'_stats_by_hour a left join ',statspref,'_stats_by_day b on date_format(a.snapshot_time,"%Y-%m-%d 00:00:00")<=>b.snapshot_time
  and a.uuid<=>b.uuid and a.producer_uuid<=>b.producer_uuid and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype and a.relation<=>b.relation and a.commodity_key<=>b.commodity_key
  where b.snapshot_time is not null and b.uuid is not null and b.producer_uuid is not null and b.property_type is not null
  and b.property_subtype is not null and b.relation is not null and b.commodity_key is not null and a.aggregated=3 group by 1,2,3,4,5,6,7');

    PREPARE stmt from @daily_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_daily_updates_sql=concat('update ',statspref,'_stats_by_day a, ',statspref,'_daily_upd_vw b
  set a.snapshot_time=b.snapshot_time,a.uuid=b.uuid,a.producer_uuid=b.producer_uuid, a.property_type=b.property_type,
  a.property_subtype=b.property_subtype, a.relation=b.relation, a.commodity_key=b.commodity_key,
  a.min_value=if(b.min_value<a.min_value, b.min_value, a.min_value),
  a.max_value=if(b.max_value>a.max_value,b.max_value,a.max_value),
  a.avg_value=((a.avg_value*a.samples)+(b.avg_value*b.new_samples))/(a.samples+b.new_samples),
  a.samples=a.samples+b.new_samples,
  a.aggregated=0
  where a.snapshot_time<=>b.snapshot_time and a.uuid<=>b.uuid
  and a.producer_uuid<=>b.producer_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  and a.relation<=>b.relation
  and a.commodity_key<=>b.commodity_key');



    PREPARE stmt from @perform_daily_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',statspref,'_stats_by_hour set aggregated=1 where aggregated=3');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    /*
    drop view if exists ',statspref,'_ins_vw;
    drop view if exists ',statspref,'_upd_vw;
    */


    /* MONTHLY AGGREGATE */
    set @monthly_insert_sql=concat('update ',statspref,'_stats_by_day a left join ',statspref,'_stats_by_month b
on date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.producer_uuid<=>b.producer_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
and a.relation<=>b.relation
and a.commodity_key<=>b.commodity_key
set a.aggregated=2
where b.snapshot_time is null
and b.uuid is null
and b.producer_uuid is null
and b.property_type is null
and b.property_subtype is null
and b.relation is null
and b.commodity_key is null
and a.aggregated=0');

    PREPARE stmt from @monthly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    set @monthly_update_sql=concat('update ',statspref,'_stats_by_day a left join ',statspref,'_stats_by_month b
on date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.producer_uuid<=>b.producer_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
and a.relation<=>b.relation
and a.commodity_key<=>b.commodity_key
set a.aggregated=3
where a.aggregated=0');


    PREPARE stmt from @monthly_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists ',statspref,'_monthly_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_insert_view=concat('create view if not exists ',statspref,'_monthly_ins_vw as
  select date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
  a.uuid,
  a.producer_uuid,
  a.property_type,
  a.property_subtype,
  a.relation,
  a.commodity_key,
  max(a.capacity) as capacity,
  min(a.min_value) as min_value,
  max(a.max_value) as max_value,
  avg(a.avg_value) as avg_value,
  sum(a.samples) as samples,
  sum(a.new_samples) as new_samples
  from ',statspref,'_stats_by_day a left join ',statspref,'_stats_by_month b
  on date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.producer_uuid<=>b.producer_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  and a.relation<=>b.relation
  and a.commodity_key<=>b.commodity_key
  where b.snapshot_time is null
  and b.uuid is null
  and b.producer_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and b.relation is null
  and b.commodity_key is null
  and a.aggregated=2 group by 1,2,3,4,5,6,7');

    PREPARE stmt from @monthly_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_inserts_sql=concat('insert into ',statspref,'_stats_by_month (snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,max_value,min_value,avg_value,samples,new_samples)
     select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,max_value,min_value,avg_value,samples,new_samples from ',statspref,'_monthly_ins_vw');

    PREPARE stmt from @perform_monthly_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',statspref,'_stats_by_day set aggregated=1 where aggregated=2');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @view_delete=concat('drop view if exists ',statspref,'_monthly_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_update_view=concat('create view if not exists ',statspref,'_monthly_upd_vw as
  select date_format(last_day(a.snapshot_time),"%Y-%m-%d %H:00:00") as snapshot_time, a.uuid,a.producer_uuid,a.property_type,a.property_subtype,a.relation, a.commodity_key,max(a.capacity) as capacity,
  min(a.min_value) as min_value, max(a.max_value) as max_value, avg(a.avg_value) as avg_value, sum(a.samples) as samples, sum(a.new_samples) as new_samples
  from ',statspref,'_stats_by_day a left join ',statspref,'_stats_by_month b on date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00")<=>b.snapshot_time
  and a.uuid<=>b.uuid and a.producer_uuid<=>b.producer_uuid and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype and a.relation<=>b.relation and a.commodity_key<=>b.commodity_key
  where b.snapshot_time is not null and b.uuid is not null and b.producer_uuid is not null and b.property_type is not null
  and b.property_subtype is not null and b.relation is not null and b.commodity_key is not null and a.aggregated=3 group by 1,2,3,4,5,6,7');

    PREPARE stmt from @monthly_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_updates_sql=concat('update ',statspref,'_stats_by_month a, ',statspref,'_monthly_upd_vw b
  set a.snapshot_time=b.snapshot_time,a.uuid=b.uuid,a.producer_uuid=b.producer_uuid, a.property_type=b.property_type,
  a.property_subtype=b.property_subtype, a.relation=b.relation, a.commodity_key=b.commodity_key,
  a.min_value=if(b.min_value<a.min_value, b.min_value, a.min_value),
  a.max_value=if(b.max_value>a.max_value,b.max_value,a.max_value),
  a.avg_value=((a.avg_value*a.samples)+(b.avg_value*b.new_samples))/(a.samples+b.new_samples),
  a.samples=a.samples+b.new_samples,
  a.new_samples=b.new_samples
  where a.snapshot_time<=>b.snapshot_time and a.uuid<=>b.uuid
  and a.producer_uuid<=>b.producer_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  and a.relation<=>b.relation
  and a.commodity_key<=>b.commodity_key');

    PREPARE stmt from @perform_monthly_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',statspref,'_stats_by_day set aggregated=1 where aggregated=3');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    /*
    drop view if exists ',statspref,'_ins_vw;
    drop view if exists ',statspref,'_upd_vw;
    */


  END;
//


/* Create aggregate_stats_event MySQL event                      */
/* Triggers every 5 minutes                                      */
/* Calls aggregate() procedure for each entity stats table group */

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
END //





-- XL MySQL Events to implement out of retention period historical data purge
-- Retention periods are defined in the retention_policies table.
-- Create the retention policies table if it does not already exist.
-- Populate it with default out-of-box values.  Indexes not required.



DROP TABLE IF EXISTS retention_policies;
//

CREATE TABLE retention_policies
(
  policy_name varchar(50),
  retention_period integer
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
//

-- Insert default retention periods
-- 2 hours for 'latest'
-- 72 hours
-- 60 days
-- 24 months


insert into retention_policies (policy_name,retention_period) values ('retention_latest_hours',2); //
insert into retention_policies (policy_name,retention_period) values ('retention_hours',72); //
insert into retention_policies (policy_name,retention_period) values ('retention_days', 60); //
insert into retention_policies (policy_name,retention_period) values ('retention_months', 24); //

/*
Create batch delete procedures.

*/

DROP PROCEDURE if exists purge_expired_latest;
//
CREATE PROCEDURE purge_expired_latest(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_latest
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour)
             from retention_policies where policy_name="retention_latest_hours") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END;
//


DROP PROCEDURE if exists purge_expired_hours;
//

CREATE PROCEDURE purge_expired_hours(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_hour
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour)
             from retention_policies where policy_name="retention_hours") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END;
//

DROP PROCEDURE if exists purge_expired_days;
//


CREATE PROCEDURE purge_expired_days(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_day
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period day)
             from retention_policies where policy_name="retention_days") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END;
//


DROP PROCEDURE if exists purge_expired_months;
//

CREATE PROCEDURE purge_expired_months(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_month
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period month)
             from retention_policies where policy_name="retention_months") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END;
//




-- The following MySQL events require the MySQL event scheduler to be enabled to trigger.
-- Enable the MySQL event scheduler by adding event_scheduler=ON to your MySQL configuration file my.cnf in the [mysqld] section.

-- NOTE:  The event scheduler is enabled by default in the XL build.


DROP EVENT IF EXISTS purge_expired_latest;
//
DROP EVENT IF EXISTS purge_expired_hours;
//
DROP EVENT IF EXISTS purge_expired_days;
//
DROP EVENT IF EXISTS purge_expired_months;

//

CREATE
EVENT purge_expired_hours
  ON SCHEDULE EVERY 1 HOUR
DO BEGIN

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

END //

CREATE
EVENT purge_expired_days
  ON SCHEDULE EVERY 1 DAY
DO BEGIN
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

END //

CREATE
EVENT purge_expired_months
  ON SCHEDULE EVERY 1 MONTH
DO BEGIN
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



UPDATE version_info SET version=68.2 WHERE id=1;