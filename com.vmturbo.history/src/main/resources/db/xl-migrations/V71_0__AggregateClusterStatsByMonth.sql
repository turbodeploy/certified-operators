


DELIMITER //

ALTER TABLE cluster_stats_by_day add aggregated tinyint(1) default 0;
//
CREATE INDEX cluster_aggr_idx on cluster_stats_by_day (aggregated);
//
ALTER TABLE cluster_stats_by_month add samples integer(11);
//


/*
 * Procedure aggregateClusterStats(), is called by aggregate_cluster_event MYSQL event every day as table changes every day
 * This procedure helps to rollup the values from the dataily table to the monthly table.
 */
DROP PROCEDURE if exists aggregateClusterStats;
//
CREATE PROCEDURE aggregateClusterStats()
  BEGIN

    /* MONTHLY CLUSTER STATS AGGREGATE */
    SET @monthly_insert_sql=concat('update cluster_stats_by_day a left join cluster_stats_by_month b 
  on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
  and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  set a.aggregated=2
  where b.recorded_on is null
  and b.internal_name is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=0');

    PREPARE stmt from @monthly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists cluster_stats_monthly_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_insert_view=concat('create view cluster_stats_monthly_ins_vw as
  select date_format(last_day(a.recorded_on),"%Y-%m-01") as recorded_on,
  a.internal_name,
  a.property_type,
  a.property_subtype,
  avg(a.value) as value 
  from cluster_stats_by_day a left join cluster_stats_by_month b 
  on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
  and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.recorded_on is null
  and b.internal_name is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=2 group by 1,2,3,4');


    PREPARE stmt from @monthly_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_inserts_sql=concat('insert into cluster_stats_by_month (recorded_on, internal_name, property_type,property_subtype,value,samples)
     select recorded_on, internal_name, property_type,property_subtype,value,1 from cluster_stats_monthly_ins_vw');

    PREPARE stmt from @perform_monthly_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update cluster_stats_by_day set aggregated=1 where aggregated=2');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



/* UPDATE EXISTING STATS */

    set @monthly_update_sql=concat('update cluster_stats_by_day a left join cluster_stats_by_month b
on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
and a.internal_name<=>b.internal_name
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=3
where a.aggregated=0');

	PREPARE stmt from @monthly_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists cluster_stats_monthly_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_update_view=concat('create view cluster_stats_monthly_upd_vw as
  select date_format(last_day(a.recorded_on),"%Y-%m-01") as recorded_on,
  a.internal_name,
  a.property_type,
  a.property_subtype,
  avg(a.value) as value,
  sum(a.value) as samples  
  from cluster_stats_by_day a left join cluster_stats_by_month b 
  on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
  and a.internal_name<=>b.internal_name 
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.recorded_on is not null 
  and b.internal_name is not null 
  and b.property_type is not null
  and b.property_subtype is not null
  and a.aggregated=3 group by 1,2,3,4');

    PREPARE stmt from @monthly_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_updates_sql=concat('update cluster_stats_by_month a, cluster_stats_monthly_upd_vw b
  set a.recorded_on=b.recorded_on,
  a.internal_name=b.internal_name,
  a.property_type=b.property_type,
  a.property_subtype=b.property_subtype,
  a.value=((a.value*a.samples)+(b.value*b.samples))/(a.samples+b.samples),
  a.samples=a.samples+b.samples  
  where a.recorded_on<=>b.recorded_on and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype');

    PREPARE stmt from @perform_monthly_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update cluster_stats_by_day set aggregated=1 where aggregated=3');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



  END;
//

/* Create aggregate_cluster_event MySQL event                      */
/* Triggers every 1 DAY                                            */

DROP EVENT IF EXISTS aggregate_cluster_event;
//

CREATE
EVENT aggregate_cluster_event
  ON SCHEDULE EVERY 10 MINUTE
DO BEGIN
  call aggregateClusterStats();
END //


/*
 * create purging procedures
 */

DROP PROCEDURE if exists purge_expired_days_cluster;
//


CREATE PROCEDURE purge_expired_days_cluster()
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM cluster_stats_by_day  
             where recorded_on<(select date_sub(current_timestamp, interval retention_period day)
             from retention_policies where policy_name="retention_days") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END;
//


DROP PROCEDURE if exists purge_expired_months_cluster;
//

CREATE PROCEDURE purge_expired_months_cluster()
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM cluster_stats_by_month
             where recorded_on<(select date_sub(current_timestamp, interval retention_period month)
             from retention_policies where name="retention_months") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END;
//

DROP EVENT IF EXISTS purge_expired_days_cluster;
//
DROP EVENT IF EXISTS purge_expired_months_cluster;
//


CREATE
EVENT purge_expired_days_cluster
  ON SCHEDULE EVERY 1 DAY
DO BEGIN
  call purge_expired_days_cluste();
END //

CREATE
EVENT purge_expired_months_cluster
  ON SCHEDULE EVERY 1 MONTH
DO BEGIN
  call purge_expired_months_cluster();
END //
DELIMITER ;

UPDATE version_info SET version=71.0 WHERE id=1;
