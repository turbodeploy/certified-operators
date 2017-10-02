
/* Create table to track status of aggregation process */

drop table if exists aggregation_status;
create table  aggregation_status (status varchar(32));

/* Create table to track historical performance stats */

drop table if exists appl_performance;
create table appl_performance(
     log_time timestamp default current_timestamp,
     id varchar(32),
     performance_type varchar(64),
     entity_type varchar(64),
     rows_aggregated int,
     start_time datetime,
     end_time datetime,
     runtime_seconds int);

/* Prepare app historical stats tables */

/* Drop not required aggregated column index */
alter table app_stats_latest drop index app_aggr_idx;
alter table ch_stats_latest drop index ch_aggr_idx;
alter table cnt_stats_latest drop index cnt_aggr_idx;
alter table dpod_stats_latest drop index dpod_aggr_idx;
alter table ds_stats_latest drop index ds_aggr_idx;
alter table iom_stats_latest drop index iom_aggr_idx;
alter table pm_stats_latest drop index pm_aggr_idx;
alter table sc_stats_latest drop index sc_aggr_idx;
alter table sw_stats_latest drop index sw_aggr_idx;
alter table vdc_stats_latest drop index vdc_aggr_idx;
alter table vm_stats_latest drop index vm_aggr_idx;
alter table vpod_stats_latest drop index vpod_aggr_idx;

alter table app_stats_by_hour drop index app_aggr_hour_idx;
alter table ch_stats_by_hour drop index ch_aggr_hour_idx;
alter table cnt_stats_by_hour drop index cnt_aggr_hour_idx;
alter table dpod_stats_by_hour drop index dpod_aggr_hour_idx;
alter table ds_stats_by_hour drop index ds_aggr_hour_idx;
alter table iom_stats_by_hour drop index iom_aggr_hour_idx;
alter table pm_stats_by_hour drop index pm_aggr_hour_idx;
alter table sc_stats_by_hour drop index sc_aggr_hour_idx;
alter table sw_stats_by_hour drop index sw_aggr_hour_idx;
alter table vdc_stats_by_hour drop index vdc_aggr_hour_idx;
alter table vm_stats_by_hour drop index vm_aggr_hour_idx;
alter table vpod_stats_by_hour drop index vpod_aggr_hour_idx;

alter table app_stats_by_day drop index app_aggr_day_idx;
alter table ch_stats_by_day drop index ch_aggr_day_idx;
alter table cnt_stats_by_day drop index cnt_aggr_day_idx;
alter table dpod_stats_by_day drop index dpod_aggr_day_idx;
alter table ds_stats_by_day drop index ds_aggr_day_idx;
alter table iom_stats_by_day drop index iom_aggr_day_idx;
alter table pm_stats_by_day drop index pm_aggr_day_idx;
alter table sc_stats_by_day drop index sc_aggr_day_idx;
alter table sw_stats_by_day drop index sw_aggr_day_idx;
alter table vdc_stats_by_day drop index vdc_aggr_day_idx;
alter table vm_stats_by_day drop index vm_aggr_day_idx;
alter table vpod_stats_by_day drop index vpod_aggr_day_idx;


/* Drop existing app stats compound indexes, which are no longer required */
alter table app_stats_latest drop index app_latest_idx;
alter table app_stats_by_hour drop index app_stats_by_hour_idx;
alter table app_stats_by_day drop index app_stats_by_day_idx;
alter table app_stats_by_month drop index app_stats_by_mth_idx;

/* Add new app primary key columns.  */
alter table app_stats_latest add hour_key varchar(32);
alter table app_stats_latest add day_key varchar(32);
alter table app_stats_latest add month_key varchar(32);

alter table app_stats_by_hour add hour_key varchar(32);
alter table app_stats_by_hour add day_key varchar(32);
alter table app_stats_by_hour add month_key varchar(32);

alter table app_stats_by_day add day_key varchar(32);
alter table app_stats_by_day add month_key varchar(32);

alter table app_stats_by_month add month_key varchar(32);

/* Create app primary keys. */
alter table app_stats_by_hour add primary key (hour_key,snapshot_time);
alter table app_stats_by_day add primary key (day_key,snapshot_time);
alter table app_stats_by_month add primary key (month_key,snapshot_time);


/* Prepare ch historical stats tables */

/* Drop existing ch stats compound indexes, which are no longer required */
alter table ch_stats_latest drop index ch_latest_idx;
alter table ch_stats_by_hour drop index ch_stats_by_hour_idx;
alter table ch_stats_by_day drop index ch_stats_by_day_idx;
alter table ch_stats_by_month drop index ch_stats_by_mth_idx;

/* Add new ch primary key columns.  */
alter table ch_stats_latest add hour_key varchar(32);
alter table ch_stats_latest add day_key varchar(32);
alter table ch_stats_latest add month_key varchar(32);

alter table ch_stats_by_hour add hour_key varchar(32);
alter table ch_stats_by_hour add day_key varchar(32);
alter table ch_stats_by_hour add month_key varchar(32);

alter table ch_stats_by_day add day_key varchar(32);
alter table ch_stats_by_day add month_key varchar(32);

alter table ch_stats_by_month add month_key varchar(32);

/* Create ch primary keys. */
alter table ch_stats_by_hour add primary key (hour_key,snapshot_time);
alter table ch_stats_by_day add primary key (day_key,snapshot_time);
alter table ch_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare cnt historical stats tables */

/* Drop existing cnt stats compound indexes, which are no longer required */
alter table cnt_stats_latest drop index cnt_latest_idx;
alter table cnt_stats_by_hour drop index cnt_stats_by_hour_idx;
alter table cnt_stats_by_day drop index cnt_stats_by_day_idx;
alter table cnt_stats_by_month drop index cnt_stats_by_mth_idx;

/* Add new cnt primary key columns.  */
alter table cnt_stats_latest add hour_key varchar(32);
alter table cnt_stats_latest add day_key varchar(32);
alter table cnt_stats_latest add month_key varchar(32);

alter table cnt_stats_by_hour add hour_key varchar(32);
alter table cnt_stats_by_hour add day_key varchar(32);
alter table cnt_stats_by_hour add month_key varchar(32);

alter table cnt_stats_by_day add day_key varchar(32);
alter table cnt_stats_by_day add month_key varchar(32);

alter table cnt_stats_by_month add month_key varchar(32);

/* Create cnt primary keys. */
alter table cnt_stats_by_hour add primary key (hour_key,snapshot_time);
alter table cnt_stats_by_day add primary key (day_key,snapshot_time);
alter table cnt_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare dpod historical stats tables */

/* Drop existing dpod stats compound indexes, which are no longer required */
alter table dpod_stats_latest drop index dpod_latest_idx;
alter table dpod_stats_by_hour drop index dpod_stats_by_hour_idx;
alter table dpod_stats_by_day drop index dpod_stats_by_day_idx;
alter table dpod_stats_by_month drop index dpod_stats_by_mth_idx;

/* Add new dpod primary key columns.  */
alter table dpod_stats_latest add hour_key varchar(32);
alter table dpod_stats_latest add day_key varchar(32);
alter table dpod_stats_latest add month_key varchar(32);

/* Create dpod primary keys. */
alter table dpod_stats_by_hour add hour_key varchar(32);
alter table dpod_stats_by_hour add day_key varchar(32);
alter table dpod_stats_by_hour add month_key varchar(32);

alter table dpod_stats_by_day add day_key varchar(32);
alter table dpod_stats_by_day add month_key varchar(32);

alter table dpod_stats_by_month add month_key varchar(32);

/* Create ds primary keys. */
alter table dpod_stats_by_hour add primary key (hour_key,snapshot_time);
alter table dpod_stats_by_day add primary key (day_key,snapshot_time);
alter table dpod_stats_by_month add primary key (month_key,snapshot_time);
/* Prepare ds historical stats tables */

/* Drop existing ds stats compound indexes, which are no longer required */
alter table ds_stats_latest drop index ds_latest_idx;
alter table ds_stats_by_hour drop index ds_stats_by_hour_idx;
alter table ds_stats_by_day drop index ds_stats_by_day_idx;
alter table ds_stats_by_month drop index ds_stats_by_mth_idx;

/* Add new ds primary key columns.  */
alter table ds_stats_latest add hour_key varchar(32);
alter table ds_stats_latest add day_key varchar(32);
alter table ds_stats_latest add month_key varchar(32);

alter table ds_stats_by_hour add hour_key varchar(32);
alter table ds_stats_by_hour add day_key varchar(32);
alter table ds_stats_by_hour add month_key varchar(32);

alter table ds_stats_by_day add day_key varchar(32);
alter table ds_stats_by_day add month_key varchar(32);

alter table ds_stats_by_month add month_key varchar(32);

/* Create ds primary keys. */
alter table ds_stats_by_hour add primary key (hour_key,snapshot_time);
alter table ds_stats_by_day add primary key (day_key,snapshot_time);
alter table ds_stats_by_month add primary key (month_key,snapshot_time);


/* Prepare iom historical stats tables */

/* Drop existing iom stats compound indexes, which are no longer required */
alter table iom_stats_latest drop index iom_latest_idx;
alter table iom_stats_by_hour drop index iom_stats_by_hour_idx;
alter table iom_stats_by_day drop index iom_stats_by_day_idx;
alter table iom_stats_by_month drop index iom_stats_by_mth_idx;

/* Add new iom primary key columns.  */
alter table iom_stats_latest add hour_key varchar(32);
alter table iom_stats_latest add day_key varchar(32);
alter table iom_stats_latest add month_key varchar(32);

alter table iom_stats_by_hour add hour_key varchar(32);
alter table iom_stats_by_hour add day_key varchar(32);
alter table iom_stats_by_hour add month_key varchar(32);

alter table iom_stats_by_day add day_key varchar(32);
alter table iom_stats_by_day add month_key varchar(32);

alter table iom_stats_by_month add month_key varchar(32);

/* Create iom primary keys. */
alter table iom_stats_by_hour add primary key (hour_key,snapshot_time);
alter table iom_stats_by_day add primary key (day_key,snapshot_time);
alter table iom_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare pm historical stats tables */

/* Drop existing pm stats compound indexes, which are no longer required */
alter table pm_stats_latest drop index pm_latest_idx;
alter table pm_stats_by_hour drop index pm_stats_by_hour_idx;
alter table pm_stats_by_day drop index pm_stats_by_day_idx;
alter table pm_stats_by_month drop index pm_stats_by_mth_idx;

/* Add new pm primary key columns.  */
alter table pm_stats_latest add hour_key varchar(32);
alter table pm_stats_latest add day_key varchar(32);
alter table pm_stats_latest add month_key varchar(32);

alter table pm_stats_by_hour add hour_key varchar(32);
alter table pm_stats_by_hour add day_key varchar(32);
alter table pm_stats_by_hour add month_key varchar(32);

alter table pm_stats_by_day add day_key varchar(32);
alter table pm_stats_by_day add month_key varchar(32);

alter table pm_stats_by_month add month_key varchar(32);

/* Create pm primary keys. */
alter table pm_stats_by_hour add primary key (hour_key,snapshot_time);
alter table pm_stats_by_day add primary key (day_key,snapshot_time);
alter table pm_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare sc historical stats tables */

/* Drop existing sc stats compound indexes, which are no longer required */
alter table sc_stats_latest drop index sc_latest_idx;
alter table sc_stats_by_hour drop index sc_stats_by_hour_idx;
alter table sc_stats_by_day drop index sc_stats_by_day_idx;
alter table sc_stats_by_month drop index sc_stats_by_mth_idx;

/* Add new sc primary key columns.  */
alter table sc_stats_latest add hour_key varchar(32);
alter table sc_stats_latest add day_key varchar(32);
alter table sc_stats_latest add month_key varchar(32);

alter table sc_stats_by_hour add hour_key varchar(32);
alter table sc_stats_by_hour add day_key varchar(32);
alter table sc_stats_by_hour add month_key varchar(32);

alter table sc_stats_by_day add day_key varchar(32);
alter table sc_stats_by_day add month_key varchar(32);

alter table sc_stats_by_month add month_key varchar(32);

/* Create sc primary keys. */
alter table sc_stats_by_hour add primary key (hour_key,snapshot_time);
alter table sc_stats_by_day add primary key (day_key,snapshot_time);
alter table sc_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare sw historical stats tables */

/* Drop existing sc stats compound indexes, which are no longer required */
alter table sw_stats_latest drop index sw_latest_idx;
alter table sw_stats_by_hour drop index sw_stats_by_hour_idx;
alter table sw_stats_by_day drop index sw_stats_by_day_idx;
alter table sw_stats_by_month drop index sw_stats_by_mth_idx;

/* Add new sc primary key columns.  */
alter table sw_stats_latest add hour_key varchar(32);
alter table sw_stats_latest add day_key varchar(32);
alter table sw_stats_latest add month_key varchar(32);

alter table sw_stats_by_hour add hour_key varchar(32);
alter table sw_stats_by_hour add day_key varchar(32);
alter table sw_stats_by_hour add month_key varchar(32);

alter table sw_stats_by_day add day_key varchar(32);
alter table sw_stats_by_day add month_key varchar(32);

alter table sw_stats_by_month add month_key varchar(32);

/* Create sc primary keys. */
alter table sw_stats_by_hour add primary key (hour_key,snapshot_time);
alter table sw_stats_by_day add primary key (day_key,snapshot_time);
alter table sw_stats_by_month add primary key (month_key,snapshot_time);



/* Prepare vdc historical stats tables */

/* Drop existing vdc stats compound indexes, which are no longer required */
alter table vdc_stats_latest drop index vdc_latest_idx;
alter table vdc_stats_by_hour drop index vdc_stats_by_hour_idx;
alter table vdc_stats_by_day drop index vdc_stats_by_day_idx;
alter table vdc_stats_by_month drop index vdc_stats_by_mth_idx;

/* Add new vdc primary key columns.  */
alter table vdc_stats_latest add hour_key varchar(32);
alter table vdc_stats_latest add day_key varchar(32);
alter table vdc_stats_latest add month_key varchar(32);

alter table vdc_stats_by_hour add hour_key varchar(32);
alter table vdc_stats_by_hour add day_key varchar(32);
alter table vdc_stats_by_hour add month_key varchar(32);

alter table vdc_stats_by_day add day_key varchar(32);
alter table vdc_stats_by_day add month_key varchar(32);

alter table vdc_stats_by_month add month_key varchar(32);

/* Create vdc primary keys. */
alter table vdc_stats_by_hour add primary key (hour_key,snapshot_time);
alter table vdc_stats_by_day add primary key (day_key,snapshot_time);
alter table vdc_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare vm historical stats tables */

/* Drop existing vm stats compound indexes, which are no longer required */
alter table vm_stats_latest drop index vm_latest_idx;
alter table vm_stats_by_hour drop index vm_stats_by_hour_idx;
alter table vm_stats_by_day drop index vm_stats_by_day_idx;
alter table vm_stats_by_month drop index vm_stats_by_mth_idx;

/* Add new vm primary key columns.  */
alter table vm_stats_latest add hour_key varchar(32);
alter table vm_stats_latest add day_key varchar(32);
alter table vm_stats_latest add month_key varchar(32);

alter table vm_stats_by_hour add hour_key varchar(32);
alter table vm_stats_by_hour add day_key varchar(32);
alter table vm_stats_by_hour add month_key varchar(32);

alter table vm_stats_by_day add day_key varchar(32);
alter table vm_stats_by_day add month_key varchar(32);

alter table vm_stats_by_month add month_key varchar(32);

/* Create vm primary keys. */
alter table vm_stats_by_hour add primary key (hour_key,snapshot_time);
alter table vm_stats_by_day add primary key (day_key,snapshot_time);
alter table vm_stats_by_month add primary key (month_key,snapshot_time);

/* Prepare vpod historical stats tables */

/* Drop existing vpod stats compound indexes, which are no longer required */
alter table vpod_stats_latest drop index vpod_latest_idx;
alter table vpod_stats_by_hour drop index vpod_stats_by_hour_idx;
alter table vpod_stats_by_day drop index vpod_stats_by_day_idx;
alter table vpod_stats_by_month drop index vpod_stats_by_mth_idx;

/* Add new vpod primary key columns.  */
alter table vpod_stats_latest add hour_key varchar(32);
alter table vpod_stats_latest add day_key varchar(32);
alter table vpod_stats_latest add month_key varchar(32);

alter table vpod_stats_by_hour add hour_key varchar(32);
alter table vpod_stats_by_hour add day_key varchar(32);
alter table vpod_stats_by_hour add month_key varchar(32);

alter table vpod_stats_by_day add day_key varchar(32);
alter table vpod_stats_by_day add month_key varchar(32);

alter table vpod_stats_by_month add month_key varchar(32);

/* Create vpod primary keys. */
alter table vpod_stats_by_hour add primary key (hour_key,snapshot_time);
alter table vpod_stats_by_day add primary key (day_key,snapshot_time);
alter table vpod_stats_by_month add primary key (month_key,snapshot_time);


DELIMITER //


/* Create app trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
CREATE TRIGGER set_app_primary_keys BEFORE INSERT ON app_stats_latest
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




/* Create ch trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
CREATE TRIGGER set_ch_primary_keys BEFORE INSERT ON ch_stats_latest
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

/* Create cnt trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
CREATE TRIGGER set_cnt_primary_keys BEFORE INSERT ON cnt_stats_latest
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


 /* Create dpod trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
 CREATE TRIGGER set_dpod_primary_keys BEFORE INSERT ON dpod_stats_latest
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

  /* Create ds trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
  CREATE TRIGGER set_ds_primary_keys BEFORE INSERT ON ds_stats_latest
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

   /* Create iom trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
   CREATE TRIGGER set_iom_primary_keys BEFORE INSERT ON iom_stats_latest
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

  /* Create pm trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
  CREATE TRIGGER set_pm_primary_keys BEFORE INSERT ON pm_stats_latest
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

 /* Create sc trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
 CREATE TRIGGER set_sc_primary_keys BEFORE INSERT ON sc_stats_latest
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

/* Create vdc trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
CREATE TRIGGER set_vdc_primary_keys BEFORE INSERT ON vdc_stats_latest
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

/* Create vm trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
CREATE TRIGGER set_vm_primary_keys BEFORE INSERT ON vm_stats_latest
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

/* Create vpod trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */
CREATE TRIGGER set_vpod_primary_keys BEFORE INSERT ON vpod_stats_latest
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

    select count(*) into running_aggregations from aggregation_status where status='Running';
    select running_aggregations;

    set sql_mode='';

    if running_aggregations > 0 then
        select 'Aggregations running... exiting.' as '';
        leave aggregate_proc;
    end if;

    insert into aggregation_status values ('Running');

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

DELIMITER ;


  /* Set initial primary key values where null */

  /* Initialize hour_key, day_key, month_key for app_ history tables where existing keys are null */

  update app_stats_latest
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

  update app_stats_by_hour
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

  update app_stats_by_day
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

  update app_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for ch_ history tables where existing keys are null */

  update ch_stats_latest
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

  update ch_stats_by_hour
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

  update ch_stats_by_day
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

  update ch_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for cnt_ history tables where existing keys are null */

  update cnt_stats_latest
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

  update cnt_stats_by_hour
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

  update cnt_stats_by_day
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

  update cnt_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for dpod_ history tables where existing keys are null */

  update dpod_stats_latest
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

  update dpod_stats_by_hour
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

  update dpod_stats_by_day
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

  update dpod_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for ds_ history tables where existing keys are null */

  update ds_stats_latest
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

  update ds_stats_by_hour
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

  update ds_stats_by_day
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

  update ds_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for iom_ history tables where existing keys are null */

  update iom_stats_latest
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

  update iom_stats_by_hour
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

  update iom_stats_by_day
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

  update iom_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for pm_ history tables where existing keys are null */

  update pm_stats_latest
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

  update pm_stats_by_hour
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

  update pm_stats_by_day
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

  update pm_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for sc_ history tables where existing keys are null */

  update sc_stats_latest
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

  update sc_stats_by_hour
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

  update sc_stats_by_day
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

  update sc_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for sw_ history tables where existing keys are null */

  update sw_stats_latest
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

  update sw_stats_by_hour
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

  update sw_stats_by_day
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

  update sw_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for vdc_ history tables where existing keys are null */

  update vdc_stats_latest
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

  update vdc_stats_by_hour
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

  update vdc_stats_by_day
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

  update vdc_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for vm_ history tables where existing keys are null */

  update vm_stats_latest
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

  update vm_stats_by_hour
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

  update vm_stats_by_day
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

  update vm_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  /* Initialize hour_key, day_key, month_key for vpod_ history tables where existing keys are null */

  update vpod_stats_latest
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

  update vpod_stats_by_hour
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

  update vpod_stats_by_day
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

  update vpod_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;

  UPDATE version_info SET version=68.9 WHERE id=1;
