-- Prepare tables for anyncronous stats rollup

-- Drop triggers if they already exist from a previous build

drop trigger if exists app_stats_latest_after_insert;
drop trigger if exists ch_stats_latest_after_insert;
drop trigger if exists cnt_stats_latest_after_insert;
drop trigger if exists dpod_stats_latest_after_insert;
drop trigger if exists ds_stats_latest_after_insert;
drop trigger if exists iom_stats_latest_after_insert;
drop trigger if exists pm_stats_latest_after_insert;
drop trigger if exists sc_stats_latest_after_insert;
drop trigger if exists sw_stats_latest_after_insert;
drop trigger if exists vdc_stats_latest_after_insert;
drop trigger if exists vm_stats_latest_after_insert;
drop trigger if exists vpod_stats_latest_after_insert;

-- Modify xx_stats tables.  Add a new boolean column, aggregated,
-- To keep track of whether the particular row has been proccessed.

alter table app_stats_by_hour add aggregated boolean not null default false;
alter table ch_stats_by_hour add aggregated boolean not null default false;
alter table cnt_stats_by_hour add aggregated boolean not null default false;
alter table dpod_stats_by_hour add aggregated boolean not null default false;
alter table ds_stats_by_hour add aggregated boolean not null default false;
alter table iom_stats_by_hour add aggregated boolean not null default false;
alter table pm_stats_by_hour add aggregated boolean not null default false;
alter table sc_stats_by_hour add aggregated boolean not null default false;
alter table sw_stats_by_hour add aggregated boolean not null default false;
alter table vdc_stats_by_hour add aggregated boolean not null default false;
alter table vm_stats_by_hour add aggregated boolean not null default false;
alter table vpod_stats_by_hour add aggregated boolean not null default false;

alter table app_stats_by_day add aggregated boolean not null default false;
alter table ch_stats_by_day add aggregated boolean not null default false;
alter table cnt_stats_by_day add aggregated boolean not null default false;
alter table dpod_stats_by_day add aggregated boolean not null default false;
alter table ds_stats_by_day add aggregated boolean not null default false;
alter table iom_stats_by_day add aggregated boolean not null default false;
alter table pm_stats_by_day add aggregated boolean not null default false;
alter table sc_stats_by_day add aggregated boolean not null default false;
alter table sw_stats_by_day add aggregated boolean not null default false;
alter table vdc_stats_by_day add aggregated boolean not null default false;
alter table vm_stats_by_day add aggregated boolean not null default false;
alter table vpod_stats_by_day add aggregated boolean not null default false;

update app_stats_latest set aggregated=true;
update ch_stats_latest set aggregated=true;
update cnt_stats_latest set aggregated=true;
update dpod_stats_latest set aggregated=true;
update ds_stats_latest set aggregated=true;
update iom_stats_latest set aggregated=true;
update pm_stats_latest set aggregated=true;
update sc_stats_latest set aggregated=true;
update sw_stats_latest set aggregated=true;
update vdc_stats_latest set aggregated=true;
update vm_stats_latest set aggregated=true;
update vpod_stats_latest set aggregated=true;

update app_stats_by_hour set aggregated=true;
update ch_stats_by_hour set aggregated=true;
update cnt_stats_by_hour set aggregated=true;
update dpod_stats_by_hour set aggregated=true;
update ds_stats_by_hour set aggregated=true;
update iom_stats_by_hour set aggregated=true;
update pm_stats_by_hour set aggregated=true;
update sc_stats_by_hour set aggregated=true;
update sw_stats_by_hour set aggregated=true;
update vdc_stats_by_hour set aggregated=true;
update vm_stats_by_hour set aggregated=true;
update vpod_stats_by_hour set aggregated=true;

update app_stats_by_day set aggregated=true;
update ch_stats_by_day set aggregated=true;
update cnt_stats_by_day set aggregated=true;
update dpod_stats_by_day set aggregated=true;
update ds_stats_by_day set aggregated=true;
update iom_stats_by_day set aggregated=true;
update pm_stats_by_day set aggregated=true;
update sc_stats_by_day set aggregated=true;
update sw_stats_by_day set aggregated=true;
update vdc_stats_by_day set aggregated=true;
update vm_stats_by_day set aggregated=true;
update vpod_stats_by_day set aggregated=true;


-- Create index on aggregated column

create index app_aggr_hour_idx on app_stats_by_hour (aggregated);
create index ch_aggr_hour_idx on ch_stats_by_hour (aggregated);
create index cnt_aggr_hour_idx on cnt_stats_by_hour (aggregated);
create index dpod_aggr_hour_idx on dpod_stats_by_hour (aggregated);
create index ds_aggr_hour_idx on ds_stats_by_hour (aggregated);
create index iom_aggr_hour_idx on iom_stats_by_hour (aggregated);
create index pm_aggr_hour_idx on pm_stats_by_hour (aggregated);
create index sc_aggr_hour_idx on sc_stats_by_hour (aggregated);
create index sw_aggr_hour_idx on sw_stats_by_hour (aggregated);
create index vdc_aggr_hour_idx on vdc_stats_by_hour (aggregated);
create index vm_aggr_hour_idx on vm_stats_by_hour (aggregated);
create index vpod_aggr_hour_idx on vpod_stats_by_hour (aggregated);

create index app_aggr_day_idx on app_stats_by_day (aggregated);
create index ch_aggr_day_idx on ch_stats_by_day (aggregated);
create index cnt_aggr_day_idx on cnt_stats_by_day (aggregated);
create index dpod_aggr_day_idx on dpod_stats_by_day (aggregated);
create index ds_aggr_day_idx on ds_stats_by_day (aggregated);
create index iom_aggr_day_idx on iom_stats_by_day (aggregated);
create index pm_aggr_day_idx on pm_stats_by_day (aggregated);
create index sc_aggr_day_idx on sc_stats_by_day (aggregated);
create index sw_aggr_day_idx on sw_stats_by_day (aggregated);
create index vdc_aggr_day_idx on vdc_stats_by_day (aggregated);
create index vm_aggr_day_idx on vm_stats_by_day (aggregated);
create index vpod_aggr_day_idx on vpod_stats_by_day (aggregated);

/*  Add new column to keep track of the number of new samples added to an existing set */

alter table app_stats_by_hour add new_samples integer;
alter table app_stats_by_day add new_samples integer;
alter table app_stats_by_month add new_samples integer;

alter table ch_stats_by_hour add new_samples integer;
alter table ch_stats_by_day add new_samples integer;
alter table ch_stats_by_month add new_samples integer;

alter table cnt_stats_by_hour add new_samples integer;
alter table cnt_stats_by_day add new_samples integer;
alter table cnt_stats_by_month add new_samples integer;

alter table dpod_stats_by_hour add new_samples integer;
alter table dpod_stats_by_day add new_samples integer;
alter table dpod_stats_by_month add new_samples integer;

alter table ds_stats_by_hour add new_samples integer;
alter table ds_stats_by_day add new_samples integer;
alter table ds_stats_by_month add new_samples integer;

alter table iom_stats_by_hour add new_samples integer;
alter table iom_stats_by_day add new_samples integer;
alter table iom_stats_by_month add new_samples integer;

alter table pm_stats_by_hour add new_samples integer;
alter table pm_stats_by_day add new_samples integer;
alter table pm_stats_by_month add new_samples integer;

alter table sc_stats_by_hour add new_samples integer;
alter table sc_stats_by_day add new_samples integer;
alter table sc_stats_by_month add new_samples integer;

alter table sw_stats_by_hour add new_samples integer;
alter table sw_stats_by_day add new_samples integer;
alter table sw_stats_by_month add new_samples integer;

alter table vdc_stats_by_hour add new_samples integer;
alter table vdc_stats_by_day add new_samples integer;
alter table vdc_stats_by_month add new_samples integer;

alter table vm_stats_by_hour add new_samples integer;
alter table vm_stats_by_day add new_samples integer;
alter table vm_stats_by_month add new_samples integer;

alter table vpod_stats_by_hour add new_samples integer;
alter table vpod_stats_by_day add new_samples integer;
alter table vpod_stats_by_month add new_samples integer;

UPDATE version_info SET version=68.1 WHERE id=1;