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


-- Modify xx_stats_latest tables.  Add a new boolean column, aggregated,
-- To keep track of whether the particular row has been proccessed.

alter table app_stats_latest add aggregated boolean not null default false;
alter table ch_stats_latest add aggregated boolean not null default false;
alter table cnt_stats_latest add aggregated boolean not null default false;
alter table dpod_stats_latest add aggregated boolean not null default false;
alter table ds_stats_latest add aggregated boolean not null default false;
alter table iom_stats_latest add aggregated boolean not null default false;
alter table pm_stats_latest add aggregated boolean not null default false;
alter table sc_stats_latest add aggregated boolean not null default false;
alter table sw_stats_latest add aggregated boolean not null default false;
alter table vdc_stats_latest add aggregated boolean not null default false;
alter table vm_stats_latest add aggregated boolean not null default false;
alter table vpod_stats_latest add aggregated boolean not null default false;


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


-- Create index on aggregated column

create index app_aggr_idx on app_stats_latest (aggregated);
create index ch_aggr_idx on ch_stats_latest (aggregated);
create index cnt_aggr_idx on cnt_stats_latest (aggregated);
create index dpod_aggr_idx on dpod_stats_latest (aggregated);
create index ds_aggr_idx on ds_stats_latest (aggregated);
create index iom_aggr_idx on iom_stats_latest (aggregated);
create index pm_aggr_idx on pm_stats_latest (aggregated);
create index sc_aggr_idx on sc_stats_latest (aggregated);
create index sw_aggr_idx on sw_stats_latest (aggregated);
create index vdc_aggr_idx on vdc_stats_latest (aggregated);
create index vm_aggr_idx on vm_stats_latest (aggregated);
create index vpod_aggr_idx on vpod_stats_latest (aggregated);



-- STATS TABLE ALTERS

alter table app_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_hour COLLATE=utf8_general_ci;

alter table app_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_day COLLATE=utf8_general_ci;

alter table app_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_by_month COLLATE=utf8_general_ci;

alter table app_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table app_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table app_stats_latest COLLATE=utf8_general_ci;

alter table ch_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_hour COLLATE=utf8_general_ci;

alter table ch_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_day COLLATE=utf8_general_ci;

alter table ch_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_by_month COLLATE=utf8_general_ci;

alter table ch_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ch_stats_latest COLLATE=utf8_general_ci;


alter table cnt_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_hour COLLATE=utf8_general_ci;

alter table cnt_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_day COLLATE=utf8_general_ci;

alter table cnt_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_by_month COLLATE=utf8_general_ci;

alter table cnt_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table cnt_stats_latest COLLATE=utf8_general_ci;

alter table dpod_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_hour COLLATE=utf8_general_ci;

alter table dpod_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_day COLLATE=utf8_general_ci;

alter table dpod_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_by_month COLLATE=utf8_general_ci;

alter table dpod_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table dpod_stats_latest COLLATE=utf8_general_ci;


alter table ds_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_hour COLLATE=utf8_general_ci;

alter table ds_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_day COLLATE=utf8_general_ci;

alter table ds_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_by_month COLLATE=utf8_general_ci;

alter table ds_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table ds_stats_latest COLLATE=utf8_general_ci;


alter table iom_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_hour COLLATE=utf8_general_ci;

alter table iom_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_day COLLATE=utf8_general_ci;

alter table iom_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_by_month COLLATE=utf8_general_ci;

alter table iom_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table iom_stats_latest COLLATE=utf8_general_ci;

alter table pm_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_hour COLLATE=utf8_general_ci;

alter table pm_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_day COLLATE=utf8_general_ci;

alter table pm_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_by_month COLLATE=utf8_general_ci;

alter table pm_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table pm_stats_latest COLLATE=utf8_general_ci;

alter table sc_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_hour COLLATE=utf8_general_ci;

alter table sc_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_day COLLATE=utf8_general_ci;

alter table sc_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_by_month COLLATE=utf8_general_ci;

alter table sc_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sc_stats_latest COLLATE=utf8_general_ci;

alter table sw_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_hour COLLATE=utf8_general_ci;

alter table sw_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_day COLLATE=utf8_general_ci;

alter table sw_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_by_month COLLATE=utf8_general_ci;

alter table sw_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table sw_stats_latest COLLATE=utf8_general_ci;

alter table vdc_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_hour COLLATE=utf8_general_ci;

alter table vdc_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_day COLLATE=utf8_general_ci;

alter table vdc_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_by_month COLLATE=utf8_general_ci;

alter table vdc_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vdc_stats_latest COLLATE=utf8_general_ci;

alter table vm_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_hour COLLATE=utf8_general_ci;

alter table vm_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_day COLLATE=utf8_general_ci;

alter table vm_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_by_month COLLATE=utf8_general_ci;

alter table vm_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vm_stats_latest COLLATE=utf8_general_ci;

alter table vpod_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_hour COLLATE=utf8_general_ci;

alter table vpod_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_day COLLATE=utf8_general_ci;

alter table vpod_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_by_month COLLATE=utf8_general_ci;

alter table vpod_stats_latest modify uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_latest modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_general_ci default null;
alter table vpod_stats_latest COLLATE=utf8_general_ci;

-- CREATE INDEXES FOR AGGREGATION

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats
create index app_latest_idx on app_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index ch_latest_idx on ch_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index cnt_latest_idx on cnt_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index dpod_latest_idx on dpod_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index ds_latest_idx on ds_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index iom_latest_idx on iom_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index pm_latest_idx on pm_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index sc_latest_idx on sc_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index sw_latest_idx on sw_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vdc_latest_idx on vdc_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vm_latest_idx on vm_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);
create index vpod_latest_idx on vpod_stats_latest(snapshot_time,uuid,producer_uuid,property_type,property_subtype,relation);



UPDATE version_info SET version=67.6 WHERE id=1;