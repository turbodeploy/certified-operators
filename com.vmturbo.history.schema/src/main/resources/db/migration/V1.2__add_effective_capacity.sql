-- Add "effective_capacity" to all stats tables
alter table app_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table app_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table app_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table app_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table ch_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ch_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ch_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ch_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table cnt_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table cnt_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table cnt_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table cnt_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table cpod_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table cpod_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table cpod_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table cpod_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table da_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table da_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table da_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table da_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table dpod_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table dpod_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table dpod_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table dpod_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table ds_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ds_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ds_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ds_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table iom_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table iom_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table iom_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table iom_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table lp_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table lp_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table lp_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table lp_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table market_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table market_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table market_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table market_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table pm_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table pm_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table pm_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table pm_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table ri_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ri_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ri_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table ri_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table sc_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table sc_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table sc_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table sc_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table sw_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table sw_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table sw_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table sw_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table vdc_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vdc_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vdc_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vdc_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table vm_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vm_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vm_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vm_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;

alter table vpod_stats_latest add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vpod_stats_by_hour add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vpod_stats_by_day add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
alter table vpod_stats_by_month add column if not exists effective_capacity decimal(15,3) DEFAULT NULL;
