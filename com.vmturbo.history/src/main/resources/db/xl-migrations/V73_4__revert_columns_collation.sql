-- All the tables from OpsManager are explicitly set to utf8_unicode_ci collation, while in migration 67.6 it has been corrupted

alter table app_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_hour COLLATE=utf8_unicode_ci;
alter table app_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_day COLLATE=utf8_unicode_ci;
alter table app_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_by_month COLLATE=utf8_unicode_ci;
alter table app_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table app_stats_latest COLLATE=utf8_unicode_ci;
alter table ch_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_hour COLLATE=utf8_unicode_ci;
alter table ch_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_day COLLATE=utf8_unicode_ci;
alter table ch_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_by_month COLLATE=utf8_unicode_ci;
alter table ch_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ch_stats_latest COLLATE=utf8_unicode_ci;
alter table cnt_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_hour COLLATE=utf8_unicode_ci;
alter table cnt_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_day COLLATE=utf8_unicode_ci;
alter table cnt_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_by_month COLLATE=utf8_unicode_ci;
alter table cnt_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table cnt_stats_latest COLLATE=utf8_unicode_ci;
alter table dpod_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_hour COLLATE=utf8_unicode_ci;
alter table dpod_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_day COLLATE=utf8_unicode_ci;
alter table dpod_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_by_month COLLATE=utf8_unicode_ci;
alter table dpod_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table dpod_stats_latest COLLATE=utf8_unicode_ci;
alter table ds_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_hour COLLATE=utf8_unicode_ci;
alter table ds_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_day COLLATE=utf8_unicode_ci;
alter table ds_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_by_month COLLATE=utf8_unicode_ci;
alter table ds_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table ds_stats_latest COLLATE=utf8_unicode_ci;
alter table iom_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_hour COLLATE=utf8_unicode_ci;
alter table iom_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_day COLLATE=utf8_unicode_ci;
alter table iom_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_by_month COLLATE=utf8_unicode_ci;
alter table iom_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table iom_stats_latest COLLATE=utf8_unicode_ci;
alter table pm_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_hour COLLATE=utf8_unicode_ci;
alter table pm_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_day COLLATE=utf8_unicode_ci;
alter table pm_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_by_month COLLATE=utf8_unicode_ci;
alter table pm_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table pm_stats_latest COLLATE=utf8_unicode_ci;
alter table sc_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_hour COLLATE=utf8_unicode_ci;
alter table sc_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_day COLLATE=utf8_unicode_ci;
alter table sc_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_by_month COLLATE=utf8_unicode_ci;
alter table sc_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sc_stats_latest COLLATE=utf8_unicode_ci;
alter table sw_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_hour COLLATE=utf8_unicode_ci;
alter table sw_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_day COLLATE=utf8_unicode_ci;
alter table sw_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_by_month COLLATE=utf8_unicode_ci;
alter table sw_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table sw_stats_latest COLLATE=utf8_unicode_ci;
alter table vdc_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_hour COLLATE=utf8_unicode_ci;
alter table vdc_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_day COLLATE=utf8_unicode_ci;
alter table vdc_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_by_month COLLATE=utf8_unicode_ci;
alter table vdc_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vdc_stats_latest COLLATE=utf8_unicode_ci;
alter table vm_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_hour COLLATE=utf8_unicode_ci;
alter table vm_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_day COLLATE=utf8_unicode_ci;
alter table vm_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_by_month COLLATE=utf8_unicode_ci;
alter table vm_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vm_stats_latest COLLATE=utf8_unicode_ci;
alter table vpod_stats_by_hour modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_hour modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_hour modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_hour COLLATE=utf8_unicode_ci;
alter table vpod_stats_by_day modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_day modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_day modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_day COLLATE=utf8_unicode_ci;
alter table vpod_stats_by_month modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_month modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_month modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_by_month COLLATE=utf8_unicode_ci;
alter table vpod_stats_latest modify uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_latest modify producer_uuid varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_latest modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_latest modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_latest modify commodity_key varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table vpod_stats_latest COLLATE=utf8_unicode_ci;
alter table market_stats_by_hour modify entity_type varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_hour COLLATE=utf8_unicode_ci;
alter table market_stats_by_day modify entity_type varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_day COLLATE=utf8_unicode_ci;
alter table market_stats_by_month modify entity_type varchar(80) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_unicode_ci default null;
alter table market_stats_by_month COLLATE=utf8_unicode_ci;

-- Recreating procedure with correct collation - unicode
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
    DECLARE cur1 CURSOR FOR (select substring(PARTITION_NAME, 7) from information_schema.partitions where table_name=stats_table COLLATE utf8_unicode_ci and TABLE_SCHEMA=database() and substring(PARTITION_NAME, 7) <> '' order by PARTITION_NAME asc);

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

