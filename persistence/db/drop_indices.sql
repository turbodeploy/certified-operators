
use vmtdb ;

drop index pm_bh_uuid_idx             on pm_stats_by_hour ;
drop index pm_bh_property_type_idx    on pm_stats_by_hour ;
drop index pm_bh_property_subtype_idx on pm_stats_by_hour ;

drop index pm_bd_snapshot_time_idx    on pm_stats_by_day ;
drop index pm_bd_uuid_idx             on pm_stats_by_day ;
drop index pm_bd_property_type_idx    on pm_stats_by_day ;
drop index pm_bd_property_subtype_idx on pm_stats_by_day ;

drop index pm_bm_snapshot_time_idx    on pm_stats_by_month ;
drop index pm_bm_uuid_idx             on pm_stats_by_month ;
drop index pm_bm_property_type_idx    on pm_stats_by_month ;
drop index pm_bm_property_subtype_idx on pm_stats_by_month ;


drop index vm_bh_uuid_idx             on vm_stats_by_hour ;
drop index vm_bh_property_type_idx    on vm_stats_by_hour ;
drop index vm_bh_property_subtype_idx on vm_stats_by_hour ;

drop index vm_bd_snapshot_time_idx    on vm_stats_by_day ;
drop index vm_bd_uuid_idx             on vm_stats_by_day ;
drop index vm_bd_property_type_idx    on vm_stats_by_day ;
drop index vm_bd_property_subtype_idx on vm_stats_by_day ;

drop index vm_bm_snapshot_time_idx    on vm_stats_by_month ;
drop index vm_bm_uuid_idx             on vm_stats_by_month ;
drop index vm_bm_property_type_idx    on vm_stats_by_month ;
drop index vm_bm_property_subtype_idx on vm_stats_by_month ;


drop index ds_bh_uuid_idx             on ds_stats_by_hour ;
drop index ds_bh_property_type_idx    on ds_stats_by_hour ;
drop index ds_bh_property_subtype_idx on ds_stats_by_hour ;

drop index ds_bd_snapshot_time_idx    on ds_stats_by_day ;
drop index ds_bd_uuid_idx             on ds_stats_by_day ;
drop index ds_bd_property_type_idx    on ds_stats_by_day ;
drop index ds_bd_property_subtype_idx on ds_stats_by_day ;

drop index ds_bm_snapshot_time_idx    on ds_stats_by_month ;
drop index ds_bm_uuid_idx             on ds_stats_by_month ;
drop index ds_bm_property_type_idx    on ds_stats_by_month ;
drop index ds_bm_property_subtype_idx on ds_stats_by_month ;



drop index sw_bh_uuid_idx             on sw_stats_by_hour ;
drop index sw_bh_property_type_idx    on sw_stats_by_hour ;
drop index sw_bh_property_subtype_idx on sw_stats_by_hour ;

drop index sw_bd_snapshot_time_idx    on sw_stats_by_day ;
drop index sw_bd_uuid_idx             on sw_stats_by_day ;
drop index sw_bd_property_type_idx    on sw_stats_by_day ;
drop index sw_bd_property_subtype_idx on sw_stats_by_day ;

drop index sw_bm_snapshot_time_idx    on sw_stats_by_month ;
drop index sw_bm_uuid_idx             on sw_stats_by_month ;
drop index sw_bm_property_type_idx    on sw_stats_by_month ;
drop index sw_bm_property_subtype_idx on sw_stats_by_month ;



drop index da_bh_uuid_idx             on da_stats_by_hour ;
drop index da_bh_property_type_idx    on da_stats_by_hour ;
drop index da_bh_property_subtype_idx on da_stats_by_hour ;

drop index da_bd_snapshot_time_idx    on da_stats_by_day ;
drop index da_bd_uuid_idx             on da_stats_by_day ;
drop index da_bd_property_type_idx    on da_stats_by_day ;
drop index da_bd_property_subtype_idx on da_stats_by_day ;

drop index da_bm_snapshot_time_idx    on da_stats_by_month ;
drop index da_bm_uuid_idx             on da_stats_by_month ;
drop index da_bm_property_type_idx    on da_stats_by_month ;
drop index da_bm_property_subtype_idx on da_stats_by_month ;



drop index sc_bh_uuid_idx             on sc_stats_by_hour ;
drop index sc_bh_property_type_idx    on sc_stats_by_hour ;
drop index sc_bh_property_subtype_idx on sc_stats_by_hour ;

drop index sc_bd_snapshot_time_idx    on sc_stats_by_day ;
drop index sc_bd_uuid_idx             on sc_stats_by_day ;
drop index sc_bd_property_type_idx    on sc_stats_by_day ;
drop index sc_bd_property_subtype_idx on sc_stats_by_day ;

drop index sc_bm_snapshot_time_idx    on sc_stats_by_month ;
drop index sc_bm_uuid_idx             on sc_stats_by_month ;
drop index sc_bm_property_type_idx    on sc_stats_by_month ;
drop index sc_bm_property_subtype_idx on sc_stats_by_month ;



drop index app_bh_uuid_idx             on app_stats_by_hour ;
drop index app_bh_property_type_idx    on app_stats_by_hour ;
drop index app_bh_property_subtype_idx on app_stats_by_hour ;

drop index app_bd_snapshot_time_idx    on app_stats_by_day ;
drop index app_bd_uuid_idx             on app_stats_by_day ;
drop index app_bd_property_type_idx    on app_stats_by_day ;
drop index app_bd_property_subtype_idx on app_stats_by_day ;

drop index app_bm_snapshot_time_idx    on app_stats_by_month ;
drop index app_bm_uuid_idx             on app_stats_by_month ;
drop index app_bm_property_type_idx    on app_stats_by_month ;
drop index app_bm_property_subtype_idx on app_stats_by_month ;



drop index vdc_bh_uuid_idx             on vdc_stats_by_hour ;
drop index vdc_bh_property_type_idx    on vdc_stats_by_hour ;
drop index vdc_bh_property_subtype_idx on vdc_stats_by_hour ;

drop index vdc_bd_snapshot_time_idx    on vdc_stats_by_day ;
drop index vdc_bd_uuid_idx             on vdc_stats_by_day ;
drop index vdc_bd_property_type_idx    on vdc_stats_by_day ;
drop index vdc_bd_property_subtype_idx on vdc_stats_by_day ;

drop index vdc_bm_snapshot_time_idx    on vdc_stats_by_month ;
drop index vdc_bm_uuid_idx             on vdc_stats_by_month ;
drop index vdc_bm_property_type_idx    on vdc_stats_by_month ;
drop index vdc_bm_property_subtype_idx on vdc_stats_by_month ;


drop index cl_bd_recorded_on_idx      on cluster_stats_by_day ;
drop index cl_bd_internal_name_idx    on cluster_stats_by_day ;
drop index cl_bd_property_type_idx    on cluster_stats_by_day ;
drop index cl_bd_property_subtype_idx on cluster_stats_by_day ;

drop index cl_bm_recorded_on_idx      on cluster_stats_by_month ;
drop index cl_bm_internal_name_idx    on cluster_stats_by_month ;
drop index cl_bm_property_type_idx    on cluster_stats_by_month ;
drop index cl_bm_property_subtype_idx on cluster_stats_by_month ;

drop index cl_m_recorded_on_idx   on cluster_members ;
drop index cl_m_internal_name_idx on cluster_members ;
drop index cl_m_group_uuid_idx    on cluster_members ;
drop index cl_m_member_uuid_idx   on cluster_members ;
drop index cl_m_group_type_idx    on cluster_members ;


drop index entity_uuid_idx on entities ;
drop index entity_attr_name_idx on entity_attrs ;
drop index entity_assn_name_idx on entity_assns ;
