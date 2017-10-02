
use vmtdb;


drop index pm_bh_uuid_idx             on pm_stats_by_hour ;
drop index pm_bh_property_type_idx    on pm_stats_by_hour ;
drop index pm_bh_property_subtype_idx on pm_stats_by_hour ;
go

drop index pm_bd_snapshot_time_idx    on pm_stats_by_day ;
drop index pm_bd_uuid_idx             on pm_stats_by_day ;
drop index pm_bd_property_type_idx    on pm_stats_by_day ;
drop index pm_bd_property_subtype_idx on pm_stats_by_day ;
go

drop index pm_bm_snapshot_time_idx    on pm_stats_by_month ;
drop index pm_bm_uuid_idx             on pm_stats_by_month ;
drop index pm_bm_property_type_idx    on pm_stats_by_month ;
drop index pm_bm_property_subtype_idx on pm_stats_by_month ;
go


drop index vm_bh_uuid_idx             on vm_stats_by_hour ;
drop index vm_bh_property_type_idx    on vm_stats_by_hour ;
drop index vm_bh_property_subtype_idx on vm_stats_by_hour ;
go

drop index vm_bd_snapshot_time_idx    on vm_stats_by_day ;
drop index vm_bd_uuid_idx             on vm_stats_by_day ;
drop index vm_bd_property_type_idx    on vm_stats_by_day ;
drop index vm_bd_property_subtype_idx on vm_stats_by_day ;
go

drop index vm_bm_snapshot_time_idx    on vm_stats_by_month ;
drop index vm_bm_uuid_idx             on vm_stats_by_month ;
drop index vm_bm_property_type_idx    on vm_stats_by_month ;
drop index vm_bm_property_subtype_idx on vm_stats_by_month ;
go


drop index ds_bh_uuid_idx             on ds_stats_by_hour ;
drop index ds_bh_property_type_idx    on ds_stats_by_hour ;
drop index ds_bh_property_subtype_idx on ds_stats_by_hour ;
go

drop index ds_bd_snapshot_time_idx    on ds_stats_by_day ;
drop index ds_bd_uuid_idx             on ds_stats_by_day ;
drop index ds_bd_property_type_idx    on ds_stats_by_day ;
drop index ds_bd_property_subtype_idx on ds_stats_by_day ;
go

drop index ds_bm_snapshot_time_idx    on ds_stats_by_month ;
drop index ds_bm_uuid_idx             on ds_stats_by_month ;
drop index ds_bm_property_type_idx    on ds_stats_by_month ;
drop index ds_bm_property_subtype_idx on ds_stats_by_month ;
go


drop index entity_uuid_idx on entities ;
drop index entity_attr_name_idx on entity_attrs ;
drop index entity_assn_name_idx on entity_assns ;
go
