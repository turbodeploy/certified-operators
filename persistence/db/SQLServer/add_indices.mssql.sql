
use vmtdb;


create index pm_bh_uuid_idx             on pm_stats_by_hour (uuid);
create index pm_bh_property_type_idx    on pm_stats_by_hour (property_type);
create index pm_bh_property_subtype_idx on pm_stats_by_hour (property_subtype);
go

create index pm_bd_snapshot_time_idx    on pm_stats_by_day (snapshot_time);
create index pm_bd_uuid_idx             on pm_stats_by_day (uuid);
create index pm_bd_property_type_idx    on pm_stats_by_day (property_type);
create index pm_bd_property_subtype_idx on pm_stats_by_day (property_subtype);
go

create index pm_bm_snapshot_time_idx    on pm_stats_by_month (snapshot_time);
create index pm_bm_uuid_idx             on pm_stats_by_month (uuid);
create index pm_bm_property_type_idx    on pm_stats_by_month (property_type);
create index pm_bm_property_subtype_idx on pm_stats_by_month (property_subtype);
go


create index vm_bh_uuid_idx             on vm_stats_by_hour (uuid);
create index vm_bh_property_type_idx    on vm_stats_by_hour (property_type);
create index vm_bh_property_subtype_idx on vm_stats_by_hour (property_subtype);
go

create index vm_bd_snapshot_time_idx    on vm_stats_by_day (snapshot_time);
create index vm_bd_uuid_idx             on vm_stats_by_day (uuid);
create index vm_bd_property_type_idx    on vm_stats_by_day (property_type);
create index vm_bd_property_subtype_idx on vm_stats_by_day (property_subtype);
go

create index vm_bm_snapshot_time_idx    on vm_stats_by_month (snapshot_time);
create index vm_bm_uuid_idx             on vm_stats_by_month (uuid);
create index vm_bm_property_type_idx    on vm_stats_by_month (property_type);
create index vm_bm_property_subtype_idx on vm_stats_by_month (property_subtype);
go


create index ds_bh_uuid_idx             on ds_stats_by_hour (uuid);
create index ds_bh_property_type_idx    on ds_stats_by_hour (property_type);
create index ds_bh_property_subtype_idx on ds_stats_by_hour (property_subtype);
go

create index ds_bd_snapshot_time_idx    on ds_stats_by_day (snapshot_time);
create index ds_bd_uuid_idx             on ds_stats_by_day (uuid);
create index ds_bd_property_type_idx    on ds_stats_by_day (property_type);
create index ds_bd_property_subtype_idx on ds_stats_by_day (property_subtype);
go

create index ds_bm_snapshot_time_idx    on ds_stats_by_month (snapshot_time);
create index ds_bm_uuid_idx             on ds_stats_by_month (uuid);
create index ds_bm_property_type_idx    on ds_stats_by_month (property_type);
create index ds_bm_property_subtype_idx on ds_stats_by_month (property_subtype);
go


create index entity_uuid_idx on entities(uuid) ;
create index entity_attr_name_idx on entity_attrs(name) ;
create index entity_assn_name_idx on entity_assns(name) ;
go
