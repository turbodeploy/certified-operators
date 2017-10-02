
use vmtdb;


/*
    ---------------------------------------------------------------
        Views
          
          IF OBJECT_ID ('dbo.Reorder', 'V') IS NOT NULL DROP VIEW dbo.Reorder ;
    ---------------------------------------------------------------
*/


IF OBJECT_ID ('dbo.cluster_membership', 'V') IS NOT NULL drop view dbo.cluster_membership;
IF OBJECT_ID ('dbo.pm_vm_count_by_day_per_pm_group', 'V') IS NOT NULL drop view dbo.pm_vm_count_by_day_per_pm_group;
IF OBJECT_ID ('dbo.vm_storage_used_by_day_per_vm_group', 'V') IS NOT NULL drop view dbo.vm_storage_used_by_day_per_vm_group;
IF OBJECT_ID ('dbo.active_vms', 'V') IS NOT NULL drop view dbo.active_vms;
IF OBJECT_ID ('dbo.user_ds_stats_by_day', 'V') IS NOT NULL drop view dbo.user_ds_stats_by_day;
IF OBJECT_ID ('dbo.user_vm_stats_by_day_per_group', 'V') IS NOT NULL drop view dbo.user_vm_stats_by_day_per_group;
IF OBJECT_ID ('dbo.user_vm_stats_by_day', 'V') IS NOT NULL drop view dbo.user_vm_stats_by_day;
IF OBJECT_ID ('dbo.user_pm_stats_by_day', 'V') IS NOT NULL drop view dbo.user_pm_stats_by_day;
IF OBJECT_ID ('dbo.user_ds_stats_by_hour', 'V') IS NOT NULL drop view dbo.user_ds_stats_by_hour;
IF OBJECT_ID ('dbo.user_vm_stats_by_hour_per_group', 'V') IS NOT NULL drop view dbo.user_vm_stats_by_hour_per_group;
IF OBJECT_ID ('dbo.user_vm_stats_by_hour', 'V') IS NOT NULL drop view dbo.user_vm_stats_by_hour;
IF OBJECT_ID ('dbo.user_pm_stats_by_hour', 'V') IS NOT NULL drop view dbo.user_pm_stats_by_hour;
IF OBJECT_ID ('dbo.ds_stats_by_day_per_ds_group', 'V') IS NOT NULL drop view dbo.ds_stats_by_day_per_ds_group;
IF OBJECT_ID ('dbo.vm_stats_by_day_per_vm_group_agg', 'V') IS NOT NULL drop view dbo.vm_stats_by_day_per_vm_group_agg;
IF OBJECT_ID ('dbo.vm_stats_by_day_per_vm_group', 'V') IS NOT NULL drop view dbo.vm_stats_by_day_per_vm_group;
IF OBJECT_ID ('dbo.pm_stats_by_day_per_pm_group', 'V') IS NOT NULL drop view dbo.pm_stats_by_day_per_pm_group;
IF OBJECT_ID ('dbo.ds_stats_by_hour_per_ds_group', 'V') IS NOT NULL drop view dbo.ds_stats_by_hour_per_ds_group;
IF OBJECT_ID ('dbo.vm_stats_by_hour_per_vm_group_agg', 'V') IS NOT NULL drop view dbo.vm_stats_by_hour_per_vm_group_agg;
IF OBJECT_ID ('dbo.vm_stats_by_hour_per_vm_group', 'V') IS NOT NULL drop view dbo.vm_stats_by_hour_per_vm_group;
IF OBJECT_ID ('dbo.pm_stats_by_hour_per_pm_group', 'V') IS NOT NULL drop view dbo.pm_stats_by_hour_per_pm_group;
IF OBJECT_ID ('dbo.ds_capacity_by_day_per_ds_group', 'V') IS NOT NULL drop view dbo.ds_capacity_by_day_per_ds_group;
IF OBJECT_ID ('dbo.vm_capacity_by_day_per_vm_group', 'V') IS NOT NULL drop view dbo.vm_capacity_by_day_per_vm_group;
IF OBJECT_ID ('dbo.pm_capacity_by_day_per_pm_group', 'V') IS NOT NULL drop view dbo.pm_capacity_by_day_per_pm_group;
IF OBJECT_ID ('dbo.ds_group_members', 'V') IS NOT NULL drop view dbo.ds_group_members;
IF OBJECT_ID ('dbo.vm_group_members_agg', 'V') IS NOT NULL drop view dbo.vm_group_members_agg;
IF OBJECT_ID ('dbo.vm_group_members', 'V') IS NOT NULL drop view dbo.vm_group_members;
IF OBJECT_ID ('dbo.pm_group_members', 'V') IS NOT NULL drop view dbo.pm_group_members;
IF OBJECT_ID ('dbo.entity_group_members', 'V') IS NOT NULL drop view dbo.entity_group_members;
IF OBJECT_ID ('dbo.ds_group_assns', 'V') IS NOT NULL drop view dbo.ds_group_assns;
IF OBJECT_ID ('dbo.vm_group_assns', 'V') IS NOT NULL drop view dbo.vm_group_assns;
IF OBJECT_ID ('dbo.pm_group_assns', 'V') IS NOT NULL drop view dbo.pm_group_assns;
IF OBJECT_ID ('dbo.entity_group_assns', 'V') IS NOT NULL drop view dbo.entity_group_assns;
IF OBJECT_ID ('dbo.app_groups', 'V') IS NOT NULL drop view dbo.app_groups;
IF OBJECT_ID ('dbo.ds_groups', 'V') IS NOT NULL drop view dbo.ds_groups;
IF OBJECT_ID ('dbo.vm_groups', 'V') IS NOT NULL drop view dbo.vm_groups;
IF OBJECT_ID ('dbo.pm_groups', 'V') IS NOT NULL drop view dbo.pm_groups;
IF OBJECT_ID ('dbo.entity_groups', 'V') IS NOT NULL drop view dbo.entity_groups;
IF OBJECT_ID ('dbo.ds_capacity_by_day', 'V') IS NOT NULL drop view dbo.ds_capacity_by_day;
IF OBJECT_ID ('dbo.ds_capacity_by_hour', 'V') IS NOT NULL drop view dbo.ds_capacity_by_hour;
IF OBJECT_ID ('dbo.vm_capacity_by_day', 'V') IS NOT NULL drop view dbo.vm_capacity_by_day;
IF OBJECT_ID ('dbo.vm_capacity_by_hour', 'V') IS NOT NULL drop view dbo.vm_capacity_by_hour;
IF OBJECT_ID ('dbo.pm_capacity_by_day', 'V') IS NOT NULL drop view dbo.pm_capacity_by_day;
IF OBJECT_ID ('dbo.pm_capacity_by_hour', 'V') IS NOT NULL drop view dbo.pm_capacity_by_hour;
IF OBJECT_ID ('dbo.ds_util_info_yesterday', 'V') IS NOT NULL drop view dbo.ds_util_info_yesterday;
IF OBJECT_ID ('dbo.ds_util_stats_yesterday', 'V') IS NOT NULL drop view dbo.ds_util_stats_yesterday;
IF OBJECT_ID ('dbo.vm_util_info_yesterday', 'V') IS NOT NULL drop view dbo.vm_util_info_yesterday;
IF OBJECT_ID ('dbo.vm_util_stats_yesterday', 'V') IS NOT NULL drop view dbo.vm_util_stats_yesterday;
IF OBJECT_ID ('dbo.pm_util_info_yesterday', 'V') IS NOT NULL drop view dbo.pm_util_info_yesterday;
IF OBJECT_ID ('dbo.pm_util_stats_yesterday', 'V') IS NOT NULL drop view dbo.pm_util_stats_yesterday;
IF OBJECT_ID ('dbo.ds_instances', 'V') IS NOT NULL drop view dbo.ds_instances;
IF OBJECT_ID ('dbo.vm_instances', 'V') IS NOT NULL drop view dbo.vm_instances;
IF OBJECT_ID ('dbo.pm_instances', 'V') IS NOT NULL drop view dbo.pm_instances;
IF OBJECT_ID ('dbo.ds_summary_stats_by_month', 'V') IS NOT NULL drop view dbo.ds_summary_stats_by_month;
IF OBJECT_ID ('dbo.vm_summary_stats_by_month', 'V') IS NOT NULL drop view dbo.vm_summary_stats_by_month;
IF OBJECT_ID ('dbo.pm_summary_stats_by_month', 'V') IS NOT NULL drop view dbo.pm_summary_stats_by_month;
IF OBJECT_ID ('dbo.ds_summary_stats_by_day', 'V') IS NOT NULL drop view dbo.ds_summary_stats_by_day;
IF OBJECT_ID ('dbo.vm_summary_stats_by_day', 'V') IS NOT NULL drop view dbo.vm_summary_stats_by_day;
IF OBJECT_ID ('dbo.pm_summary_stats_by_day', 'V') IS NOT NULL drop view dbo.pm_summary_stats_by_day;
go

create view pm_summary_stats_by_day as
select
  (dbo.unix_timestamp(convert(date,dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  pm_stats_by_hour
group by
  convert(date,dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;
go

create view vm_summary_stats_by_day as
select
  (dbo.unix_timestamp(convert(date,dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  vm_stats_by_hour
where
  property_type like 'Storage%'
group by
  convert(date,dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  producer_uuid,
  property_type,
  property_subtype

union all

select
  (dbo.unix_timestamp(convert(date,dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  vm_stats_by_hour
where
  property_type not like 'Storage%'
group by
  convert(date,dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;
go

create view ds_summary_stats_by_day as
select
  (dbo.unix_timestamp(convert(date,dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  ds_stats_by_hour
group by
  convert(date,dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;
go

/* -- -- -- */

create view pm_summary_stats_by_month as
select
  (dbo.unix_timestamp(dbo.last_day(dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  pm_stats_by_day
group by
  year(dbo.from_unixtime(snapshot_time/1000)),
  month(dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;
go


create view vm_summary_stats_by_month as
select
  (dbo.unix_timestamp(dbo.last_day(dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  vm_stats_by_day
where
  property_type like 'Storage%'
group by
  year(dbo.from_unixtime(snapshot_time/1000)),
  month(dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  producer_uuid,
  property_type,
  property_subtype

union all

select
  (dbo.unix_timestamp(dbo.last_day(dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  vm_stats_by_day
where
  property_type not like 'Storage%'
group by
  year(dbo.from_unixtime(snapshot_time/1000)),
  month(dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;
go


create view ds_summary_stats_by_month as
select
  (dbo.unix_timestamp(dbo.last_day(dbo.from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(capacity) as capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stdev(avg_value) as std_dev
from
  ds_stats_by_day
group by
  year(dbo.from_unixtime(snapshot_time/1000)),
  month(dbo.from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;
go

/* -- -- -- */



create view pm_instances as
select name, display_name ,uuid
from entities
where creation_class = 'PhysicalMachine'
;
go

create view vm_instances as
select name, display_name, uuid
from entities
where creation_class = 'VirtualMachine'
;
go

create view ds_instances as
select name, display_name, uuid
from entities
where creation_class = 'Storage'
;
go



create view pm_util_stats_yesterday as
select * from pm_stats_by_day
  where convert(date,dbo.from_unixtime(snapshot_time/1000)) = dbo.date_sub(convert(date,dbo.now()), 1)
  and property_subtype = 'utilization'
;
go

create view pm_util_info_yesterday as
select
  snapshot_time,
  display_name,
  pm_instances.uuid as uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value
from
  pm_util_stats_yesterday,
  pm_instances
  where pm_util_stats_yesterday.uuid = pm_instances.uuid
;
go


create view vm_util_stats_yesterday as
select * from vm_stats_by_day 
  where convert(date,dbo.from_unixtime(snapshot_time/1000)) = dbo.date_sub(convert(date,dbo.now()), 1)
  and property_subtype = 'utilization'
;
go

create view vm_util_info_yesterday as
select
  snapshot_time,
  display_name,
  vm_instances.uuid as uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value
from
  vm_util_stats_yesterday,
  vm_instances
  where vm_util_stats_yesterday.uuid = vm_instances.uuid
;
go

create view ds_util_stats_yesterday as
select * from ds_stats_by_day 
  where convert(date,dbo.from_unixtime(snapshot_time/1000)) = dbo.date_sub(convert(date,dbo.now()), 1)
  and property_subtype = 'utilization'
;
go

create view ds_util_info_yesterday as
select
  snapshot_time,
  display_name,
  ds_instances.uuid as uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value
from
  ds_util_stats_yesterday,
  ds_instances
  where ds_util_stats_yesterday.uuid = ds_instances.uuid
;
go

/* -- -- -- */

create view pm_capacity_by_hour as
select
  'PhysicalMachine' as class_name,
  display_name,
  pm_stats_by_hour.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity,0) as used_capacity,
  round((1.0-avg_value)*capacity,0) as available_capacity,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
  datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  pm_stats_by_hour, pm_instances
where
  pm_stats_by_hour.uuid = pm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;
go


create view pm_capacity_by_day as
select
  'PhysicalMachine' as class_name,
  pm_stats_by_day.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity,0) as used_capacity,
  round((1.0-avg_value)*capacity,0) as available_capacity,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  pm_stats_by_day, pm_instances
where
  pm_stats_by_day.uuid = pm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;
go


/* --- --- --- */

create view vm_capacity_by_hour as
select
  'VirtualMachine' as class_name,
  vm_stats_by_hour.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity,0) as used_capacity,
  round((1.0-avg_value)*capacity,0) as available_capacity,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
  datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  vm_stats_by_hour, vm_instances
where
  vm_stats_by_hour.uuid = vm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;
go


create view vm_capacity_by_day as
select
  'VirtualMachine' as class_name,
  vm_stats_by_day.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity,0) as used_capacity,
  round((1.0-avg_value)*capacity,0) as available_capacity,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  vm_stats_by_day, vm_instances
where
  vm_stats_by_day.uuid = vm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;
go


/* --- --- --- */

create view ds_capacity_by_hour as
select
  'Storage' as class_name,
  ds_stats_by_hour.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity,0) as used_capacity,
  round((1.0-avg_value)*capacity,0) as available_capacity,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
  datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  ds_stats_by_hour, ds_instances
where
  ds_stats_by_hour.uuid = ds_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;
go


create view ds_capacity_by_day as
select
  'Storage' as class_name,
  ds_stats_by_day.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity,0) as used_capacity,
  round((1.0-avg_value)*capacity,0) as available_capacity,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  ds_stats_by_day, ds_instances
where
  ds_stats_by_day.uuid = ds_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;
go


/* --- --- --- */

create view entity_groups as
select
  entities.id as entity_id,
  max(entities.uuid) as group_uuid,
  max(entities.name) as internal_name,
  max(case entity_attrs.name
    when 'displayName' then entity_attrs.value
    else ''
    end
    ) as group_name,
  max(case entity_attrs.name
    when 'SETypeName' then entity_attrs.value
    else ''
    end
    ) as group_type
from
  entity_attrs, entities  
where
  entity_entity_id = entities.id
  and entities.creation_class = 'Group'
  and entity_attrs.name in ('displayName','SETypeName')
group by
  entities.id
;
go



/* --- --- --- */

create view pm_groups as
select
  entity_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_groups
where
  group_type = 'PhysicalMachine'
;
go

create view vm_groups as
select
  entity_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_groups
where
  group_type = 'VirtualMachine'
;
go

create view ds_groups as
select
  entity_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_groups
where
  group_type = 'Storage'
;
go

create view app_groups as
select
  entity_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_groups
where
  group_type = 'Application'
;
go



/* --- --- --- */

create view entity_group_assns as
select
  id as assn_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_assns left outer join dbo.entity_groups on entity_assns.entity_entity_id = entity_id
where
  entity_assns.name = 'AllGroupMembers'
  and group_name is not null and group_type is not null
;
go

create view pm_group_assns as
select
  id as assn_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_assns left outer join pm_groups on entity_assns.entity_entity_id = entity_id
where
  entity_assns.name = 'AllGroupMembers'
  and group_name is not null and group_type is not null
;
go

create view vm_group_assns as
select
  id as assn_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_assns left outer join vm_groups on entity_assns.entity_entity_id = entity_id
where
  entity_assns.name = 'AllGroupMembers'
  and group_name is not null and group_type is not null
;
go

create view ds_group_assns as
select
  id as assn_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_assns left outer join ds_groups on entity_assns.entity_entity_id = entity_id
where
  entity_assns.name = 'AllGroupMembers'
  and group_name is not null and group_type is not null
;
go



/* --- --- --- */

create view entity_group_members as
select
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
  entity_group_assns
    left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
      left outer join entities on entities.id = entity_dest_id
where
  uuid is not null
;
go

create view pm_group_members as
select
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
  pm_group_assns
    left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
      left outer join entities on entities.id = entity_dest_id
where
  uuid is not null
;
go

create view vm_group_members as
select
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
  vm_group_assns
    left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
      left outer join entities on entities.id = entity_dest_id
where
  uuid is not null
;
go

create view vm_group_members_agg as
select
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
  vm_group_assns
    left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
      left outer join entities on entities.id = entity_dest_id
where
  uuid is not null
  and group_name like 'VMs_%'
  and internal_name not like '%VMsByStorage%'
  and internal_name not like '%VMsByNetwork%'
;
go

create view ds_group_members as
select
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
  ds_group_assns
    left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
      left outer join entities on entities.id = entity_dest_id
where
  uuid is not null
;
go


/* --- --- --- */

create view pm_capacity_by_day_per_pm_group as
select *
from
  pm_capacity_by_day, pm_group_members
where
  member_uuid = uuid
;
go

create view vm_capacity_by_day_per_vm_group as
select *
from
  vm_capacity_by_day, vm_group_members
where
  member_uuid = uuid
;
go

create view ds_capacity_by_day_per_ds_group as
select *
from
  ds_capacity_by_day, ds_group_members
where
  member_uuid = uuid
;
go

/* --- --- --- */

create view pm_stats_by_hour_per_pm_group as
select *
from
  pm_group_members, pm_stats_by_hour
where
  member_uuid = uuid
;
go

create view vm_stats_by_hour_per_vm_group as
select *
from
  vm_group_members, vm_stats_by_hour
where
  member_uuid = uuid
;
go

create view vm_stats_by_hour_per_vm_group_agg as
select *
from
  vm_group_members_agg, vm_stats_by_hour
where
  member_uuid = uuid
;
go

create view ds_stats_by_hour_per_ds_group as
select *
from
  ds_group_members, ds_stats_by_hour
where
  member_uuid = uuid
;
go


/* --- --- --- */

create view pm_stats_by_day_per_pm_group as
select *
from
  pm_group_members, pm_stats_by_day
where
  member_uuid = uuid
;
go

create view vm_stats_by_day_per_vm_group as
select *
from
  vm_group_members, vm_stats_by_day
where
  member_uuid = uuid
;
go

create view vm_stats_by_day_per_vm_group_agg as
select *
from
  vm_group_members_agg, vm_stats_by_day
where
  member_uuid = uuid
;
go

create view ds_stats_by_day_per_ds_group as
select *
from
  ds_group_members, ds_stats_by_day
where
  member_uuid = uuid
;
go


/* --- --- --- views supporting custom reports --- --- --- */

create view user_pm_stats_by_hour as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
    datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  pm_stats_by_hour, pm_instances
where
  pm_stats_by_hour.uuid = pm_instances.uuid
;
go

create view user_vm_stats_by_hour as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
    datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  vm_stats_by_hour, vm_instances
where
  vm_stats_by_hour.uuid = vm_instances.uuid
;
go

create view user_vm_stats_by_hour_per_group as
select
    group_name,
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
    datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  vm_stats_by_hour, vm_group_members
where
  member_uuid = uuid
;
go

create view user_ds_stats_by_hour as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
    datepart(hour,dbo.from_unixtime(snapshot_time/1000)) as hour_number
from
  ds_stats_by_hour, ds_instances
where
  ds_stats_by_hour.uuid = ds_instances.uuid
;
go


/* by day versions */


create view user_pm_stats_by_day as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  pm_stats_by_day, pm_instances
where
  pm_stats_by_day.uuid = pm_instances.uuid
;
go

create view user_vm_stats_by_day as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  vm_stats_by_day, vm_instances
where
  vm_stats_by_day.uuid = vm_instances.uuid
;
go

create view user_vm_stats_by_day_per_group as
select
    group_name,
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  vm_stats_by_day, vm_group_members
where
  member_uuid = uuid
;
go

create view user_ds_stats_by_day as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on
from
  ds_stats_by_day, ds_instances
where
  ds_stats_by_day.uuid = ds_instances.uuid
;
go

/* --- --- --- */

create view active_vms as
select distinct
  uuid
from 
  vm_stats_by_hour
where
  property_type = 'VCPU'
  and property_subtype = 'utilization'
  and max_value >= 0.010
;
go

/* --- --- --- */

create view vm_storage_used_by_day_per_vm_group as
select
  max(group_uuid) as uuid,
  group_name,
  max(group_type) as group_type,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
  max(property_type) as property_type,
  max(property_subtype) as property_subtype,
  round(sum(avg_value),0) as storage_used
from
  vm_stats_by_day_per_vm_group_agg
where
  property_type = 'StorageAmount'
  and property_subtype = 'used'
group by
  group_name, convert(date,dbo.from_unixtime(snapshot_time/1000))
;
go


/* --- --- --- */

create view pm_vm_count_by_day_per_pm_group as
select
  max(group_uuid) as group_uuid,
  group_name,
  max(group_type) as group_type,
  convert(date,dbo.from_unixtime(snapshot_time/1000)) as recorded_on,
  'HostedVMs' as property_type,
  'count' as property_subtype,
  round(sum(avg_value),0) as vm_count
from
  pm_stats_by_day_per_pm_group
where
  group_type = 'PhysicalMachine'
  and property_type = 'Produces'
group by
  group_name, convert(date,dbo.from_unixtime(snapshot_time/1000))
;
go


create view cluster_membership as
select
    dbo.date_sub(convert(date,dbo.now()), 1) as recorded_on,
    group_uuid,
    internal_name,
    group_name,
    group_type,
    member_uuid,
    display_name
from
    pm_group_members
where
    internal_name like 'GROUP-%ByCluster%'

union all

select
    dbo.date_sub(convert(date,dbo.now()), 1) as recorded_on,
    group_uuid,
    internal_name,
    group_name,
    group_type,
    member_uuid,
    display_name
from
    vm_group_members
where
    internal_name like 'GROUP-%ByCluster%'
;
go
