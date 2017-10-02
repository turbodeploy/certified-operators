
SET sql_mode='ANSI_QUOTES';

use vmtdb ;


/*
    ---------------------------------------------------------------
        Views
    ---------------------------------------------------------------
*/


create or replace view pm_summary_stats_by_day as
select
  date(snapshot_time) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  pm_stats_by_hour
group by
  date(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;


create or replace view vm_summary_stats_by_day as
select
  date(snapshot_time) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  vm_stats_by_hour
group by
  date(snapshot_time),
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  commodity_key
;


create or replace view ds_summary_stats_by_day as
select
  date(snapshot_time) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  ds_stats_by_hour
group by
  date(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;

create or replace view vdc_summary_stats_by_day as
select
  date(snapshot_time) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  vdc_stats_by_hour
group by
  date(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;

create or replace view app_summary_stats_by_day as
select
  date(snapshot_time) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  app_stats_by_hour
group by
  date(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;



/* -- -- -- */

create or replace view pm_summary_stats_by_month as
select
  last_day(max(snapshot_time)) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  pm_stats_by_day
group by
  year(snapshot_time),
  month(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;


create or replace view vm_summary_stats_by_month as
select
  last_day(max(snapshot_time)) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  vm_stats_by_day
group by
  year(snapshot_time),
  month(snapshot_time),
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  commodity_key
;


create or replace view ds_summary_stats_by_month as
select
  last_day(max(snapshot_time)) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  ds_stats_by_day
group by
  year(snapshot_time),
  month(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;

create or replace view vdc_summary_stats_by_month as
select
  last_day(max(snapshot_time)) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  vdc_stats_by_day
group by
  year(snapshot_time),
  month(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;

create or replace view app_summary_stats_by_month as
select
  last_day(max(snapshot_time)) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  app_stats_by_day
group by
  year(snapshot_time),
  month(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;

source /srv/rails/webapps/persistence/db/view_da_summary_stats_by_day.sql;
source /srv/rails/webapps/persistence/db/view_sc_summary_stats_by_day.sql;
source /srv/rails/webapps/persistence/db/view_sw_summary_stats_by_day.sql;
source /srv/rails/webapps/persistence/db/view_iom_summary_stats_by_day.sql;
source /srv/rails/webapps/persistence/db/view_ch_summary_stats_by_day.sql;

source /srv/rails/webapps/persistence/db/view_da_summary_stats_by_month.sql;
source /srv/rails/webapps/persistence/db/view_sc_summary_stats_by_month.sql;
source /srv/rails/webapps/persistence/db/view_sw_summary_stats_by_month.sql;
source /srv/rails/webapps/persistence/db/view_iom_summary_stats_by_month.sql;
source /srv/rails/webapps/persistence/db/view_ch_summary_stats_by_month.sql;


/*
 * Cluster stats
 */
create or replace view cluster_summary_stats_by_month as
select
  start_of_month(recorded_on) as recorded_on,
  internal_name,
  property_type,
  property_subtype,
  if(property_subtype='utilization', avg(value), (if(property_subtype='numCPUs' OR property_subtype='numSockets', round(max(value)), round(avg(value))))) as value
from
  cluster_stats_by_day
where 
	recorded_on between start_of_month(date_sub(now(), interval 1 month)) and end_of_month(date_sub(now(), interval 1 month))
group by
  internal_name,
  year(recorded_on),
  month(recorded_on),
  property_type,
  property_subtype
order by recorded_on desc, property_type
;

/* -- -- -- */



create or replace view pm_instances as
select name, display_name ,uuid
from entities
where creation_class = 'PhysicalMachine'
;

create or replace view vm_instances as
select name, display_name, uuid
from entities
where creation_class = 'VirtualMachine'
;

create or replace view ds_instances as
select name, display_name, uuid
from entities
where creation_class = 'Storage'
;



create or replace view pm_util_stats_yesterday as
select * from pm_stats_by_day
  where date(snapshot_time) = date_sub(date(now()), interval 1 day)
  and property_subtype = 'utilization'
;

create or replace view pm_util_info_yesterday as
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


create or replace view vm_util_stats_yesterday as
select * from vm_stats_by_day 
  where date(snapshot_time) = date_sub(date(now()), interval 1 day)
  and property_subtype = 'utilization'
;

create or replace view vm_util_info_yesterday as
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


create or replace view ds_util_stats_yesterday as
select * from ds_stats_by_day 
  where date(snapshot_time) = date_sub(date(now()), interval 1 day)
  and property_subtype = 'utilization'
;

create or replace view ds_util_info_yesterday as
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

/* -- -- -- */

create or replace view pm_capacity_by_hour as
select
  'PhysicalMachine' as class_name,
  display_name,
  pm_stats_by_hour.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity) as used_capacity,
  round((1.0-avg_value)*capacity) as available_capacity,
  date(snapshot_time) as recorded_on,
  hour(snapshot_time) as hour_number
from
  pm_stats_by_hour, pm_instances
where
  pm_stats_by_hour.uuid = pm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;


create or replace view pm_capacity_by_day as
select
  'PhysicalMachine' as class_name,
  pm_stats_by_day.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity) as used_capacity,
  round((1.0-avg_value)*capacity) as available_capacity,
  date(snapshot_time) as recorded_on
from
  pm_stats_by_day, pm_instances
where
  pm_stats_by_day.uuid = pm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;


/* --- --- --- */

create or replace view vm_capacity_by_hour as
select
  'VirtualMachine' as class_name,
  vm_stats_by_hour.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity) as used_capacity,
  round((1.0-avg_value)*capacity) as available_capacity,
  date(snapshot_time) as recorded_on,
  hour(snapshot_time) as hour_number
from
  vm_stats_by_hour, vm_instances
where
  vm_stats_by_hour.uuid = vm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;


create or replace view vm_capacity_by_day as
select
  'VirtualMachine' as class_name,
  vm_stats_by_day.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity) as used_capacity,
  round((1.0-avg_value)*capacity) as available_capacity,
  date(snapshot_time) as recorded_on
from
  vm_stats_by_day, vm_instances
where
  vm_stats_by_day.uuid = vm_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;


/* --- --- --- */

create or replace view ds_capacity_by_hour as
select
  'Storage' as class_name,
  ds_stats_by_hour.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity) as used_capacity,
  round((1.0-avg_value)*capacity) as available_capacity,
  date(snapshot_time) as recorded_on,
  hour(snapshot_time) as hour_number
from
  ds_stats_by_hour, ds_instances
where
  ds_stats_by_hour.uuid = ds_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;


create or replace view ds_capacity_by_day as
select
  'Storage' as class_name,
  ds_stats_by_day.uuid as uuid,
  producer_uuid,
  property_type,
  avg_value as utilization,
  capacity,
  round(avg_value*capacity) as used_capacity,
  round((1.0-avg_value)*capacity) as available_capacity,
  date(snapshot_time) as recorded_on
from
  ds_stats_by_day, ds_instances
where
  ds_stats_by_day.uuid = ds_instances.uuid
  and property_subtype = 'utilization'
  and capacity > 0.00
;


/* --- --- --- */

create or replace view entity_groups as
select
  entities.id as entity_id,
  entities.uuid as group_uuid,
  entities.name as internal_name,
  max(if(entity_attrs.name = 'displayName', entity_attrs.value,'')) as group_name,
  max(if(entity_attrs.name = 'SETypeName',  entity_attrs.value,'')) as group_type
from
  entity_attrs left outer join entities on entity_entity_id = entities.id
where
  entities.creation_class = 'Group'
  and entity_attrs.name in ('displayName','SETypeName')
group by
  entities.id
having
  group_name is not null and group_name <> ''
;



/* --- --- --- */

create or replace view pm_groups as
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

create or replace view vm_groups as
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

create or replace view ds_groups as
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

create or replace view app_groups as
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



/* --- --- --- */

create or replace view entity_group_assns as
select
  id as assn_id,
  group_uuid,
  internal_name,
  group_name,
  group_type
from
  entity_assns left outer join entity_groups on entity_assns.entity_entity_id = entity_id
where
  entity_assns.name = 'AllGroupMembers'
  and group_name is not null and group_type is not null
;

create or replace view pm_group_assns as
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

create or replace view vm_group_assns as
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

create or replace view ds_group_assns as
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



/* --- --- --- */

create or replace view entity_group_members_helper as
select entity_dest_id, group_uuid, internal_name, group_name, group_type
from entity_group_assns left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
;

create or replace view entity_group_members as
select distinct
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
	entity_group_members_helper
	join entities on entities.id = entity_dest_id
where
  uuid is not null
;

create or replace view pm_group_members_helper as
select entity_dest_id, group_uuid, internal_name, group_name, group_type
from pm_group_assns left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
;

create or replace view pm_group_members as
select distinct
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
	pm_group_members_helper
	join entities on entities.id = entity_dest_id
where
  uuid is not null
  and creation_class='PhysicalMachine'
;

create or replace view vm_group_members_helper as
select entity_dest_id, group_uuid, internal_name, group_name, group_type
from vm_group_assns left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
;

create or replace view vm_group_members as
select distinct
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
  	vm_group_members_helper
	join entities on entities.id = entity_dest_id
where
  uuid is not null
  and creation_class='VirtualMachine'
;

create or replace view vm_group_members_agg as
select distinct
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
    vm_group_members_helper
	join entities on entities.id = entity_dest_id
where
  uuid is not null
  and group_name like 'VMs_%'
  and internal_name not like '%VMsByStorage%'
  and internal_name not like '%VMsByNetwork%'
  and creation_class='VirtualMachine'
;

create or replace view ds_group_members_helper as
select entity_dest_id, group_uuid, internal_name, group_name, group_type
from ds_group_assns left outer join entity_assns_members_entities on assn_id = entity_assn_src_id
;

create or replace view ds_group_members as
select distinct
  group_uuid,
  internal_name,
  group_name,
  group_type,
  uuid as member_uuid,
  display_name
from
	ds_group_members_helper
	join entities on entities.id = entity_dest_id
where
  uuid is not null
  and creation_class='Storage'
;


/* --- --- --- */

create or replace view pm_capacity_by_day_per_pm_group as
select *
from
  pm_capacity_by_day, pm_group_members
where
  member_uuid = uuid
;

create or replace view vm_capacity_by_day_per_vm_group as
select *
from
  vm_capacity_by_day, vm_group_members
where
  member_uuid = uuid
;

create or replace view ds_capacity_by_day_per_ds_group as
select *
from
  ds_capacity_by_day, ds_group_members
where
  member_uuid = uuid
;

/* --- --- --- */

create or replace view pm_stats_by_hour_per_pm_group as
select *
from
  pm_group_members, pm_stats_by_hour
where
  member_uuid = uuid
;

create or replace view vm_stats_by_hour_per_vm_group as
select *
from
  vm_group_members, vm_stats_by_hour
where
  member_uuid = uuid
;

create or replace view vm_stats_by_hour_per_vm_group_agg as
select *
from
  vm_group_members_agg, vm_stats_by_hour
where
  member_uuid = uuid
;

create or replace view ds_stats_by_hour_per_ds_group as
select *
from
  ds_group_members, ds_stats_by_hour
where
  member_uuid = uuid
;


/* --- --- --- */

create or replace view pm_stats_by_day_per_pm_group as
select *
from
  pm_group_members, pm_stats_by_day
where
  member_uuid = uuid
;

create or replace view vm_stats_by_day_per_vm_group as
select *
from
  vm_group_members, vm_stats_by_day
where
  member_uuid = uuid
;

create or replace view vm_stats_by_day_per_vm_group_agg as
select *
from
  vm_group_members_agg, vm_stats_by_day
where
  member_uuid = uuid
;

create or replace view ds_stats_by_day_per_ds_group as
select *
from
  ds_group_members, ds_stats_by_day
where
  member_uuid = uuid
;


/* --- --- --- views supporting custom reports --- --- --- */

create or replace view user_pm_stats_by_hour as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on,
    hour(snapshot_time) as hour_number
from
  pm_stats_by_hour, pm_instances
where
  pm_stats_by_hour.uuid = pm_instances.uuid
;

create or replace view user_vm_stats_by_hour as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on,
    hour(snapshot_time) as hour_number
from
  vm_stats_by_hour, vm_instances
where
  vm_stats_by_hour.uuid = vm_instances.uuid
;

create or replace view user_vm_stats_by_hour_per_group as
select
    group_name,
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on,
    hour(snapshot_time) as hour_number
from
  vm_stats_by_hour, vm_group_members
where
  member_uuid = uuid
;

create or replace view user_ds_stats_by_hour as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on,
    hour(snapshot_time) as hour_number
from
  ds_stats_by_hour, ds_instances
where
  ds_stats_by_hour.uuid = ds_instances.uuid
;


/* by day versions */


create or replace view user_pm_stats_by_day as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on
from
  pm_stats_by_day, pm_instances
where
  pm_stats_by_day.uuid = pm_instances.uuid
;

create or replace view user_vm_stats_by_day as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on
from
  vm_stats_by_day, vm_instances
where
  vm_stats_by_day.uuid = vm_instances.uuid
;

create or replace view user_vm_stats_by_day_per_group as
select
    group_name,
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on
from
  vm_stats_by_day, vm_group_members
where
  member_uuid = uuid
;

create or replace view user_ds_stats_by_day as
select
    display_name as instance_name,
    property_type,
    property_subtype,
    capacity,
    avg_value,
    min_value,
    max_value,
    date(snapshot_time) as recorded_on
from
  ds_stats_by_day, ds_instances
where
  ds_stats_by_day.uuid = ds_instances.uuid
;

/* --- --- --- */

create or replace view active_vms as
select distinct
  uuid
from 
  vm_stats_by_hour
where
  property_type = 'VCPU'
  and property_subtype = 'utilization'
  and max_value >= 0.010
;

/* --- --- --- */

create or replace view vm_storage_used_by_day_per_vm_group as
select
  group_uuid,
  group_name,
  group_type,
  date(snapshot_time) as recorded_on,
  property_type,
  property_subtype,
  round(sum(avg_value)) as storage_used
from
  vm_stats_by_day_per_vm_group_agg
where
  property_type = 'StorageAmount'
  and property_subtype = 'used'
group by
  group_name, recorded_on
order by
  group_name, recorded_on
;


/* --- --- --- */

create or replace view pm_vm_count_by_day_per_pm_group as
select
  group_uuid,
  group_name,
  group_type,
  date(snapshot_time) as recorded_on,
  'HostedVMs' as property_type,
  'count' as property_subtype,
  round(sum(avg_value)) as vm_count
from
  pm_stats_by_day_per_pm_group
where
  group_type = 'PhysicalMachine'
  and property_type = 'Produces'
group by
  group_name, recorded_on
order by
  group_name, recorded_on
;

create or replace view cluster_membership_helper as
select distinct 
	entities.uuid, entity_attrs.value as val
from  
	entity_attrs join entities
	on (entity_attrs.entity_entity_id=entities.id)
where entity_attrs.name='groupStatsEnabled' and entity_attrs.value='true';

create or replace view cluster_membership as
select distinct
    date_sub(date(now()), interval 1 day) as recorded_on,
    group_uuid,
    internal_name,
    group_name,
    group_type,
    member_uuid,
    display_name
from
    pm_group_members
left join
	cluster_membership_helper
on (pm_group_members.group_uuid = cluster_membership_helper.uuid)
where
    (internal_name like 'GROUP-%ByCluster%') or (cluster_membership_helper.uuid is not null)

union all

select distinct
    date_sub(date(now()), interval 1 day) as recorded_on,
    group_uuid,
    internal_name,
    group_name,
    group_type,
    member_uuid,
    display_name
from
    vm_group_members
left join
	cluster_membership_helper
on (vm_group_members.group_uuid = cluster_membership_helper.uuid)
where
    (internal_name like 'GROUP-%ByCluster%') or (cluster_membership_helper.uuid is not null)
;

create or replace view cluster_members_end_of_month as
select * from cluster_members where recorded_on=end_of_month(recorded_on)
;

create or replace view cluster_members_yesterday as
select * from cluster_members where recorded_on=date(date_sub(now(), interval 1 day))
;

create or replace view users as
select 
  entities.id, 
  entities.display_name as login, 
  entities.name as name, 
  NULL as email, 
  NULL as crypted_password, 
  '' as salt,
  entities.uuid as remember_token, 
  NULL as remember_token_expires_at 
from 
  entities
where 
  creation_class='User'
;

/*
    Experimental
*/

create or replace view user_vm_storage as
select
    vm_instances.display_name as vm_name,
    ds_instances.display_name as ds_name,
    avg_value as storage_amount
from
  vm_stats_by_day
    left outer join vm_instances on vm_stats_by_day.uuid = vm_instances.uuid
        left outer join ds_instances on vm_stats_by_day.producer_uuid = ds_instances.uuid
where
  date(snapshot_time) = date_sub(date(now()), interval 1 day)
  and property_type = 'StorageAmount' and property_subtype = 'used'
;


delimiter //

DROP FUNCTION IF EXISTS start_of_day //
CREATE FUNCTION start_of_day(ref_date date) RETURNS date DETERMINISTIC
BEGIN
    return date(date_format(ref_date, "%Y-%m-%d 00:00:00")) ;
END //


DROP FUNCTION IF EXISTS end_of_day //
CREATE FUNCTION end_of_day(ref_date date) RETURNS date DETERMINISTIC
BEGIN
    return date(date_add(date_sub(date_format(ref_date, "%Y-%m-%d %H:00:00"), interval 1 second), interval 1 day)) ;
END //




DROP FUNCTION IF EXISTS start_of_hour //
CREATE FUNCTION start_of_hour(ref_date timestamp) RETURNS timestamp DETERMINISTIC
BEGIN
    return timestamp(date_format(ref_date, "%Y-%m-%d %H:00:00")) ;
END //


DROP FUNCTION IF EXISTS end_of_hour //
CREATE FUNCTION end_of_hour(ref_date timestamp) RETURNS timestamp DETERMINISTIC
BEGIN
    return timestamp(date_add(date_sub(date_format(ref_date, "%Y-%m-%d %H:00:00"), interval 1 second), interval 1 hour)) ;
END //

delimiter ;



/*
 * FROM EACH DAY, move to genViews/genFunctions
 */
delimiter //

DROP FUNCTION IF EXISTS ftn_vm_count_for_month //
CREATE FUNCTION ftn_vm_count_for_month (month_day_1 date) RETURNS int DETERMINISTIC
BEGIN

    set @ms_day_1 := start_of_month_ms(month_day_1) ;
    set @ms_day_n := end_of_month_ms(month_day_1) ;

    set @count := (select count(uuid) as n_vms
                    from (select distinct uuid from vm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END //

DROP FUNCTION IF EXISTS ftn_pm_count_for_month //
CREATE FUNCTION ftn_pm_count_for_month (month_day_1 date) RETURNS int DETERMINISTIC
BEGIN

    set @ms_day_1 := start_of_month_ms(month_day_1) ;
    set @ms_day_n := end_of_month_ms(month_day_1) ;

    set @count := (select count(uuid) as n_pms
                    from (select distinct uuid from pm_stats_by_day
                            where
                            property_type = 'priceIndex'
                            and snapshot_time between @ms_day_1 and @ms_day_n
                         ) as uuids
                  ) ;

    return @count ;

END //


DROP FUNCTION IF EXISTS cluster_nm1_factor //
CREATE FUNCTION cluster_nm1_factor(arg_group_name varchar(255)) RETURNS float DETERMINISTIC
BEGIN
    set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;

    IF @n_hosts > 0 THEN
      set @factor := (@n_hosts-1.0) / @n_hosts ;
    ELSE
      set @factor := 0 ;
    END IF ;

    return @factor ;
END //

DROP FUNCTION IF EXISTS cluster_nm2_factor //
CREATE FUNCTION cluster_nm2_factor(arg_group_name varchar(255)) RETURNS float DETERMINISTIC
BEGIN
    set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;

    IF @n_hosts > 1 THEN
      set @factor := (@n_hosts-2.0) / @n_hosts ;
    ELSE
      set @factor := 0 ;
    END IF ;

    return @factor ;
END //

delimiter ;
