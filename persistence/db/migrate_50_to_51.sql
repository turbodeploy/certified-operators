use vmtdb ;

start transaction ;

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

/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,51) ;

commit ;
