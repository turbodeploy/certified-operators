

use vmtdb ;

start transaction ;


create or replace view entity_group_members as
select distinct
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

create or replace view pm_group_members as
select distinct
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

create or replace view vm_group_members as
select distinct
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

create or replace view vm_group_members_agg as
select distinct
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

create or replace view ds_group_members as
select distinct
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


delete from version_info ;
insert into version_info values (1,40) ;

commit ;
