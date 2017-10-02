

use vmtdb ;

start transaction ;

create or replace view users as
select 
	entities.id, 
	entities.display_name as login, 
	entities.name as name, 
	NULL as email, 
	NULL as crypted_password, 
	'' as salt, 
	entities.created_at as created_at, 
	entities.updated_at as updated_at, 
	NULL as remember_token, 
	NULL as remember_token_expires_at 
from 
	entities, 
	entity_assns, 
	entity_assns_members_entities 
where 
	entity_assns.name = 'users'
	and entity_assns_members_entities.entity_assn_src_id=entity_assns.id
	and entity_assns_members_entities.entity_dest_id = entities.id
;


delete from version_info ;
insert into version_info values (1,38) ;


commit ;
