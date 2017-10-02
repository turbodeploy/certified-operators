
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
  entities.uuid as remember_token, 
  NULL as remember_token_expires_at 
from 
  entities
where 
  creation_class='User'
;


