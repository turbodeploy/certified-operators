use vmtdb ;

start transaction ;

source /srv/rails/webapps/persistence/db/stored_procedures.sql;

/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,56) ;

commit ;
