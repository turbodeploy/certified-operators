use vmtdb ;

start transaction ;

source stored_procedures.sql;

/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,53) ;

commit ;
