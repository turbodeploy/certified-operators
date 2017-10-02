use vmtdb ;

source verify_db_charset_and_collation.sql;

/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,54) ;