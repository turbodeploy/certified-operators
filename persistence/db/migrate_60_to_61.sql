use vmtdb ;

start transaction ;

source /srv/rails/webapps/persistence/db/views.sql;
source /srv/rails/webapps/persistence/db/functions.sql;
source /srv/rails/webapps/persistence/db/clusterAggPreviousDay.sql;

/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,61) ;

commit ;