
use vmtdb ;

drop table if exists version_info ;
create table version_info (
   version int
);

delete from version_info ;
insert into version_info values (19) ;
