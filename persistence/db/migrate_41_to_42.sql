

use vmtdb ;

start transaction ;

update standard_reports set title='Monthly Socket Audit Report' where id=189;

delete from version_info ;
insert into version_info values (1,42) ;

commit ;
