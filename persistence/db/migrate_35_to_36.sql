

use vmtdb ;

start transaction ;

create or replace view cluster_members_end_of_month as
select * from cluster_members where recorded_on=end_of_month(recorded_on)
;




delete from version_info ;
insert into version_info values (1,36) ;


commit ;
