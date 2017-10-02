
use vmtdb ;

start transaction ;

create index pm_bd_snapshot_time_idx    on pm_stats_by_day (snapshot_time);
create index vm_bd_snapshot_time_idx    on vm_stats_by_day (snapshot_time);
create index ds_bd_snapshot_time_idx    on ds_stats_by_day (snapshot_time);

delete from version_info ;
insert into version_info values (1,24) ;

commit ;
