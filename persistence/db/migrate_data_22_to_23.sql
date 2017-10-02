

/*

select distinct property_name from snapshots_stats_by_hour
where class_name = 'PhysicalMachine'
and property_subtype = 'used'

*/

select 'pm stats by hour part 1' as progress_step ;

insert into pm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'utilization' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'PhysicalMachine'
  and property_subtype in ('capacity','utilization')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'pm stats by hour part 2' as progress_step ;

insert into pm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'used' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'PhysicalMachine'
  and property_type like 'Q%VCPU'
  and property_subtype in ('capacity','used')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'pm stats by hour part 3' as progress_step ;

insert into pm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  null as capacity,
  avg_property_value as avg_value,
  min_property_value as min_value,
  max_property_value as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'PhysicalMachine'
  and property_type in ('priceIndex','produces')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;


/*

select distinct property_name from snapshots_stats_by_hour
where class_name = 'VirtualMachine'
and property_subtype = 'used'

*/


select 'vm stats by hour part 1' as progress_step ;

insert into vm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'utilization' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'VirtualMachine'
  and property_type in ('VCPU','VMem')
  and property_subtype in ('capacity','utilization')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'vm stats by hour part 2' as progress_step ;

insert into vm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  replace(replace(property_type,'Q',''),'VCPU','')*20000 as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'VirtualMachine'
  and property_type like 'Q%VCPU'
  and property_subtype in ('used')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'vm stats by hour part 3' as progress_step ;

insert into vm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0))  as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'VirtualMachine'
  and property_type not like 'Q%VCPU'
  and property_subtype in ('used','unused','conf','swap','snapshot','log','disk')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;

select 'vm stats by hour part 4' as progress_step ;

insert into vm_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  null as capacity,
  avg_property_value as avg_value,
  min_property_value as min_value,
  max_property_value as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'VirtualMachine'
  and property_type = 'priceIndex'
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;


/*

select distinct property_name from snapshots_stats_by_hour
where class_name = 'Storage'
and property_subtype = 'used'

*/

select 'ds stats by hour part 1' as progress_step ;

insert into ds_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'utilization' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'Storage'
  and property_type in ('StorageAmount','StorageAccess','StorageLatency')
  and property_subtype in ('capacity','utilization')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'ds stats by hour part 2' as progress_step ;

insert into ds_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'used' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'Storage'
  and property_type in ('StorageAccess','StorageLatency')
  and property_subtype in ('capacity','used')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'ds stats by hour part 3' as progress_step ;

insert into ds_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0))  as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'Storage'
  and property_type = 'StorageAmount'
  and property_subtype in ('used','unused','conf','swap','snapshot','log','disk')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;

select 'ds stats by hour part 4' as progress_step ;

insert into ds_stats_by_hour
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  null as capacity,
  avg_property_value as avg_value,
  min_property_value as min_value,
  max_property_value as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_hour
where
  class_name = 'Storage'
  and property_type in ('Produces','priceIndex')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;



/*
    ==========================================================================
*/



/*

select distinct property_name from snapshots_stats_by_day
where class_name = 'PhysicalMachine'
and property_subtype = 'used'

*/

select 'pm stats by day part 1' as progress_step ;

insert into pm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'utilization' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'PhysicalMachine'
  and property_subtype in ('capacity','utilization')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'pm stats by day part 2' as progress_step ;

insert into pm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'used' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'PhysicalMachine'
  and property_type like 'Q%VCPU'
  and property_subtype in ('capacity','used')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'pm stats by day part 3' as progress_step ;

insert into pm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  null as capacity,
  avg_property_value as avg_value,
  min_property_value as min_value,
  max_property_value as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'PhysicalMachine'
  and property_type in ('priceIndex','produces')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;


/*

select distinct property_name from snapshots_stats_by_day
where class_name = 'VirtualMachine'
and property_subtype = 'used'

*/


select 'vm stats by day part 1' as progress_step ;

insert into vm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'utilization' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'VirtualMachine'
  and property_type in ('VCPU','VMem')
  and property_subtype in ('capacity','utilization')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'vm stats by day part 2' as progress_step ;

insert into vm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  replace(replace(property_type,'Q',''),'VCPU','')*20000 as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'VirtualMachine'
  and property_type like 'Q%VCPU'
  and property_subtype in ('used')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'vm stats by day part 3' as progress_step ;

insert into vm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0))  as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'VirtualMachine'
  and property_type not like 'Q%VCPU'
  and property_subtype in ('used','unused','conf','swap','snapshot','log','disk')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;

select 'vm stats by day part 4' as progress_step ;

insert into vm_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  null as capacity,
  avg_property_value as avg_value,
  min_property_value as min_value,
  max_property_value as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'VirtualMachine'
  and property_type = 'priceIndex'
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;


/*

select distinct property_name from snapshots_stats_by_day
where class_name = 'Storage'
and property_subtype = 'used'

*/

select 'ds stats by day part 1' as progress_step ;

insert into ds_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'utilization' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'Storage'
  and property_type in ('StorageAmount','StorageAccess','StorageLatency')
  and property_subtype in ('capacity','utilization')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'ds stats by day part 2' as progress_step ;

insert into ds_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  'used' as property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0)) as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'Storage'
  and property_type in ('StorageAccess','StorageLatency')
  and property_subtype in ('capacity','used')
group by
  date(from_unixtime(snapshot_time/1000)),
  hour(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;

select 'ds stats by day part 3' as progress_step ;

insert into ds_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  max(if(property_subtype = 'capacity',avg_property_value,0))  as capacity,
  max(if(property_subtype <> 'capacity',avg_property_value,0)) as avg_value,
  max(if(property_subtype <> 'capacity',min_property_value,0)) as min_value,
  max(if(property_subtype <> 'capacity',max_property_value,0)) as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'Storage'
  and property_type = 'StorageAmount'
  and property_subtype in ('used','unused','conf','swap','snapshot','log','disk')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;

select 'ds stats by day part 4' as progress_step ;

insert into ds_stats_by_day
select
  snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  null as capacity,
  avg_property_value as avg_value,
  min_property_value as min_value,
  max_property_value as max_value,
  0.00 as std_dev
from
  snapshots_stats_by_day
where
  class_name = 'Storage'
  and property_type in ('Produces','priceIndex')
group by
  date(from_unixtime(snapshot_time/1000)),
  uuid,
  property_type
;


/* -- -- -- */

update entities, (select distinct uuid, instance_name from snapshots_stats_by_day) as snapshots
    set entities.display_name = snapshots.instance_name
    where convert(entities.uuid using utf8) = convert(snapshots.uuid using utf8)
;
