

use vmtdb ;

start transaction ;


/*
 * 
 * DATA MIGRATION
 * 
 */

/* Helper Procedures: */
/* DAILY */
delimiter //
drop procedure if exists migrate_clusterAgg_Enterprise//
create procedure migrate_clusterAgg_Enterprise()
begin

/* Default to populate all of the available previous days */
set @last_stats = IFNULL((select min(recorded_on) from cluster_stats_by_day where internal_name='GROUP-PMsByCluster'), (select date(date_sub(now(), interval 1 day)) as d));

insert into cluster_stats_by_day 
select
	day_date as recorded_on,
	'GROUP-PMsByCluster' as internal_name,
	if(property_type='numCPUs', 'numCores',property_type) as property_type,
	property_subtype,
	if(property_subtype='utilization', value, sum(value)) as value
from

(
select distinct
	recorded_on as day_date, 'Host' as property_type, 'numHosts' as property_subtype, count(member_uuid) as value 
from 
	cluster_members 
where 
	recorded_on < @last_stats
and group_type='PhysicalMachine'
group by recorded_on

union 

/* Memory capacity */
select date(concat(year, month, day)) as day_date, 'Mem' as property_type, 'capacity' as property_subtype, sum(mem)/1024/1024 as value 
from
  ( select 
    date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    uuid,
    max(capacity) as mem
    from 
    pm_stats_by_day
    where date(from_unixtime(snapshot_time/1000)) < @last_stats
       and property_type = 'Mem'
       and property_subtype = 'utilization'
    group by 
      year, month, day, uuid
  ) as pm_stats_by_day
natural join
  (select 
		member_uuid as uuid, 
		day(recorded_on) as day, 
		month(recorded_on) as month, 
		year(recorded_on) as year
    from 
		cluster_members 
	where 
		recorded_on < @last_stats
	and group_type='PhysicalMachine'
  ) as members
group by
  year, month, day

union 

/* Mem and CPU utilization average */
select date(concat(year, month, day)) as day_date, property_type, property_subtype, avg(mem) as value 
from
  ( select 
    date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    uuid, property_type, property_subtype,
    avg(avg_value) as mem
    from 
    pm_stats_by_day
    where date(from_unixtime(snapshot_time/1000)) < @last_stats
       and property_type in ('Mem', 'CPU')
       and property_subtype = 'utilization'
    group by 
      year, month, day, uuid, property_type
  ) as pm_stats_by_day
natural join
  (select 
		member_uuid as uuid, 
		day(recorded_on) as day, 
		month(recorded_on) as month, 
		year(recorded_on) as year
    from 
		cluster_members 
	where 
		recorded_on < @last_stats
	and group_type='PhysicalMachine'
   ) as members
group by
  year, month, day, property_type

union 

/* VM Count */
select 
	distinct
	recorded_on as day_date, 'VM' as property_type, 'numVMs' as property_subtype, count(member_uuid) as value 
from 
	cluster_members
where 
	recorded_on < @last_stats
and group_type='VirtualMachine'
group by recorded_on

union 

/*****/

/* CPU Sockets,Cores */
select date(concat(year, month, day)) as day_date, 'CPU' as property_type, property_type as property_subtype, sum(cpu_prop) as value
from
  ( select 
	date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    property_type, 
    property_subtype, 
    uuid, 
	max(avg_value) as cpu_prop
	from
		pm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) < @last_stats
       and property_type in ('numSockets', 'numCPUs')
    group by
      snapshot_time, uuid, property_type, property_subtype
  ) as pm_stats_by_day
natural join
  (select distinct 
		day(recorded_on) as day, 
		month(recorded_on) as month, 
		year(recorded_on) as year,
		member_uuid as uuid 
	from cluster_members
where 
	recorded_on < @last_stats
and group_type='PhysicalMachine'
  ) as pm_uuids
group by year, month, day, property_type


/*****/

union

select date(concat(year, month, day)) as day_date, 'Storage' as property_type, 'capacity' as property_subtype, sum(capacity) as value 
from
(select date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year, 
	date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month, 
	date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day, 
	ds_uuid, 
	capacity
 from 
   (select snapshot_time,
        uuid as ds_uuid, 
 		max(capacity) as capacity 
 	from
		ds_stats_by_day
    where property_subtype = 'utilization' 
		 and property_type = 'StorageAmount' 
		 and date(from_unixtime(snapshot_time/1000)) < @last_stats
    group by 
     snapshot_time, uuid
   ) as ds_stats_by_month
natural join
   (select distinct producer_uuid as ds_uuid
	from
		(select distinct uuid as vm_uuid, producer_uuid 
		from 
			vm_stats_by_day
			where 
		    date(from_unixtime(snapshot_time/1000)) < @last_stats
			and property_subtype = 'used'
			and property_type = 'StorageAmount'
		) as vm_stats_by_day
	join
		(select distinct
			member_uuid as vm_uuid
		from 
			cluster_members 
		where 
			recorded_on < @last_stats
		and group_type='VirtualMachine'
		) as vm_group_members
	using (vm_uuid)
   ) as included_ds_uuids
) as values_by_month
group by
  year, month, day

union 

select 
	recorded_on as day_date, 'Storage' as property_type, 'allocated' as property_subtype, sum(used_capacity) as value 
from
	(select
		recorded_on,
		uuid,
		avg_value as used_capacity
	from 
		(select 
			date(from_unixtime(snapshot_time/1000)) as recorded_on, uuid, avg_value
		from 
			vm_stats_by_day
		where
			date(from_unixtime(snapshot_time/1000)) < @last_stats
			and property_type = 'StorageAmount' 
			and property_subtype = 'used'
		) as vm_stats_by_day
	natural join
		(select distinct
			recorded_on,
			member_uuid as uuid
		from 
			cluster_members 
		where 
			recorded_on < @last_stats
		and group_type='VirtualMachine'
		) as members
	group by 
		recorded_on,
		uuid
	) as values_by_month
group by
  recorded_on

union 


select date(concat(date_format(date(from_unixtime(snapshot_time/1000)),'%Y'), date_format(date(from_unixtime(snapshot_time/1000)),'%m'), 
date_format(date(from_unixtime(snapshot_time/1000)),'%d'))) as day_date, 
'Storage' as property_type, 'available' as property_subtype, sum((1.0-max_value)*capacity) as value 
from
(select
	snapshot_time,
	uuid,
	capacity,
	max_value
from 
	(select 
		snapshot_time, uuid, capacity, max_value 
	from 
		ds_stats_by_day
	where 
		property_type = 'StorageAmount' 
		and property_subtype = 'utilization'
		and date(from_unixtime(snapshot_time/1000)) < @last_stats 
	) as ds_stats_by_day,
	(select distinct ds_uuid
	from
		(select distinct 
			uuid as vm_uuid, producer_uuid as ds_uuid
		from 
			vm_stats_by_day
			where 
		    date(from_unixtime(snapshot_time/1000)) < @last_stats
			and property_type = 'StorageAmount' 
			and property_subtype='used' 
		) as vm_to_ds
	natural join
		(select distinct
			member_uuid as vm_uuid
		from 
			cluster_members 
		where 
			recorded_on < @last_stats
		and group_type='VirtualMachine'
		) as members
	) as ds_uuids
where
	convert(uuid using utf8) = convert(ds_uuid using utf8)
group by 
	snapshot_time,
	uuid
) as values_by_month
group by
	snapshot_time

union

/* VMem */
select date(concat(year, month, day)) as day_date, 'VMem' as property_type, 'capacity' as proeprty_subtype, sum(vmem) / 1024 / 1024 as value
from
  ( select 
	date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    uuid, 
    max(capacity) as vmem
    from vm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) < @last_stats 
	   and property_type = 'VMem'
       and property_subtype = 'utilization'
    group by uuid, year, month, day
  ) as vm_stats_by_day
natural join
  (select distinct member_uuid as uuid from cluster_members
  	where 
		recorded_on < @last_stats
	and group_type='VirtualMachine'
  ) as yesterday_vms
group by year, month, day, property_type

) as daily_stats

group by 
  day_date, property_type, property_subtype;

end //
delimiter ;


/* PERFORM MIGRATION: */
call migrate_clusterAgg_Enterprise;


/* clean up helper procedures: */
drop procedure if exists migrate_clusterAgg_Enterprise;

/*
 * Register version info:
 */
insert into version_info values (5,1) ;

commit ;
