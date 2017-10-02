

use vmtdb ;

start transaction ;

/*
 * Summary tables:
 */
drop table if exists cluster_stats_by_day ;
create table cluster_stats_by_day (
    recorded_on date,
	internal_name varchar(250),
	property_type varchar(36),
    property_subtype varchar(36),
    value decimal(15,3)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

drop table if exists cluster_stats_by_month ;
create table cluster_stats_by_month (
    recorded_on date,
	internal_name varchar(250),
	property_type varchar(36),
    property_subtype varchar(36),
    value decimal(15,3)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

/*
 * Views:
 */
create or replace view cluster_members_yesterday as
select * from cluster_members where recorded_on=date(date_sub(now(), interval 1 day))
;

create or replace view cluster_summary_stats_by_month as
select
  start_of_month(recorded_on) as recorded_on,
  internal_name,
  property_type,
  property_subtype,
  if(property_subtype='utilization', avg(value), round(avg(value))) as value
from
  cluster_stats_by_day
where 
	recorded_on between start_of_month(date_sub(now(), interval 1 month)) and end_of_month(date_sub(now(), interval 1 month))
group by
  internal_name,
  year(recorded_on),
  month(recorded_on),
  property_type,
  property_subtype
order by recorded_on desc, property_type
;


/*
 * Previous day aggregation
 */
delimiter //
drop procedure if exists clusterAggPreviousDay//
create procedure clusterAggPreviousDay(IN cluster_internal_name varchar(250))
begin

insert into cluster_stats_by_day 
select
	date_sub(date(concat(year(now()),date_format(now(),'%m'),date_format(now(),'%d'))), interval 1 day) as recoreded_on,
	convert(cluster_internal_name using utf8) as internal_name,
	if(property_type='numCPUs', 'numCores',property_type) as property_type,
	property_subtype,
	if(property_subtype='utilization', val, sum(val)) as value
from
(

select 'Host' as property_type, 'numHosts' as property_subtype, count(uuid) as val
from
  (select distinct member_uuid as uuid from cluster_members_yesterday
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as yesterday_pms

union

select 'VM' as property_type, 'numVMs' as property_subtype, count(uuid) as val
from
  (select distinct member_uuid as uuid from cluster_members_yesterday
    where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
  ) as yesterday_vms

union

select 'Mem' as property_type, 'capacity' as property_subtype, sum(mem) / 1024 / 1024 as val
from
  ( select
		uuid,
		max(capacity) as mem
    from
		pm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
       and property_type = 'Mem'
       and property_subtype = 'utilization'
    group by
      uuid
  ) as pm_stats_by_day
natural join
  (select distinct member_uuid as uuid from cluster_members_yesterday
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as yesterday_pms

union

/* Mem and CPU utilization average */
select property_type, property_subtype, avg(mem) as val 
from
  ( select property_type, property_subtype, uuid, avg(avg_value) as mem
    from 
		pm_stats_by_day
    where date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
       and property_type in ('Mem', 'CPU')
       and property_subtype = 'utilization'
    group by uuid, property_type
  ) as pm_stats_by_day
natural join
  (select distinct member_uuid as uuid
    from cluster_members_yesterday where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as yesterday_pms
group by property_type

union

select 'CPU' as property_type, property_type as property_subtype, sum(cpu_prop) as val
from
  ( select property_type, property_subtype, uuid, max(avg_value) as cpu_prop
    from
		pm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
       and property_type in ('numSockets', 'numCPUs')
    group by
      uuid, property_type
  ) as pm_stats_by_day
natural join
  (select distinct member_uuid as uuid from cluster_members_yesterday
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as yesterday_pms
group by property_type

union

select 'Storage' as property_type, 'capacity' as property_subtype, sum(capacity) as val
from
(select ds_stats_by_month.ds_uuid as ds_uuid, capacity
 from
   (select uuid as ds_uuid,
        max(capacity) as capacity
    from
     ds_stats_by_day
    where
     date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
	 and property_type = 'StorageAmount'
     and property_subtype = 'utilization'
    group by
     uuid
   ) as ds_stats_by_month
natural join
   (select distinct producer_uuid as ds_uuid from
     (select uuid as vm_uuid, producer_uuid
     from vm_stats_by_day
     where
	   property_subtype = 'used' and property_type = 'StorageAmount'
       and date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
     group by
       uuid
     ) as vm_to_ds
	natural join
     (select distinct member_uuid as vm_uuid from cluster_members_yesterday
      where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
     ) as yesterday_vms
   ) as included_ds_uuids
) as values_by_month

union

select 'Storage' as property_type, 'allocated' as property_subtype, sum(used_capacity) as val
from
  (select uuid, avg_value as used_capacity
    from
      vm_stats_by_day
    where
      property_type = 'StorageAmount'
      and property_subtype = 'used'
	  and date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
  ) as vm_stats_by_month
natural join
  (select distinct member_uuid as uuid from cluster_members_yesterday
    where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
  ) as members

union

select 'Storage' as property_type, 'available' as property_subtype, sum((1.0-max_value)*capacity) as val
from
(select
  ds_uuid as uuid, capacity, max_value
  from
	  (select uuid as ds_uuid, capacity, max_value
	  from ds_stats_by_day
	  where
		date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
		and property_type = 'StorageAmount'
		and property_subtype = 'utilization'
	  ) as ds_stats_by_day
	natural join
		(select distinct producer_uuid as ds_uuid from
			(select uuid as vm_uuid, producer_uuid
			from vm_stats_by_day
			where
				property_subtype = 'used' and property_type = 'StorageAmount'
				and date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
			group by uuid
			) as vm_to_ds
		natural join
			(select distinct member_uuid as vm_uuid from cluster_members_yesterday
			where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
			) as yesterday_vms
		) as included_ds_uuids
	group by uuid
) as values_by_month

union

select 'VMem' as property_type, 'capacity' as proeprty_subtype, sum(vmem) / 1024 / 1024 as val
from
  ( select uuid, max(capacity) as vmem
    from vm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) = date(date_sub(now(), interval 1 day))
	   and property_type = 'VMem'
       and property_subtype = 'utilization'
    group by uuid
  ) as vm_stats_by_day
natural join
  (select distinct member_uuid as uuid from cluster_members_yesterday
    where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
  ) as yesterday_vms

) as side_by_side_data

group by
  property_type, property_subtype
;

end //
delimiter ;

delimiter $$
drop procedure if exists populate_AllClusters_PreviousDayAggStats$$
create procedure populate_AllClusters_PreviousDayAggStats()
begin
	DECLARE done INT DEFAULT 0;
	DECLARE cur_clsuter_internal_name varchar(250);
	
	/* Iterate over all of the available clusters in cluster-memebrs */
	DECLARE clusters_iterator CURSOR FOR SELECT DISTINCT internal_name FROM cluster_members_yesterday WHERE group_type='PhysicalMachine';
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN clusters_iterator;

	REPEAT
	FETCH clusters_iterator INTO cur_clsuter_internal_name;
	IF NOT done THEN
		call clusterAggPreviousDay(cur_clsuter_internal_name);
	END IF;
	UNTIL done END REPEAT;

	CLOSE clusters_iterator;
end $$
delimiter ;

delete from version_info where id=1;
insert into version_info values (1,45) ;

commit ;
