

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
drop procedure if exists migrate_clusterAgg_Insert_AllPreviousDays//
create procedure migrate_clusterAgg_Insert_AllPreviousDays(IN cluster_internal_name varchar(250))
begin

set @last_day_recorded_memberships = IFNULL((select min(recorded_on) as min from (select distinct recorded_on from cluster_members where recorded_on!=end_of_month(recorded_on)) as a), (select date(date_sub(now(), interval 60 day)) as d));

insert into cluster_stats_by_day 
select
	day_date as recoreded_on,
	convert(cluster_internal_name using utf8) as internal_name,
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
	convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
	and recorded_on >= @last_day_recorded_memberships
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
    where date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
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
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
		and recorded_on >= @last_day_recorded_memberships
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
    where date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
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
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
		and recorded_on >= @last_day_recorded_memberships
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
	convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
	and recorded_on >= @last_day_recorded_memberships
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
    property_type, property_subtype, uuid, max(avg_value) as cpu_prop
    from
		pm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
       and property_type in ('numSockets', 'numCPUs')
    group by
      snapshot_time, uuid, property_type, property_subtype
  ) as pm_stats_by_day
natural join
  (select distinct 
		day(recorded_on) as day, 
		month(recorded_on) as month, 
		year(recorded_on) as year,
		member_uuid as uuid from cluster_members
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
		and recorded_on >= @last_day_recorded_memberships
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
		 and date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
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
		    date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
			and property_subtype = 'used'
			and property_type = 'StorageAmount'
		) as vm_stats_by_day
	join
		(select distinct
			member_uuid as vm_uuid
		from 
			cluster_members 
		where 
			recorded_on >= @last_day_recorded_memberships
			and convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
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
			date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
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
			convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
			and date(recorded_on) >= @last_day_recorded_memberships
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
		and date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships 
	) as ds_stats_by_day,
	(select distinct ds_uuid
	from
		(select distinct 
			uuid as vm_uuid, producer_uuid as ds_uuid
		from 
			vm_stats_by_day
			where 
		    date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships
			and property_type = 'StorageAmount' 
			and property_subtype='used' 
		) as vm_to_ds
	natural join
		(select distinct
			member_uuid as vm_uuid
		from 
			cluster_members 
		where 
			recorded_on >= @last_day_recorded_memberships
			and convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
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
    uuid, max(capacity) as vmem
    from vm_stats_by_day
    where
       date(from_unixtime(snapshot_time/1000)) >= @last_day_recorded_memberships 
	   and property_type = 'VMem'
       and property_subtype = 'utilization'
    group by uuid, year, month, day
  ) as vm_stats_by_day
natural join
  (select distinct member_uuid as uuid from cluster_members
    where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
	and recorded_on >= @last_day_recorded_memberships
  ) as yesterday_vms
group by year, month, day, property_type

) as daily_stats

group by 
  day_date, property_type, property_subtype;

end //
delimiter ;

/*
call clusterAggMigration_Insert_60_PreviousDays("GROUP-PMsByCluster_vsphere-dc5.corp.vmturbo.com\\datacenter-21\\domain-c28"); #debug
*/

delimiter $$
drop procedure if exists migrate_populateAll_DailyClusterAggTable_AllAvailableDays$$
create procedure migrate_populateAll_DailyClusterAggTable_AllAvailableDays()
begin
	DECLARE done INT DEFAULT 0;
	DECLARE cur_clsuter_internal_name varchar(250);
	
	/* Iterate over all of the available clusters in cluster-memebrs */
	DECLARE clusters_iterator CURSOR FOR SELECT DISTINCT internal_name FROM cluster_members WHERE group_type='PhysicalMachine' and recorded_on>=@last_day_recorded_memberships;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	set @last_day_recorded_memberships = IFNULL((select min(recorded_on) as min from (select distinct recorded_on from cluster_members where recorded_on!=end_of_month(recorded_on)) as a), (select date(date_sub(now(), interval 60 day)) as d));
	
	OPEN clusters_iterator;

	REPEAT
	FETCH clusters_iterator INTO cur_clsuter_internal_name;
	IF NOT done THEN
		call migrate_clusterAgg_Insert_AllPreviousDays(cur_clsuter_internal_name);
	END IF;
	UNTIL done END REPEAT;

	CLOSE clusters_iterator;
end $$
delimiter ;

/* MONTHLY */
delimiter //
drop procedure if exists migrate_clusterAgg_Insert_AllPreviousMonths//
create procedure migrate_clusterAgg_Insert_AllPreviousMonths(IN cluster_internal_name varchar(250))
begin

insert into cluster_stats_by_month 
select
	month_starting as recoreded_on,
	convert(cluster_internal_name using utf8) as internal_name,
	if(property_type='numCPUs', 'numCores',property_type) as property_type,
	property_subtype,
	if(property_subtype='utilization', value, sum(value)) as value

from

(

/* Host Count */
select date(concat(year, month, '01')) as month_starting, 'Host' as property_type, 'numHosts' as property_subtype, count(uuid) as value
from
  (select distinct
    member_uuid as uuid,
    date_format(recorded_on,'%Y') as year,
    date_format(recorded_on,'%m') as month
  from
    cluster_members_end_of_month
  where
    convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as members
group by
  year, month

union

/* VM Count */
select date(concat(year, month, '01')) as month_starting, 'VM' as property_type, 'numVMs' as property_subtype, count(uuid) as value
from
  (select distinct
    member_uuid as uuid,
    date_format(recorded_on,'%Y') as year,
    date_format(recorded_on,'%m') as month
  from
    cluster_members_end_of_month
  where
    convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
  ) as members
group by
  year, month

union

/* Memory capacity */
select date(concat(year, month, '01')) as month_starting, 'Mem' as property_type, 'capacity' as property_subtype, sum(mem)/1024/1024 as value
from
  ( select
      date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
      date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
      uuid,
      max(capacity) as mem
    from
      pm_stats_by_month
    where
      day(from_unixtime(snapshot_time/1000)) >= 28
      and property_type = 'Mem'
      and property_subtype = 'utilization'
    group by
      year, month, uuid
  ) as pm_stats_by_month,
  (select distinct
      member_uuid,
      date_format(recorded_on,'%Y') as member_year,
      date_format(recorded_on,'%m') as member_month
    from
      cluster_members_end_of_month
    where
      convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as members
  where
    convert(member_uuid using utf8) = convert(pm_stats_by_month.uuid using utf8)
    and member_year = year
    and member_month = month
group by
  year, month

union

/* Mem and CPU utilization average */
select date(concat(year, month, '01')) as month_starting, property_type, property_subtype, avg(mem) as value
from
  (select
      date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
      date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
      uuid, property_type, property_subtype,
      avg(avg_value) as mem
    from
      pm_stats_by_month
    where
      day(from_unixtime(snapshot_time/1000)) >= 28
      and property_type in ('Mem', 'CPU')
      and property_subtype = 'utilization'
    group by
      year, month, uuid, property_type
  ) as pm_stats_by_month,
  (select distinct
      member_uuid,
      date_format(recorded_on,'%Y') as member_year,
      date_format(recorded_on,'%m') as member_month
    from cluster_members_end_of_month
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as members
  where
    convert(member_uuid using utf8) = convert(pm_stats_by_month.uuid using utf8)
    and member_year = year
    and member_month = month
group by
  year, month, property_type

union


select date(concat(year, month, '01')) as month_starting, 'Storage' as property_type, 'capacity' as property_subtype, sum(capacity) as value
from
(select year, month, ds_stats_by_month.ds_uuid as ds_uuid, capacity
 from
   (select
      date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
      date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
      uuid as ds_uuid,
      max(capacity) as capacity
    from
       ds_stats_by_month
    where
       day(from_unixtime(snapshot_time/1000)) >= 28
       and property_type = 'StorageAmount'
       and property_subtype = 'utilization'
    group by
       year, month, uuid
   ) as ds_stats_by_month,
   (select distinct producer_uuid as ds_uuid, vm_to_ds.year as member_year, vm_to_ds.month as member_month
     from
     (select 
        uuid as vm_uuid,
        producer_uuid,
        date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
        date_format(date(from_unixtime(snapshot_time/1000)), '%m') as month
       from
         vm_stats_by_month
       where
         day(from_unixtime(snapshot_time/1000)) >= 28
         and property_type = 'StorageAmount'
         and property_subtype = 'used'
       group by
         year(date(from_unixtime(snapshot_time/1000))),
         month(date(from_unixtime(snapshot_time/1000))),
         uuid
     ) as vm_to_ds,
     (select distinct group_name,
        member_uuid as vm_uuid,
        date_format(recorded_on,'%Y') as member_year,
        date_format(recorded_on,'%m') as member_month
       from
         cluster_members_end_of_month
       where
         convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
     ) as vm_group_members
     where
       convert(vm_to_ds.vm_uuid using utf8) = convert(vm_group_members.vm_uuid using utf8)
       and vm_to_ds.month = member_month
       and vm_to_ds.year = member_year
   ) as included_ds_uuids
 where
    convert(ds_stats_by_month.ds_uuid using utf8) = convert(included_ds_uuids.ds_uuid using utf8)
    and ds_stats_by_month.year = member_year
    and ds_stats_by_month.month = member_month
) as values_by_month
group by
  year, month

union

select date(concat(year, month, '01')) as month_starting, 'Storage' as property_type, 'allocated' as property_subtype, sum(used_capacity) as value
from
(select
  date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
  date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
  uuid,
  avg_value as used_capacity
  from
    (select *
      from
        vm_stats_by_month
      where
        day(from_unixtime(snapshot_time/1000)) >= 28
        and property_type = 'StorageAmount'
        and property_subtype = 'used'
    ) as vm_stats_by_month,
    (select distinct
        member_uuid,
        date_format(recorded_on,'%Y') as member_year,
        date_format(recorded_on,'%m') as member_month
      from
        cluster_members_end_of_month
      where
        convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
    ) as members
  where
    convert(member_uuid using utf8) = convert(vm_stats_by_month.uuid using utf8)
    and year(from_unixtime(snapshot_time/1000)) = member_year
    and month(from_unixtime(snapshot_time/1000)) = member_month
  group by
    year(date(from_unixtime(snapshot_time/1000))),
    month(date(from_unixtime(snapshot_time/1000))),
    uuid
) as values_by_month
group by
  year, month

union

select date(concat(year, month, '01')) as month_starting, 'Storage' as property_type, 'available' as property_subtype, sum((1.0-max_value)*capacity) as value
from
(select
    date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    uuid,
    capacity,
    max_value
  from
  (select *
  from
    ds_stats_by_month
  where
    day(from_unixtime(snapshot_time/1000)) >= 28
    and property_type = 'StorageAmount'
    and property_subtype = 'utilization'
  ) as ds_stats_by_month,
  (select * from
    (select distinct uuid as vm_uuid, producer_uuid as ds_uuid,
        date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
        date_format(date(from_unixtime(snapshot_time/1000)), '%m') as month
      from vm_stats_by_month
      where 
      	day(from_unixtime(snapshot_time/1000)) >= 28
      	and
      	property_type != 'VStorage'
      group by
        year(date(from_unixtime(snapshot_time/1000))),
        month(date(from_unixtime(snapshot_time/1000))),
        uuid
    ) as vm_to_ds,
    (select distinct
        group_name,
        member_uuid,
        date_format(recorded_on,'%Y') as member_year,
        date_format(recorded_on,'%m') as member_month
      from
        cluster_members_end_of_month
      where
        convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
    ) as vm_group_members
    where
      convert(member_uuid using utf8) = convert(vm_uuid using utf8)
      and year = member_year
      and month = member_month
  ) as ds_uuids
  where
    convert(uuid using utf8) = convert(ds_uuid using utf8)
    and year(from_unixtime(snapshot_time/1000)) = year
    and month(from_unixtime(snapshot_time/1000)) = month
  group by
    year(date(from_unixtime(snapshot_time/1000))),
    month(date(from_unixtime(snapshot_time/1000))),
    uuid
) as values_by_month
group by
  year, month


/*****/
union 

/* CPU Sockets,Cores */
select date(concat(year, month, '01')) as day_date, 'CPU' as property_type, property_type as property_subtype, sum(cpu_prop) as value
from
  ( select 
	date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    property_type, property_subtype, uuid, max(avg_value) as cpu_prop
    from
		pm_stats_by_month
    where
       property_type in ('numSockets', 'numCPUs')
    group by
      uuid, snapshot_time, property_type, property_subtype
  ) as pm_stats_by_day
natural join
  (select distinct 
		day(recorded_on) as day, 
		month(recorded_on) as month, 
		year(recorded_on) as year,
		member_uuid as uuid from cluster_members_end_of_month
    where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
  ) as yesterday_pms
group by year, month, property_type, property_subtype

union

/* VMem */
select date(concat(year, month, '01')) as day_date, 'VMem' as property_type, 'capacity' as proeprty_subtype, sum(vmem) / 1024 / 1024 as value
from
  ( select 
	date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    uuid, max(capacity) as vmem
    from vm_stats_by_month
    where
       property_type = 'VMem'
       and property_subtype = 'utilization'
    group by uuid, year, month
  ) as vm_stats_by_day
natural join
  (select distinct 
		day(recorded_on) as day, 
		month(recorded_on) as month, 
		year(recorded_on) as year,
		member_uuid as uuid from cluster_members_end_of_month
    where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
  ) as yesterday_vms
group by year, month

/*****/

) as monthly_stats

group by
  month_starting, property_type, property_subtype;


end //
delimiter ;

/*
 * Insert the monthly data to each of the given clusters.
 */ 
delimiter $$
drop procedure if exists migrate_populateAll_MonthlyClusterAggTable_AllAvailableMonths$$
create procedure migrate_populateAll_MonthlyClusterAggTable_AllAvailableMonths()
begin
	DECLARE done INT DEFAULT 0;
	DECLARE cur_clsuter_internal_name varchar(250);
	
	#Iterate over all of the available clusters in cluster-memebrs
	DECLARE clusters_iterator CURSOR FOR SELECT DISTINCT internal_name FROM cluster_members WHERE group_type='PhysicalMachine';
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN clusters_iterator;

	REPEAT
	FETCH clusters_iterator INTO cur_clsuter_internal_name;
	IF NOT done THEN
		call migrate_clusterAgg_Insert_AllPreviousMonths(cur_clsuter_internal_name);
	END IF;
	UNTIL done END REPEAT;

	CLOSE clusters_iterator;
end $$
delimiter ;

/* PERFORM MIGRATION: */
call migrate_populateAll_DailyClusterAggTable_AllAvailableDays();
call migrate_populateAll_MonthlyClusterAggTable_AllAvailableMonths();

/* clean up helper procedures: */
drop procedure if exists migrate_populateAll_DailyClusterAggTable_AllAvailableDays;
drop procedure if exists migrate_populateAll_MonthlyClusterAggTable_AllAvailableMonths;
drop procedure if exists migrate_clusterAgg_Insert_AllPreviousMonths;
drop procedure if exists migrate_clusterAgg_Insert_AllPreviousDays;


/*
 * Register version info:
 */
insert into version_info values (2,1) ;

commit ;
