

use vmtdb ;

start transaction ;

/*
 * Delete the Duplicate rows with value in KB and GB
 */
DELETE t1 FROM cluster_stats_by_day t1, cluster_stats_by_day t2 WHERE t1.value > t2.value AND t1.recorded_on = t2.recorded_on AND t1.internal_name=t2.internal_name AND t1.property_type=t2.property_type AND t1.property_subtype=t2.property_subtype;
DELETE t1 FROM cluster_stats_by_month t1, cluster_stats_by_month t2 WHERE t1.value > t2.value AND t1.recorded_on = t2.recorded_on AND t1.internal_name=t2.internal_name AND t1.property_type=t2.property_type AND t1.property_subtype=t2.property_subtype;

/*
 * Delete the Duplicate rows with all columns same and keep one instance of duplicate rows
 */
CREATE TABLE cluster_stats_by_month_temp SELECT DISTINCT * FROM cluster_stats_by_month;
DELETE from cluster_stats_by_month;
INSERT INTO cluster_stats_by_month SELECT * from cluster_stats_by_month_temp;
DROP TABLE cluster_stats_by_month_temp;

CREATE TABLE cluster_stats_by_day_temp SELECT DISTINCT * FROM cluster_stats_by_day;
DELETE from cluster_stats_by_day;
INSERT INTO cluster_stats_by_day SELECT * from cluster_stats_by_day_temp;
DROP TABLE cluster_stats_by_day_temp;


/*
 * Delete rows with NULL values
 */
DELETE from cluster_stats_by_day where value IS NULL;
DELETE from cluster_stats_by_month where value IS NULL;


/*
 * Fix incorrect units in VMem capacity entries
 */
update cluster_stats_by_month set value=((value/1024)/1024) where property_type='VMem' and property_subtype='capacity' and value>=100000;
update cluster_stats_by_day set value=((value/1024)/1024) where property_type='VMem' and property_subtype='capacity' and value>=100000; 
 





/* Insert values for missing/corrupted values of VMem and Mem since May 2012 */

delimiter //
drop procedure if exists migrate_clusterAgg_Insert_SinceMay//
create procedure migrate_clusterAgg_Insert_SinceMay(IN cluster_internal_name varchar(250))
begin

insert into cluster_stats_by_month 
select
	month_starting as recorded_on,
	convert(cluster_internal_name using utf8) as internal_name,
	property_type,
	property_subtype,
	value

from

(
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
	 and date(from_unixtime(snapshot_time/1000)) >= '2012-05-01'
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
	 and recorded_on >= '2012-05-01'
  ) as members
  where
    convert(member_uuid using utf8) = convert(pm_stats_by_month.uuid using utf8)
    and member_year = year
    and member_month = month
group by
  year, month

union

/* VMem */
select date(concat(year, month, '01')) as day_date, 'VMem' as property_type, 'capacity' as property_subtype, sum(vmem) / 1024 / 1024 as value
from
  ( select 
	date_format(date(from_unixtime(snapshot_time/1000)),'%Y') as year,
    date_format(date(from_unixtime(snapshot_time/1000)),'%m') as month,
    date_format(date(from_unixtime(snapshot_time/1000)),'%d') as day,
    uuid, 
	max(capacity) as vmem
    from vm_stats_by_month
    where
       property_type = 'VMem'
       and property_subtype = 'utilization'
	  and date(from_unixtime(snapshot_time/1000)) >= '2012-05-01'
    
group by uuid, year, month
  ) as vm_stats_by_month,
  (select distinct 
		member_uuid,
      date_format(recorded_on,'%Y') as member_year,
      date_format(recorded_on,'%m') as member_month
	from cluster_members_end_of_month
    where convert(internal_name using utf8) = replace(convert(cluster_internal_name using utf8),'GROUP-PMs','GROUP-VMs')
		 and recorded_on >= '2012-05-01'
  ) as members
where
    convert(member_uuid using utf8) = convert(vm_stats_by_month.uuid using utf8)
    and member_year = year
    and member_month = month

group by year, month

) as monthly_stats;


end //

delimiter ;



/*
 * Insert the monthly data to each of the given clusters.
 */ 
delimiter $$
drop procedure if exists migrate_populateAll_MonthlyClusterAggTable_SinceMay$$
create procedure migrate_populateAll_MonthlyClusterAggTable_SinceMay()
begin
	DECLARE done INT DEFAULT 0;
	DECLARE cur_cluster_internal_name varchar(250);
	
	#Iterate over all of the available clusters in cluster-memebrs
	DECLARE clusters_iterator CURSOR FOR SELECT DISTINCT internal_name FROM cluster_members WHERE group_type='PhysicalMachine';
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN clusters_iterator;

	REPEAT
	FETCH clusters_iterator INTO cur_cluster_internal_name;
	IF NOT done THEN
		call migrate_clusterAgg_Insert_SinceMay(cur_cluster_internal_name);
	END IF;
	UNTIL done END REPEAT;

	CLOSE clusters_iterator;
end $$
delimiter ;


/*DELETE EXISTING ROWS TO AVOID DUPLICATES*/
DELETE FROM cluster_stats_by_month 
WHERE recorded_on >= '2012-05-01' 
and property_type IN ('Mem','VMem') 
and property_subtype='capacity';

/* PERFORM MIGRATION: */
call migrate_populateAll_MonthlyClusterAggTable_SinceMay();

drop procedure if exists migrate_clusterAgg_Insert_SinceMay;
drop procedure if exists migrate_populateAll_MonthlyClusterAggTable_SinceMay;



/*
 * Register version info:
 */
insert into version_info values (4,1) ;

commit ;
