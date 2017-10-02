Select 'Cluster' , 'MemAvg', 'MemAvgUtil' , 'MemPeak', 'MemPeakUtil', 'CPUAvg', 'CPUAvgUtil', 'CPUPeak', 'CPUPeakUtil',  'numHosts', 'VMsMax'
	union
Select 
	UtilTab.internal_name, MemAvg, MemAvgUtil, MemPeak, MemPeakUtil, CPUAvg, CPUAvgUtil, CPUPeak, CPUPeakUtil, numHost, VMsMax
from
	(
		select internal_name,
			max(case when property_type='Mem' then round(avgVal) else 0 end) 'MemAvg',
			max(case when property_type='Mem' then avg_value else 0 end) 'MemAvgUtil',
			max(case when property_type='Mem' then round(peakVal) else 0 end) 'MemPeak',
			max(case when property_type='Mem' then max_value else 0 end) 'MemPeakUtil',
			max(case when property_type='CPU' then round(avgVal) else 0 end) 'CPUAvg',
			max(case when property_type='CPU' then avg_value else 0 end) 'CPUAvgUtil',
			max(case when property_type='CPU' then round(peakVal) else 0 end) 'CPUPeak',
			max(case when property_type='CPU' then max_value else 0 end) 'CPUPeakUtil' 
		from
		(
			select 
				grp.internal_name as internal_name,
				property_type,
				avg(avg_value*capacity) as avgVal,
				avg(avg_value) as avg_value,
				avg(max_value*capacity) as peakVal,
				avg(max_value) as max_value
			from 
				pm_stats_by_day pm , pm_group_members grp
			where 
				pm.uuid=grp.member_uuid
				and pm.property_type IN ('CPU' , 'Mem')
				and pm.property_subtype='utilization'
				and capacity > 0.00
				and date(snapshot_time) >= date_sub(now(), interval 1 month)
			group by 
				grp.internal_name,
				pm.property_type
		) as pm_grp_stats
		group by 
			internal_name  
	) as UtilTab ,
	(
		select  
			internal_name ,
			max(nHost) as numHost,
			max(nVM) as VMsMax
		from 
			(
				select 
					internal_name,
					if(property_subtype='numHosts'  , avg(value), 0 ) as 'nHost',
					if(property_subtype='numVMs'  , max(value), 0 ) as 'nVM'
				from 
					cluster_stats_by_day
				where 
					recorded_on >= date_sub(now() , interval 1 month)
					and property_subtype IN ('numHosts' , 'numVMs')
				group by 
					internal_name , property_subtype
			) as cluster_stats
		group by 
			internal_name
	) as counts
where 
	CONVERT(UtilTab.internal_name using utf8) COLLATE utf8_unicode_ci= CONVERT(counts.internal_name using utf8) COLLATE utf8_unicode_ci
;