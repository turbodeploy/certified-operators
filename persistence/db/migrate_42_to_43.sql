

use vmtdb ;

start transaction ;

create or replace view vm_summary_stats_by_day as
select
  (unix_timestamp(date(from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stddev(avg_value) as std_dev
from
  vm_stats_by_hour
where
  ((property_type like 'Storage%') or (property_type = 'VStorage'))
group by
  date(FROM_UNIXTIME(snapshot_time/1000)),
  uuid,
  producer_uuid,
  property_type,
  property_subtype

union all

select
  (unix_timestamp(date(from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stddev(avg_value) as std_dev
from
  vm_stats_by_hour
where
  ((property_type not like 'Storage%') and (property_type <> 'VStorage'))
group by
  date(FROM_UNIXTIME(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;

create or replace view vm_summary_stats_by_month as
select
  (unix_timestamp(last_day(from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stddev(avg_value) as std_dev
from
  vm_stats_by_day
where
    ((property_type like 'Storage%') or (property_type = 'VStorage'))
group by
  year(FROM_UNIXTIME(snapshot_time/1000)),
  month(FROM_UNIXTIME(snapshot_time/1000)),
  uuid,
  producer_uuid,
  property_type,
  property_subtype

union all

select
  (unix_timestamp(last_day(from_unixtime(max(snapshot_time)/1000)))+12*3600)*1000 as snapshot_time,
  uuid,
  null as producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  stddev(avg_value) as std_dev
from
  vm_stats_by_day
where
  ((property_type not like 'Storage%') and (property_type <> 'VStorage'))
group by
  year(FROM_UNIXTIME(snapshot_time/1000)),
  month(FROM_UNIXTIME(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;





delete from version_info ;
insert into version_info values (1,43) ;

commit ;
