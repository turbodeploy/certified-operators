use vmtdb ;

drop view if exists ch_summary_stats_by_day ;
create or replace view ch_summary_stats_by_day as
select
  date(max(snapshot_time)) as snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg(avg_value) as avg_value,
  min(min_value) as min_value,
  max(max_value) as max_value,
  relation,
  commodity_key
from
  ch_stats_by_hour
group by
  date(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;

