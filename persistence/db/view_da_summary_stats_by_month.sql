use vmtdb ;

drop view if exists da_summary_stats_by_month ;
create or replace view da_summary_stats_by_month as
select
  last_day(max(snapshot_time)) as snapshot_time,
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
  da_stats_by_day
group by
  year(snapshot_time),
  month(snapshot_time),
  uuid,
  property_type,
  property_subtype,
  commodity_key
;
