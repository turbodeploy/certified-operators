

use vmtdb ;

start transaction ;

drop table if exists vdc_stats_by_hour ;
create table vdc_stats_by_hour (

    /* Time info */
    snapshot_time     bigint,

    uuid              varchar(80),
    producer_uuid     varchar(80),


    /* Commodity Info */

    property_type     varchar(36),
      /* MemAllocation, CPUAllocation */

    property_subtype  varchar(36),
      /* utilization, used */

    capacity          decimal(15,3),
      /* Maximum Used amount - null when irrelevant (e.g. priceIndex, Produces) */

    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    std_dev           decimal(15,3)
      /* Utilization or Used values - based on property_subtype */

) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

drop table if exists vdc_stats_by_day ;
create table vdc_stats_by_day (

    /* Time info */
    snapshot_time     bigint,

    uuid              varchar(80),
    producer_uuid     varchar(80),


    /* Commodity Info */

    property_type     varchar(36),
      /* MemAllocation, CPUAllocation */

    property_subtype  varchar(36),
      /* utilization, used */

    capacity          decimal(15,3),
      /* Maximum Used amount - null when irrelevant (e.g. priceIndex, Produces) */

    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    std_dev           decimal(15,3)
      /* Utilization or Used values - based on property_subtype */

) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

drop table if exists vdc_stats_by_month ;
create table vdc_stats_by_month (

    /* Time info */
    snapshot_time     bigint,

    uuid              varchar(80),
    producer_uuid     varchar(80),


    /* Commodity Info */

    property_type     varchar(36),
      /* MemAllocation, CPUAllocation */

    property_subtype  varchar(36),
      /* utilization, used */

    capacity          decimal(15,3),
      /* Maximum Used amount - null when irrelevant (e.g. priceIndex, Produces) */

    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    std_dev           decimal(15,3)
      /* Utilization or Used values - based on property_subtype */

) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


create or replace view vdc_summary_stats_by_day as
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
  vdc_stats_by_hour
group by
  date(FROM_UNIXTIME(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;

create or replace view vdc_summary_stats_by_month as
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
  vdc_stats_by_day
group by
  year(FROM_UNIXTIME(snapshot_time/1000)),
  month(FROM_UNIXTIME(snapshot_time/1000)),
  uuid,
  property_type,
  property_subtype
;


delete from version_info ;
insert into version_info values (1,37) ;


commit ;
