-- use vmtdb ;

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */


drop table if exists ch_stats_by_hour ;
create table ch_stats_by_hour (
    snapshot_time     DATETIME,
    uuid              varchar(80),
    producer_uuid     varchar(80),
    property_type     varchar(36),
    property_subtype  varchar(36),
    capacity          decimal(15,3),
    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    relation          TINYINT(3),
    commodity_key	  varchar(80),
    index (snapshot_time, uuid, property_type, property_subtype)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

drop table if exists ch_stats_by_day ;
create table ch_stats_by_day (
    snapshot_time     DATE,
    uuid              varchar(80),
    producer_uuid     varchar(80),
    property_type     varchar(36),
    property_subtype  varchar(36),
    capacity          decimal(15,3),
    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    relation          TINYINT(3),
    commodity_key	  varchar(80),
    index (snapshot_time, uuid, property_type, property_subtype)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

drop table if exists ch_stats_by_month ;
create table ch_stats_by_month (
    snapshot_time     DATE,
    uuid              varchar(80),
    producer_uuid     varchar(80),
    property_type     varchar(36),
    property_subtype  varchar(36),
    capacity          decimal(15,3),
    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    relation          TINYINT(3),
    commodity_key	  varchar(80),
    index (snapshot_time, uuid, property_type, property_subtype)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;
