/*
---------------------------------------------------------------
   Tables summarizing commodity snapshot data per SE type
---------------------------------------------------------------
*/

-- use vmtdb ;

drop table if exists pm_stats_by_hour ;
create table pm_stats_by_hour (
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

drop table if exists pm_stats_by_day ;
create table pm_stats_by_day (
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

drop table if exists pm_stats_by_month ;
create table pm_stats_by_month (
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

drop table if exists vm_stats_by_hour ;
create table vm_stats_by_hour (
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

drop table if exists vm_stats_by_day ;
create table vm_stats_by_day (
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

drop table if exists vm_stats_by_month ;
create table vm_stats_by_month (
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
/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */


drop table if exists ds_stats_by_hour ;
create table ds_stats_by_hour (
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

drop table if exists ds_stats_by_day ;
create table ds_stats_by_day (
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

drop table if exists ds_stats_by_month ;
create table ds_stats_by_month (
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

drop table if exists vdc_stats_by_hour ;
create table vdc_stats_by_hour (
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

drop table if exists vdc_stats_by_day ;
create table vdc_stats_by_day (
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

drop table if exists vdc_stats_by_month ;
create table vdc_stats_by_month (
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

drop table if exists app_stats_by_hour ;
create table app_stats_by_hour (
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

drop table if exists app_stats_by_day ;
create table app_stats_by_day (
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

drop table if exists app_stats_by_month ;
create table app_stats_by_month (
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

source /srv/rails/webapps/persistence/db/Disk_Array.sql
source /srv/rails/webapps/persistence/db/Storage_Controller.sql;
source /srv/rails/webapps/persistence/db/Switch.sql;
source /srv/rails/webapps/persistence/db/IOModules.sql;
source /srv/rails/webapps/persistence/db/Chassis.sql;

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

/*
 * Cluster aggregation tables
 */
drop table if exists cluster_stats_by_day ;
create table cluster_stats_by_day (
    recorded_on date,
	internal_name varchar(250),
	property_type varchar(36),
    property_subtype varchar(36),
    value decimal(15,3),
    index(recorded_on, internal_name, property_type, property_subtype)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

drop table if exists cluster_stats_by_month ;
create table cluster_stats_by_month (
    recorded_on date,
	internal_name varchar(250),
	property_type varchar(36),
    property_subtype varchar(36),
    value decimal(15,3),
    index(recorded_on, internal_name, property_type, property_subtype)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;
