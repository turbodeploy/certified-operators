

use vmtdb ;

start transaction ;


drop table if exists pm_stats_by_month ;
create table pm_stats_by_month (

    /* Time info */
    snapshot_time     bigint,

    uuid              varchar(80),
    producer_uuid     varchar(80),


    /* Commodity Info */

    property_type     varchar(36),
      /* Mem, CPU, VMem, VCPU, priceIndex, Produces, StorageAmount/disk, etc */

    property_subtype  varchar(36),
      /* utilization, used, swap, log, snapshot */

    capacity          decimal(15,3),
      /* Maximum Used amount - null when irrelevant (e.g. priceIndex, Produces) */

    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    std_dev           decimal(15,3)
      /* Utilization or Used values - based on property_subtype */

) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;



drop table if exists vm_stats_by_month ;
create table vm_stats_by_month (

    /* Time info */
    snapshot_time     bigint,

    uuid              varchar(80),
    producer_uuid     varchar(80),


    /* Commodity Info */

    property_type     varchar(36),
      /* Mem, CPU, VMem, VCPU, priceIndex, Produces, StorageAmount/disk, etc */

    property_subtype  varchar(36),
      /* utilization, used, swap, log, snapshot */

    capacity          decimal(15,3),
      /* Maximum Used amount - null when irrelevant (e.g. priceIndex, Produces) */

    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    std_dev           decimal(15,3)
      /* Utilization or Used values - based on property_subtype */

) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;




drop table if exists ds_stats_by_month ;
create table ds_stats_by_month (

    /* Time info */
    snapshot_time     bigint,

    uuid              varchar(80),
    producer_uuid     varchar(80),


    /* Commodity Info */

    property_type     varchar(36),
      /* Mem, CPU, VMem, VCPU, priceIndex, Produces, StorageAmount/disk, etc */

    property_subtype  varchar(36),
      /* utilization, used, swap, log, snapshot */

    capacity          decimal(15,3),
      /* Maximum Used amount - null when irrelevant (e.g. priceIndex, Produces) */

    avg_value         decimal(15,3),
    min_value         decimal(15,3),
    max_value         decimal(15,3),
    std_dev           decimal(15,3)
      /* Utilization or Used values - based on property_subtype */

) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;



delete from version_info ;
insert into version_info values (1,31) ;


commit ;
