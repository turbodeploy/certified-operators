
use vmtdb ;

start transaction ;


create table pm_stats_by_hour (

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

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

create table pm_stats_by_day (

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


/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */


create table vm_stats_by_hour (

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

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

create table vm_stats_by_day (

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


/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */
/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */


create table ds_stats_by_hour (

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

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

create table ds_stats_by_day (

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


create index pm_bh_uuid_idx             on pm_stats_by_hour (uuid);
create index pm_bh_property_type_idx    on pm_stats_by_hour (property_type);
create index pm_bh_property_subtype_idx on pm_stats_by_hour (property_subtype);

create index pm_bd_uuid_idx             on pm_stats_by_day (uuid);
create index pm_bd_property_type_idx    on pm_stats_by_day (property_type);
create index pm_bd_property_subtype_idx on pm_stats_by_day (property_subtype);


create index vm_bh_uuid_idx             on vm_stats_by_hour (uuid);
create index vm_bh_property_type_idx    on vm_stats_by_hour (property_type);
create index vm_bh_property_subtype_idx on vm_stats_by_hour (property_subtype);

create index vm_bd_uuid_idx             on vm_stats_by_day (uuid);
create index vm_bd_property_type_idx    on vm_stats_by_day (property_type);
create index vm_bd_property_subtype_idx on vm_stats_by_day (property_subtype);


create index ds_bh_uuid_idx             on ds_stats_by_hour (uuid);
create index ds_bh_property_type_idx    on ds_stats_by_hour (property_type);
create index ds_bh_property_subtype_idx on ds_stats_by_hour (property_subtype);

create index ds_bd_uuid_idx             on ds_stats_by_day (uuid);
create index ds_bd_property_type_idx    on ds_stats_by_day (property_type);
create index ds_bd_property_subtype_idx on ds_stats_by_day (property_subtype);



ALTER TABLE entities          CHANGE name               name               varchar(250) ;
ALTER TABLE entities          ADD COLUMN                display_name       varchar(250) ;

ALTER TABLE entity_attrs      CHANGE value              value              varchar(250) ;

ALTER TABLE audit_log_entries CHANGE target_object_name target_object_name varchar(250) ;
ALTER TABLE audit_log_entries CHANGE source_name        source_name        varchar(250) ;
ALTER TABLE audit_log_entries CHANGE destination_name   destination_name   varchar(250) ;


delete from version_info ;
insert into version_info values (1,23) ;


commit ;
