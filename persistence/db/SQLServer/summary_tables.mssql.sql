
use vmtdb;


/*
    ---------------------------------------------------------------
        Tables summarizing commodity snapshot data per SE type
    ---------------------------------------------------------------
*/


IF OBJECT_ID(N'pm_stats_by_hour', N'U') IS NOT NULL drop table "pm_stats_by_hour";
IF OBJECT_ID(N'pm_stats_by_day', N'U') IS NOT NULL drop table "pm_stats_by_day";
IF OBJECT_ID(N'pm_stats_by_month', N'U') IS NOT NULL drop table "pm_stats_by_month";

IF OBJECT_ID(N'vm_stats_by_hour', N'U') IS NOT NULL drop table "vm_stats_by_hour";
IF OBJECT_ID(N'vm_stats_by_day', N'U') IS NOT NULL drop table "vm_stats_by_day";
IF OBJECT_ID(N'vm_stats_by_month', N'U') IS NOT NULL drop table "vm_stats_by_month";

IF OBJECT_ID(N'ds_stats_by_hour', N'U') IS NOT NULL drop table "ds_stats_by_hour";
IF OBJECT_ID(N'ds_stats_by_day', N'U') IS NOT NULL drop table "ds_stats_by_day";
IF OBJECT_ID(N'ds_stats_by_month', N'U') IS NOT NULL drop table "ds_stats_by_month";
go

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

)  ;
go

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

)  ;
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

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

)  ;
go

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

)  ;
go

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

)  ;
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

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

)  ;
go


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

)  ;
go

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

)  ;
go

/* -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- */

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

)  ;
go
