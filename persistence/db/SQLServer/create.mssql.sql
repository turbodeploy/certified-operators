

use master ;
go

if not exists(select * from sys.databases where name = 'vmtdb')
    create database vmtdb ;
go

use vmtdb ;
go


IF OBJECT_ID(N'audit_log_entries', N'U') IS NOT NULL drop table "audit_log_entries";
IF OBJECT_ID(N'report_subscriptions', N'U') IS NOT NULL drop table "report_subscriptions";
IF OBJECT_ID(N'user_reports', N'U') IS NOT NULL drop table "user_reports";
IF OBJECT_ID(N'standard_reports', N'U') IS NOT NULL drop table "standard_reports";
IF OBJECT_ID(N'on_demand_reports', N'U') IS NOT NULL drop table "on_demand_reports";
IF OBJECT_ID(N'version_info', N'U') IS NOT NULL drop table "version_info";
IF OBJECT_ID(N'vc_licenses', N'U') IS NOT NULL drop table "vc_licenses";
IF OBJECT_ID(N'notifications', N'U') IS NOT NULL drop table "notifications";
IF OBJECT_ID(N'classifications_entities_entities', N'U') IS NOT NULL drop table "classifications_entities_entities";
IF OBJECT_ID(N'classifications', N'U') IS NOT NULL drop table "classifications";
IF OBJECT_ID(N'entity_assns_members_entities', N'U') IS NOT NULL drop table "entity_assns_members_entities";
IF OBJECT_ID(N'entity_assns', N'U') IS NOT NULL drop table "entity_assns";
IF OBJECT_ID(N'entity_attrs', N'U') IS NOT NULL drop table "entity_attrs";
IF OBJECT_ID(N'entities', N'U') IS NOT NULL drop table "entities";
IF OBJECT_ID(N'cluster_members', N'U') IS NOT NULL drop table cluster_members ;
go

-- -- -- -- -- --


create table "audit_log_entries" (
    "id"                int         IDENTITY(1,1) not null,
    "snapshot_time"     bigint,
    "action_name"       varchar(80),
    "category"          varchar(80),
    "user_name"         varchar(80),
    "target_object_class" varchar(80),
    "target_object_name" varchar(250),
    "target_object_uuid" varchar(80),
    "source_class"      varchar(80),
    "source_name"       varchar(250),
    "source_uuid"       varchar(80),
    "destination_class" varchar(80),
    "destination_name"  varchar(250),
    "destination_uuid"  varchar(80),
    "details"           text,
    primary key ("id")
)  ;
go


create table "user_reports" (
    "id"                int          IDENTITY(1,1) not null ,
    "title"             varchar(80),
    "category"          varchar(80),
    "short_desc"        varchar(80),
    "description"       text,
    "xml_descriptor"    text,
    "period"            varchar(80),
    "day_type"          varchar(80),
    primary key ("id")
)  ;
go


create table "standard_reports" (
    "id" int NOT NULL IDENTITY(1,1),
    "title" varchar(80) DEFAULT NULL,
    "description" text,
    "short_desc" varchar(80) DEFAULT NULL,
    "category" varchar(80) DEFAULT NULL,
    "filename" varchar(80) DEFAULT NULL,
    "period" varchar(80) DEFAULT NULL,
    "day_type" varchar(80) DEFAULT NULL,
    PRIMARY KEY (id)
)  ;
go


create table "on_demand_reports" (
    "id" int NOT NULL IDENTITY(1,1),
    "obj_type" varchar(80) DEFAULT NULL,
    "scope_type" varchar(80) DEFAULT NULL,
    "short_desc" varchar(80) DEFAULT NULL,
    "filename" varchar(80) DEFAULT NULL,
    "description" text ,
    "category" varchar(80) DEFAULT NULL,
    "title" varchar(80) DEFAULT NULL,
    PRIMARY KEY (id)
)  ;
go


create table "report_subscriptions" (
    "id"                int          IDENTITY(1,1) not null ,
    "email"             varchar(80),
    "period"            varchar(80),
    "day_type"          varchar(80),
    "obj_uuid"          varchar(80),
    "standard_report_standard_report_id" int,
    "custom_report_user_report_id" int,
    "on_demand_report_on_demand_report_id" int,
    primary key ("id")
)  ;
go


create table "version_info" (
    "id"                int          IDENTITY(1,1) not null ,
    "version"           int,
    primary key ("id")
)  ;
go


create table "vc_licenses" (
    "id"                int          IDENTITY(1,1) not null ,
    "target"            varchar(250),
    "product"           varchar(250),
    "capacity"          int,
    "used"              int,
    primary key ("id")
)  ;
go


create table "notifications" (
    "id"                int          IDENTITY(1,1) not null ,
    "clear_time"        bigint,
    "last_notify_time"  bigint,
    "severity"          varchar(80),
    "category"          varchar(80),
    "name"              varchar(250),
    "uuid"              varchar(80),
    "importance"        float(24),
    "description"       text,
    primary key ("id")
)  ;
go


create table "entity_assns" (
    "id"                int          IDENTITY(1,1) not null ,
    "name"              varchar(80),
    "entity_entity_id"  int,
    primary key ("id")
)  ;
go


create table "classifications" (
    "id"                int          IDENTITY(1,1) not null ,
    "name"              varchar(80),
    primary key ("id")
)  ;
go


create table "entities" (
    "id"                int          IDENTITY(1,1) not null ,
    "name"              varchar(250),
    "display_name"      varchar(250),
    "uuid"              varchar(80),
    "creation_class"    varchar(80),
    "created_at"        datetime,
    "updated_at"        datetime,
    primary key ("id")
)  ;
go


create table "entity_attrs" (
    "id"                int          IDENTITY(1,1) not null ,
    "name"              varchar(80),
    "data_type"         varchar(80),
    "value"             varchar(250),
    "multi_values"      text,
    "entity_entity_id"  int,
    primary key ("id")
)  ;
go


alter table "report_subscriptions" add constraint "fk_report_subscriptions_standard_report_standard_report_id" foreign key ("standard_report_standard_report_id") references "standard_reports" ("id") on delete set null;
alter table "report_subscriptions" add constraint "fk_report_subscriptions_custom_report_user_report_id" foreign key ("custom_report_user_report_id") references "user_reports" ("id") on delete set null;
alter table "report_subscriptions" add constraint "fk_report_subscriptions_on_demand_report_on_demand_report_id" foreign key ("on_demand_report_on_demand_report_id") references "on_demand_reports" ("id") on delete set null;
go

-- alter table "entity_assns" add constraint "fk_entity_assns_entity_entity_id" foreign key ("entity_entity_id") references "entities" ("id") on delete cascade;
-- alter table "entity_attrs" add constraint "fk_entity_attrs_entity_entity_id" foreign key ("entity_entity_id") references "entities" ("id") on delete cascade;

create table "entity_assns_members_entities" (
    "entity_assn_src_id"      int,
    "entity_dest_id"    int,
    constraint "fk_entity_assns_members_entities_sid" foreign key ("entity_assn_src_id") references "entity_assns" ("id") on delete cascade,
    constraint "fk_entity_assns_members_entities_did" foreign key ("entity_dest_id") references "entities" ("id") on delete cascade,
    primary key ("entity_assn_src_id", "entity_dest_id")
)  ;
go

create table "classifications_entities_entities" (
    "classification_src_id"      int,
    "entity_dest_id"    int,
    constraint "fk_classifications_entities_entities_sid" foreign key ("classification_src_id") references "classifications" ("id") on delete cascade,
    constraint "fk_classifications_entities_entities_did" foreign key ("entity_dest_id") references "entities" ("id") on delete cascade,
    primary key ("classification_src_id", "entity_dest_id")
)  ;
go


CREATE TABLE cluster_members (
  recorded_on date,
  group_uuid varchar(80),
  internal_name varchar(250),
  group_name varchar(250),
  group_type varchar(250),
  member_uuid varchar(80),
  display_name varchar(250)
)  ;
go

create table customer_info (
    customer_name             varchar(80),
    image_name           varchar(250),
    copyright          varchar(250),
    primary key ("customer_name")
)  ;
go

/* add default data to customer_info table */
delete from customer_info;
insert into customer_info values ('VMTurbo', '/srv/reports/images/logo.jpg', '/srv/reports/images/copyright.jpg') ;
go

/*
    Date/Time utility functions for dates, months, milliseconds
*/


IF OBJECT_ID(N'dbo.now', N'FN') IS NOT NULL DROP FUNCTION dbo.now;
go

CREATE FUNCTION now()
RETURNS DATETIME
AS
BEGIN
  RETURN GETDATE()
END
go


--

IF OBJECT_ID(N'dbo.from_unixtime', N'FN') IS NOT NULL DROP FUNCTION dbo.from_unixtime;
go

CREATE FUNCTION from_unixtime
(
    @unixtime BIGINT
)
RETURNS DATETIME
AS
BEGIN
  DECLARE @utc_time   DATETIME ;
  DECLARE @local_time DATETIME ;

  SET @utc_time   = dateadd(s, @unixtime, '1970-01-01') ;
  SET @local_time = dateadd(hh, datediff(hh,getutcdate(),getdate()), @utc_time) ;
  RETURN @local_time ;
END
go


--

IF OBJECT_ID(N'dbo.unix_timestamp', N'FN') IS NOT NULL DROP FUNCTION dbo.unix_timestamp;
go

CREATE FUNCTION unix_timestamp
(
    @dt DATETIME
)
RETURNS BIGINT
AS
BEGIN
    DECLARE @diff BIGINT
    IF @dt >= '20380119'
      BEGIN
        SET @diff = CONVERT(BIGINT, DATEDIFF(S, '19700101', '20380119'))
            + CONVERT(BIGINT, DATEDIFF(S, '20380119', @dt))
      END
    ELSE
        SET @diff = DATEDIFF(S, '19700101', @dt)
    RETURN @diff
END
go


--

IF OBJECT_ID(N'dbo.date_sub', N'FN') IS NOT NULL DROP FUNCTION dbo.date_sub;
go

CREATE FUNCTION date_sub
(
  @from_date    DATETIME,
  @n_days       int
)
RETURNS DATETIME
AS
BEGIN
  RETURN dateadd(d,0-@n_days,@from_date)
END
go


--

IF OBJECT_ID(N'dbo.last_day', N'FN') IS NOT NULL DROP FUNCTION dbo.last_day;
go

CREATE FUNCTION last_day(@somedate DATETIME)
RETURNS DATETIME
AS
BEGIN
  RETURN convert(date,DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,@somedate)+1,0)))
END
go


--

IF OBJECT_ID(N'dbo.ms_from_date', N'FN') IS NOT NULL DROP FUNCTION dbo.ms_from_date;
go

CREATE FUNCTION dbo.ms_from_date(@the_date datetime) RETURNS bigint
BEGIN
    return dbo.unix_timestamp(@the_date)*1000 ;
END
go

--

IF OBJECT_ID(N'dbo.ms_from_datetime', N'FN') IS NOT NULL DROP FUNCTION dbo.ms_from_datetime;
go

CREATE FUNCTION dbo.ms_from_datetime(@the_datetime datetime) RETURNS bigint
BEGIN
    return dbo.unix_timestamp(@the_datetime)*1000 ;
END
go


--

IF OBJECT_ID(N'dbo.date_from_ms', N'FN') IS NOT NULL DROP FUNCTION dbo.date_from_ms;
go

CREATE FUNCTION dbo.date_from_ms(@ms_time bigint) RETURNS date
BEGIN
    return convert(date, dbo.from_unixtime(@ms_time/1000)) ;
END
go


--

IF OBJECT_ID(N'dbo.datetime_from_ms', N'FN') IS NOT NULL DROP FUNCTION dbo.datetime_from_ms;
go

CREATE FUNCTION dbo.datetime_from_ms(@ms_time bigint) RETURNS datetime
BEGIN
    return dbo.from_unixtime(@ms_time/1000) ;
END
go


--

IF OBJECT_ID(N'dbo.days_ago', N'FN') IS NOT NULL DROP FUNCTION dbo.days_ago;
go

CREATE FUNCTION dbo.days_ago(@ndays int) RETURNS date
BEGIN
    return dbo.date_sub(convert(date, dbo.now()), @ndays) ;
END
go

--

IF OBJECT_ID(N'dbo.days_from_now', N'FN') IS NOT NULL DROP FUNCTION dbo.days_from_now;
go

CREATE FUNCTION dbo.days_from_now(@ndays int) RETURNS date
BEGIN
    return dateadd(d, @ndays, convert(date, dbo.now())) ;
END
go


--

IF OBJECT_ID(N'dbo.start_of_month', N'FN') IS NOT NULL DROP FUNCTION dbo.start_of_month;
go

CREATE FUNCTION dbo.start_of_month(@ref_date datetime) RETURNS date
BEGIN
    return dbo.date_sub(@ref_date, day(@ref_date)-1) ;
END
go


--

IF OBJECT_ID(N'dbo.end_of_month', N'FN') IS NOT NULL DROP FUNCTION dbo.end_of_month;
go

CREATE FUNCTION dbo.end_of_month(@ref_date datetime) RETURNS date
BEGIN
    return dbo.date_sub(dateadd(m, 1, dbo.start_of_month(@ref_date)), 1) ;
END
go


--

IF OBJECT_ID(N'dbo.start_of_month_ms', N'FN') IS NOT NULL DROP FUNCTION dbo.start_of_month_ms;
go

CREATE FUNCTION dbo.start_of_month_ms(@ref_date datetime) RETURNS bigint
BEGIN
    return dbo.ms_from_date(dbo.start_of_month(@ref_date)) ;
END
go


--

IF OBJECT_ID(N'dbo.end_of_month_ms', N'FN') IS NOT NULL DROP FUNCTION dbo.end_of_month_ms;
go

CREATE FUNCTION dbo.end_of_month_ms(@ref_date datetime) RETURNS bigint
BEGIN
    return dbo.ms_from_date(dateadd(m, 1, dbo.start_of_month(@ref_date)))-1 ;
END
go


--

IF OBJECT_ID(N'dbo.date_str', N'FN') IS NOT NULL DROP FUNCTION dbo.date_str;
go

CREATE FUNCTION dbo.date_str(@ref_date date) returns varchar(10)
BEGIN
    return REPLACE(CONVERT(VARCHAR(10), @ref_date, 111),'/','-') ;
END
go


--

IF OBJECT_ID(N'dbo.hour_str', N'FN') IS NOT NULL DROP FUNCTION dbo.hour_str;
go

CREATE FUNCTION dbo.hour_str(@ref_date datetime) returns varchar(2)
BEGIN
    declare @h int = datepart(HOUR,@ref_date) ;
    return
        case
          when @h >= 10 then convert(varchar(2), @h)
          else '0' + convert(varchar(1), @h)
        end
END
go


--

IF OBJECT_ID(N'dbo.month_str', N'FN') IS NOT NULL DROP FUNCTION dbo.month_str;
go

CREATE FUNCTION dbo.month_str(@ref_date datetime) returns varchar(2)
BEGIN
    declare @m int = datepart(MONTH,@ref_date) ;
    return
        case
          when @m >= 10 then convert(varchar(2), @m)
          else '0' + convert(varchar(1), @m)
        end
END
go


--

IF OBJECT_ID(N'dbo.day_str', N'FN') IS NOT NULL DROP FUNCTION dbo.day_str;
go

CREATE FUNCTION dbo.day_str(@ref_date datetime) returns varchar(2)
BEGIN
    declare @d int = datepart(DAY,@ref_date) ;
    return
        case
          when @d >= 10 then convert(varchar(2), @d)
          else '0' + convert(varchar(1), @d)
        end
END
go


--

IF OBJECT_ID(N'dbo.dayname', N'FN') IS NOT NULL DROP FUNCTION dbo.dayname;
go

CREATE FUNCTION dbo.dayname(@ref_date datetime) returns varchar(3)
BEGIN
    declare @d int = datepart(dw,@ref_date) ;
    return substring('SunMonTueWedThuFriSat',(@d-1)*3+1,3)
END
go


--

IF OBJECT_ID(N'dbo.dayofweek', N'FN') IS NOT NULL DROP FUNCTION dbo.dayofweek;
go

CREATE FUNCTION dbo.dayofweek(@ref_date datetime) returns varchar(1)
BEGIN
    declare @d int = datepart(dw,@ref_date) ;
    return convert(varchar(1), @d)
END
go



/* set database version */


SET IDENTITY_INSERT version_info ON

delete from version_info ;
insert into version_info (id,version) values (1,31) ;

SET IDENTITY_INSERT version_info OFF
go

