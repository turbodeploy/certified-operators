
SET sql_mode='ANSI_QUOTES';
SET character_set_client = utf8 ;

use vmtdb ;

drop table if exists "version_info";
create table "version_info" (
    "id"                int          not null auto_increment,
    "version"           int,
    primary key ("id")
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

delete from version_info ;
insert into version_info values (1,20) ;


ALTER TABLE audit_log_entries ADD COLUMN snapshot_time bigint ;

UPDATE audit_log_entries set snapshot_time = unix_timestamp(created_at)*1000 ;

ALTER TABLE audit_log_entries DROP COLUMN created_at ;
