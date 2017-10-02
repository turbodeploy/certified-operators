

use vmtdb ;

start transaction ;


drop table if exists cluster_members ;
CREATE TABLE cluster_members (
  recorded_on date,
  group_uuid varchar(80),
  internal_name varchar(250),
  group_name varchar(250),
  group_type varchar(250),
  member_uuid varchar(80),
  display_name varchar(250)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


delete from version_info ;
insert into version_info values (1,30) ;


commit ;
