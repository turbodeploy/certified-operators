
use vmtdb ;

start transaction ;

drop table if exists vc_licenses;
create table vc_licenses (
    id                int not null auto_increment,
    target            varchar(250),
    product           varchar(250),
    capacity          int,
    used              int,
    primary key (id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


ALTER TABLE user_reports ADD COLUMN `period` varchar(80) ;
ALTER TABLE user_reports ADD COLUMN day_type varchar(80) ;

ALTER TABLE standard_reports ADD COLUMN `period` varchar(80) ;
ALTER TABLE standard_reports ADD COLUMN day_type varchar(80) ;


delete from version_info ;
insert into version_info values (1,26) ;

commit ;
