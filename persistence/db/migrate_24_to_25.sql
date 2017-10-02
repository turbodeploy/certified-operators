
use vmtdb ;

start transaction ;

drop table if exists vc_licenses ;
create table vc_licenses (
    target   varchar(250),
    product  varchar(250),
    capacity int,
    used     int
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


delete from version_info ;
insert into version_info values (1,25) ;

commit ;
