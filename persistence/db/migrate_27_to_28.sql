

use vmtdb ;

start transaction ;


drop table if exists notifications;
create table notifications (
    id                int          not null auto_increment,
    snapshot_time     bigint,
    severity          varchar(80),
    category          varchar(80),
    name              varchar(250),
    uuid              varchar(80),
    importance        double,
    description       mediumtext,
    primary key (id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;


delete from version_info ;
insert into version_info values (1,28) ;


commit ;
