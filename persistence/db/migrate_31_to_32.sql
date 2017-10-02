

use vmtdb ;

start transaction ;


drop table if exists customer_info;
create table customer_info (
    customer_name        varchar(80) not null,
    image_name           varchar(250),
    copyright            varchar(250),
    primary key (customer_name)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;

/* add default data to customer_info table */
delete from customer_info;
insert into customer_info values ('VMTurbo', '/srv/reports/images/logo.jpg', '/srv/reports/images/copyright.jpg') ;

delete from version_info ;
insert into version_info values (1,32) ;


commit ;
