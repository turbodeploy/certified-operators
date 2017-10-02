
use vmtdb ;
go

IF OBJECT_ID(N'customer_info', N'U') IS NOT NULL drop table customer_info;
create table customer_info (
    customer_name        varchar(80) not null,
    image_name           varchar(250),
    copyright            varchar(250),
    primary key (customer_name)
) ;
go

/* add default data to customer_info table */
delete from customer_info;
insert into customer_info values ('VMTurbo', '/srv/reports/images/logo.jpg', '/srv/reports/images/copyright.jpg') ;
go

SET IDENTITY_INSERT version_info ON

delete from version_info ;
insert into version_info (id,version) values (1,32) ;

SET IDENTITY_INSERT version_info OFF
go
