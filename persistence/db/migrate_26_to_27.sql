

use vmtdb ;

start transaction ;

DROP TABLE IF EXISTS on_demand_reports;
CREATE TABLE on_demand_reports (
  id int NOT NULL auto_increment,
  obj_type varchar(80) default NULL,
  scope_type varchar(80) default NULL,
  short_desc varchar(80) default NULL,
  filename varchar(80) default NULL,
  description mediumtext default NULL,
  category varchar(80) default NULL,
  title varchar(80) default NULL,
  PRIMARY KEY  (id)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci ;



ALTER TABLE report_subscriptions ADD COLUMN obj_uuid varchar(80) ;
ALTER TABLE report_subscriptions ADD COLUMN on_demand_report_on_demand_report_id int ;

--
-- Apply constraints
--

ALTER TABLE report_subscriptions ADD CONSTRAINT fk_report_subscriptions_on_demand_report_on_demand_report_id FOREIGN KEY (on_demand_report_on_demand_report_id) REFERENCES on_demand_reports (id) on delete set null ;


delete from version_info ;
insert into version_info values (1,27) ;

commit ;
