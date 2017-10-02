use vmtdb ;

select 'Start DB migration 61->62' as progress;

alter table report_subscriptions 
add column report_id int, 
add column report_type int,
add column format int,
add constraint FOREIGN KEY (report_type) REFERENCES report_type(id);

UPDATE report_subscriptions 
	SET report_id=`standard_report_standard_report_id`, report_type=1
	where `standard_report_standard_report_id` is not null;
	
UPDATE report_subscriptions 
	SET report_id=`on_demand_report_on_demand_report_id`, report_type=2
	where `on_demand_report_on_demand_report_id` is not null;

UPDATE report_subscriptions 
	SET report_id=`custom_report_user_report_id`, report_type=3
	where `custom_report_user_report_id` is not null;
	
-- DELETE PREVIOUS COLUMNS
ALTER TABLE report_subscriptions 
	DROP COLUMN `standard_report_standard_report_id`, 
	DROP COLUMN `on_demand_report_on_demand_report_id`, 
	DROP COLUMN `custom_report_user_report_id`;

-- UI REPORTS TBALE
DROP TABLE IF EXISTS `ui_reports`;

CREATE TABLE `ui_reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `filename` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `username` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` mediumtext COLLATE utf8_unicode_ci,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
	
-- REPORTS ATTRIBUTES TBALE
-- SUBSCRIPTION ATTRIBUTES TBALE
DROP TABLE IF EXISTS `subscription_attrs`;
DROP TABLE IF EXISTS `report_attrs`;

CREATE TABLE `report_attrs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `report_id` int(11) NOT NULL,
  `report_type` tinyint(3) NOT NULL,
  `name` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `default_value` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `att_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`report_id`,`report_type`,`name`),
  KEY `report_id` (`report_id`,`report_type`,`name`),
  KEY `id` (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

LOCK TABLES `report_attrs` WRITE;
delete from report_attrs;
INSERT INTO `report_attrs` (`id`, `report_id`, `report_type`, `name`, `default_value`, `att_type`)
VALUES
	(4,2,2,'num_days_ago','30','int'),
	(2,6,2,'num_days_ago','30','int'),
	(3,6,2,'show_charts','True','boolean'),
	(1,13,2,'num_days_ago','30','int');
UNLOCK TABLES;

CREATE TABLE `subscription_attrs` (
  `subscription_id` int(11) NOT NULL,
  `report_attr_id` int(11) NOT NULL,
  `value` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`subscription_id`,`report_attr_id`),
  KEY `report_attr_id` (`report_attr_id`),
  KEY `subscription_id` (`subscription_id`,`report_attr_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

/*
 * Convert `id` of entities BIGINT
 */
ALTER TABLE entities MODIFY id BIGINT not null auto_increment;
ALTER TABLE entity_assns MODIFY entity_entity_id BIGINT;
ALTER TABLE entity_attrs MODIFY entity_entity_id BIGINT;
ALTER TABLE entity_assns_members_entities MODIFY entity_dest_id BIGINT;
ALTER TABLE classifications_entities_entities MODIFY entity_dest_id BIGINT;

select 'Migration Done' as progress;
