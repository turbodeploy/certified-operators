--
-- This migration creates stats tables for Virtual Volumes:
--    - virtual_volume_stats_latest
--    - virtual_volume_stats_by_hour
--    - virtual_volume_stats_by_day
--    - virtual_volume_stats_by_month
--

CREATE TABLE IF NOT EXISTS `virtual_volume_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `effective_capacity` decimal(15,3) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE);


CREATE TABLE IF NOT EXISTS `virtual_volume_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `effective_capacity` decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE);


CREATE TABLE IF NOT EXISTS `virtual_volume_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `effective_capacity` decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE);


CREATE TABLE IF NOT EXISTS `virtual_volume_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `effective_capacity` decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
 PARTITION future VALUES LESS THAN MAXVALUE);
