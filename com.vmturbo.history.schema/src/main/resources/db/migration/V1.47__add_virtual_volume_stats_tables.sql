--
-- This migration applies the following steps.
-- 1. Create stats tables for Virtual Volumes:
--    - virtual_volume_stats_latest
--    - virtual_volume_stats_by_hour
--    - virtual_volume_stats_by_day
--    - virtual_volume_stats_by_month
-- 2. Copy Storage commodities from vm_stats_* daily and monthly tables to sold commodities in
--    virtual_volumes_stats_* tables.
--    It is required after adding commodities bought by volumes from storage tiers.
-- 3. Migrate vm_stats_* records for Storage commodities from Storage Tier provider to Volume.
--    That is, update producer_uuid column with Volume UUID retrieved from commodity_key.
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


INSERT INTO virtual_volume_stats_by_day (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity
FROM vm_stats_by_day stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAmount';

INSERT INTO virtual_volume_stats_by_day (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity
FROM vm_stats_by_day stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageProvisioned';

INSERT INTO virtual_volume_stats_by_day (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity
FROM vm_stats_by_day stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAccess';

INSERT INTO virtual_volume_stats_by_day (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  aggregated,
  new_samples,
  day_key,
  month_key,
  effective_capacity
FROM vm_stats_by_day stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageLatency';


INSERT INTO virtual_volume_stats_by_month (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  new_samples,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  new_samples,
  month_key,
  effective_capacity
FROM vm_stats_by_month stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAmount';

INSERT INTO virtual_volume_stats_by_month (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  new_samples,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  new_samples,
  month_key,
  effective_capacity
FROM vm_stats_by_month stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageProvisioned';

INSERT INTO virtual_volume_stats_by_month (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  new_samples,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  new_samples,
  month_key,
  effective_capacity
FROM vm_stats_by_month stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAccess';

INSERT INTO virtual_volume_stats_by_month (
  snapshot_time,
  uuid,
  producer_uuid,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  commodity_key,
  samples,
  new_samples,
  month_key,
  effective_capacity)
SELECT
  snapshot_time,
  commodity_key,
  NULL,
  property_type,
  property_subtype,
  capacity,
  avg_value,
  min_value,
  max_value,
  relation,
  NULL,
  samples,
  new_samples,
  month_key,
  effective_capacity
FROM vm_stats_by_month stats
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageLatency';


UPDATE vm_stats_latest
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAmount';

UPDATE vm_stats_latest
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageProvisioned';

UPDATE vm_stats_latest
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAccess';

UPDATE vm_stats_latest
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageLatency';


UPDATE vm_stats_by_hour
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAmount';

UPDATE vm_stats_by_hour
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageProvisioned';

UPDATE vm_stats_by_hour
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAccess';

UPDATE vm_stats_by_hour
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageLatency';


UPDATE vm_stats_by_day
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAmount';

UPDATE vm_stats_by_day
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageProvisioned';

UPDATE vm_stats_by_day
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAccess';

UPDATE vm_stats_by_day
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageLatency';


UPDATE vm_stats_by_month
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAmount';

UPDATE vm_stats_by_month
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageProvisioned';

UPDATE vm_stats_by_month
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageAccess';

UPDATE vm_stats_by_month
SET
  producer_uuid = commodity_key,
  commodity_key = NULL
WHERE commodity_key REGEXP '^[0-9]+$'
AND property_type = 'StorageLatency';
