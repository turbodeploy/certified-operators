-- Create stats tables for new cloud native entities:
--    Container Platform Cluster such as Kubernetes

--
-- Table structure for Container Platform Cluster entity stats tables
--
DROP TABLE IF EXISTS container_cluster_stats_latest;
CREATE TABLE IF NOT EXISTS container_cluster_stats_latest (
  snapshot_time datetime DEFAULT NULL,
  uuid varchar(80) DEFAULT NULL,
  producer_uuid varchar(80) DEFAULT NULL,
  property_type varchar(36) DEFAULT NULL,
  property_subtype varchar(36) DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) DEFAULT NULL,
  hour_key varchar(32) DEFAULT NULL,
  day_key varchar(32) DEFAULT NULL,
  month_key varchar(32) DEFAULT NULL,
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time)) (
  PARTITION start VALUES LESS THAN (0),
  PARTITION future VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS container_cluster_stats_by_hour;
CREATE TABLE IF NOT EXISTS container_cluster_stats_by_hour (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) DEFAULT NULL,
  producer_uuid varchar(80) DEFAULT NULL,
  property_type varchar(36) DEFAULT NULL,
  property_subtype varchar(36) DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  hour_key varchar(32) NOT NULL,
  day_key varchar(32) DEFAULT NULL,
  month_key varchar(32) DEFAULT NULL,
  PRIMARY KEY (hour_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time)) (
  PARTITION start VALUES LESS THAN (0),
  PARTITION future VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS container_cluster_stats_by_day;
CREATE TABLE IF NOT EXISTS container_cluster_stats_by_day (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) DEFAULT NULL,
  producer_uuid varchar(80) DEFAULT NULL,
  property_type varchar(36) DEFAULT NULL,
  property_subtype varchar(36) DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  day_key varchar(32) NOT NULL,
  month_key varchar(32) DEFAULT NULL,
  PRIMARY KEY (day_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time)) (
  PARTITION start VALUES LESS THAN (0),
  PARTITION future VALUES LESS THAN MAXVALUE
);

DROP TABLE IF EXISTS container_cluster_stats_by_month;
CREATE TABLE IF NOT EXISTS container_cluster_stats_by_month (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) DEFAULT NULL,
  producer_uuid varchar(80) DEFAULT NULL,
  property_type varchar(36) DEFAULT NULL,
  property_subtype varchar(36) DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  month_key varchar(32) NOT NULL,
  PRIMARY KEY (month_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY RANGE (to_seconds(snapshot_time)) (
  PARTITION start VALUES LESS THAN (0),
  PARTITION future VALUES LESS THAN MAXVALUE
);
