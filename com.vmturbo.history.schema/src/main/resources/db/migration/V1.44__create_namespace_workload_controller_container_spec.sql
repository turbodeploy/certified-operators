-- Create stats tables for new cloud native entities:
--    Namespace, Workload Controller, and Container Spec

--
-- Table structure for Namespace entity stats tables
--
-- Kubernetes supports multiple virtual clusters backed by the same physical cluster.
-- These virtual clusters are called namespaces.
--
DROP TABLE IF EXISTS nspace_stats_latest;
CREATE TABLE nspace_stats_latest (
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

DROP TABLE IF EXISTS nspace_stats_by_hour;
CREATE TABLE nspace_stats_by_hour (
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

DROP TABLE IF EXISTS nspace_stats_by_day;
CREATE TABLE nspace_stats_by_day (
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

DROP TABLE IF EXISTS nspace_stats_by_month;
CREATE TABLE nspace_stats_by_month (
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

--
-- Table structure for Workload Controller entity stats tables
--
-- In Kubernetes, controllers are control loops that watch the state of your cluster,
-- then make or request changes where needed. Each controller tries to move the
-- current cluster state closer to the desired state.
--
DROP TABLE IF EXISTS wkld_ctl_stats_latest;
CREATE TABLE wkld_ctl_stats_latest (
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

DROP TABLE IF EXISTS wkld_ctl_stats_by_hour;
CREATE TABLE wkld_ctl_stats_by_hour (
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

DROP TABLE IF EXISTS wkld_ctl_stats_by_day;
CREATE TABLE wkld_ctl_stats_by_day (
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

DROP TABLE IF EXISTS wkld_ctl_stats_by_month;
CREATE TABLE wkld_ctl_stats_by_month (
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

--
-- Table structure for Container Spec entity stats tables
--
-- In Kubernetes, individual containers managed by Controllers are defined
-- via configuration and individual container instances are spun up or down in pods
-- to match the configured specification. We model this shared container specification
-- defining a like set of container replicas managed by a Controller via the
-- ContainerSpec entity type.
--
DROP TABLE IF EXISTS cnt_spec_stats_latest;
CREATE TABLE cnt_spec_stats_latest (
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

DROP TABLE IF EXISTS cnt_spec_stats_by_hour;
CREATE TABLE cnt_spec_stats_by_hour (
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

DROP TABLE IF EXISTS cnt_spec_stats_by_day;
CREATE TABLE cnt_spec_stats_by_day (
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

DROP TABLE IF EXISTS cnt_spec_stats_by_month;
CREATE TABLE cnt_spec_stats_by_month (
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