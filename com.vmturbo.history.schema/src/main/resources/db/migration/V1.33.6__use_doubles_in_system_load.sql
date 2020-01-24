-- NOTE: This migration is a copy of the migration V1.24.6, repeated here so that it will be
-- executed by clients upgrading from a prior 7.21 branch release will execute the migration
-- when upgrading to the 7.21.0 release where 7.17 and 7.21 branches of history component were
-- unified.

/**
 * The system_load table had its capacity and usage columns declared as DECIMAL(15,3), which failed
 * when we started seeing cluster-wide total memory capacity exceeding a terabyte. There's no need
 * for decimal fraction fidelity in these numbers, so switching to DOUBLE should suffice from here
 * on out.
 *
 * In addition, the current indexing does not support a pruning process that is being introduced
 * separately in the 7.17.5 release, relies on DB operations that delete records based on the
 * snapshot_time column. Currently, the sole index on the table is a composite index over the
 * fields (slice, property_type, snapshot_time), which cannot be used for these delete operations.
 * Accordingly, we will redefine the index so that snapshot_time comes first.
 *
 * Because there was previously no pruning of system_load tables, these changes may take a very
 * long time to complete in large customer installations. For that reason, we'll' drop and recreate
 * the table from scratch. The downside is that an upgrading customer will then have to build up
 * system_load data for a few days before seeing headrooms stats.
 */

DROP TABLE IF EXISTS system_load;

CREATE TABLE system_load (
  slice varchar(250) DEFAULT NULL,
  snapshot_time datetime DEFAULT NULL,
  uuid varchar(80) DEFAULT NULL,
  producer_uuid varchar(80) DEFAULT NULL,
  property_type varchar(36) DEFAULT NULL,
  property_subtype varchar(36) DEFAULT NULL,
  capacity double DEFAULT NULL,
  avg_value double DEFAULT NULL,
  min_value double DEFAULT NULL,
  max_value double DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) DEFAULT NULL,
  KEY snapshot_time_slice_property_type (snapshot_time, slice, property_type)
);
