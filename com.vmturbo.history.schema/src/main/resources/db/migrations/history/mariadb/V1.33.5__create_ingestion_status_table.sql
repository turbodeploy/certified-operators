-- NOTE: This migration is a copy of the migration V1.24.5, repeated here so that it will be
-- executed by clients upgrading from a prior 7.21 branch release will execute the migration
-- when upgrading to the 7.21.0 release where 7.17 and 7.21 branches of history component were
-- unified.

/**
 * Create a new table for persisting status of live topology processing, so that processing
 * can restart more intelligently if the component is restarted.
 *
 * Upgrades from some 7.17 branches will already have created this table, so we use
 * CREATE...IF NOT EXISTS rather than DROP...IF EXISTS followed by CREATE
 */

CREATE TABLE IF NOT EXISTS ingestion_status(
    snapshot_time BIGINT,
    -- This is a JSON serialization of status information for the given snapshot time
    status VARCHAR(10000),
    PRIMARY KEY (snapshot_time));
