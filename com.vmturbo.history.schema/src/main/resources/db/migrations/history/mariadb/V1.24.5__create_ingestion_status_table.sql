/**
 * Create a new table for persisting status of live topology processing, so that processing
 * can restart more intelligently if the component is restarted.
 */
DROP TABLE IF EXISTS ingestion_status;

CREATE TABLE ingestion_status(
    snapshot_time BIGINT,
    -- This is a JSON serialization of status information for the given snapshot time
    status VARCHAR(10000),
    PRIMARY KEY (snapshot_time));
