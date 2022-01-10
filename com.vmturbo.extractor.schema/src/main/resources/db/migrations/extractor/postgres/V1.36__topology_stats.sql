DROP TABLE IF EXISTS topology_stats;

CREATE TABLE topology_stats (
  time timestamptz NOT NULL,
  attrs jsonb NULL
);

-- Set up hypertable configuration
SELECT create_hypertable('topology_stats', 'time', chunk_time_interval => INTERVAL '1 month');

SELECT add_retention_policy('topology_stats', INTERVAL '12 months');

-- Copy the topology time from scope table to the topology_stats table.
INSERT INTO topology_stats
SELECT finish AS time, '{}' AS attrs FROM scope WHERE seed_oid = 0 AND scoped_oid = 0 AND scoped_type='_NONE_';

-- Remove the topology time record from the scope table.
DELETE FROM scope WHERE seed_oid = 0 AND scoped_oid = 0 AND scoped_type='_NONE_';
