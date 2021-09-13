-- Drops
DROP INDEX IF EXISTS entity_savings_by_oid_type_time;
DROP TABLE IF EXISTS entity_savings;
DROP TYPE IF EXISTS savings_type;

-- enum to represent savings types, modeled after EntitySavingsStatsType in Cost.proto.
CREATE TYPE savings_type AS ENUM (
  'REALIZED_SAVINGS',
  'REALIZED_INVESTMENTS',
  'MISSED_SAVINGS',
  'MISSED_INVESTMENTS'
);

-- hypertable to represent savings data, intended to be updated each hour.
CREATE TABLE entity_savings (
  -- hour start timestamp of data stored. E.g 1:00 PM timstamp will have data for 1:00 - 2:00 PM time period.
  time timestamptz NOT NULL,
  -- entity oid
  entity_oid bigint NOT NULL,
  -- type of savings, e.g REALIZED_SAVINGS.
  savings_type savings_type NOT NULL,
  -- savings/investment (dollars per hour)
  stats_value float4 NOT NULL
);

CREATE INDEX IF NOT EXISTS entity_savings_by_oid_type_time ON entity_savings (entity_oid, savings_type, time);

-- set up hypertable configuration
SELECT create_hypertable('entity_savings', 'time', chunk_time_interval => INTERVAL '2 days');
ALTER TABLE entity_savings SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid, savings_type'
);

SELECT add_compression_policy('entity_savings', INTERVAL '2 days');
SELECT add_retention_policy('entity_savings', INTERVAL '24 months');
