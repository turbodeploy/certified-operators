-- In migraiton 1.14, we moved existing entity and scope tables to .old versions and recreated
-- the tables according to updated schemas. Here we do the same with the metric table.

ALTER TABLE IF EXISTS metric RENAME TO metric_old;

-- In the new metric table we the entity_hash column, as this is no longer use or populated.
CREATE TABLE "metric" (
  -- timestamp on the topology from which this metric was obtained
  "time" timestamptz NOT NULL,
  -- oid of the entity to which the metric applies
  "entity_oid" int8 NOT NULL,
  -- commodity type
  "type" text NOT NULL,
  -- entity id of seller of this commodity to this buyer, for buy records
  "provider_oid" int8 NULL,
  -- commodity key, if any
  "key" text NULL,
  -- current utilization of commodity in selling entity
  "current" float8 NULL,
  -- capacity of commodity in selling entity
  "capacity" float8 NULL,
  -- utilization of commodity in selling entity
  "utilization" float8 NULL,
  -- amount of commodity currently used by buying entity
  "consumed" float8 NULL
);

-- make it a hypertable
SELECT create_hypertable('metric', 'time', chunk_time_interval => INTERVAL '2 days');

-- set up compression configuration and schedule
ALTER TABLE metric SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid',
  timescaledb.compress_orderby = 'type'
);
SELECT add_compression_policy('metric', INTERVAL '2 days');
