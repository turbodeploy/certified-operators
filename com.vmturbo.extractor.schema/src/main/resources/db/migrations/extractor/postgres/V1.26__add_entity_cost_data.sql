-- enum to represent cost categories, modeled after the definition in Cost.proto
DROP TYPE IF EXISTS cost_category;
CREATE TYPE cost_category AS ENUM (
  'ON_DEMAND_COMPUTE',
  'STORAGE',
  'IP',
  'ON_DEMAND_LICENSE',
  'RI_COMPUTE',
  'SPOT',
  'RESERVED_LICENSE',
  -- pseudo-member that is the sum of cost across all other categories & sources; this will always
  -- appear with `TOTAL` cost source
  'TOTAL'
);

-- enum to represent cost sources, modeled after the definition in Cost.proto
DROP TYPE IF EXISTS cost_source;
CREATE TYPE cost_source AS ENUM (
  'ON_DEMAND_RATE',
  'RI_INVENTORY_DISCOUNT',
  'BUY_RI_DISCOUNT',
  'UNCLASSIFIED',
  'ENTITY_UPTIME_DISCOUNT',
  -- pseudo-member that is the sum across all sources for a given category
  'TOTAL'
);

-- hypertable to represent entity cost data, intended to be updated with each topology cycle
DROP TABLE IF EXISTS entity_cost;
CREATE TABLE entity_cost (
  -- snapshot time of topology from which this record was created
  time timestamptz NOT NULL,
  -- entity oid
  entity_oid bigint NOT NULL,
  -- cost category (represents something for which CSP assesses a charge associated with this entity)
  category cost_category NOT NULL,
  -- cost source (contributors to the charge for a given cost category, e.g. things like a charge
  -- computed from a rate card, applicable discounts or surcharges, etc.
  source cost_source NOT NULL,
  -- cost value (dollars per hour)
  cost float4 NOT NULL
);

CREATE INDEX IF NOT EXISTS entity_cost_by_oid_cat_time ON entity_cost (entity_oid, category, time);

-- set up hypertable configuration
SELECT create_hypertable('entity_cost', 'time', chunk_time_interval => INTERVAL '2 days');
ALTER TABLE entity_cost SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid, category',
  timescaledb.compress_orderby = 'source'
);

SELECT add_compression_policy('entity_cost', INTERVAL '2 days');
SELECT add_retention_policy('entity_cost', INTERVAL '12 months');
