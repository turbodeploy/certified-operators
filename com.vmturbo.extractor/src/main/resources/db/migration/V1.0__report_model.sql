/*
** Initial schema for the extractor component, covering tables required for reporting.
*/

-- entity table - contains info about entities appearing in topologies.
-- more than one record can appear with the same oid, because associated data may change.
-- in that case, hash values will differ.
DROP TABLE IF EXISTS "entity";
CREATE TABLE "entity" (
  -- entity oid (included in hash)
  "oid" int8 NOT NULL,
  -- hash of entity data
  "hash" int8 NOT NULL,
  -- entity type (included in hash)
  "type" text NOT NULL,
  -- entity display name (included in hash)
  "name" text NOT NULL,
  -- entity environment type (included in hash)
  "environment" text NULL,
  -- entity state (included in hash)
  "state" text NULL,
  -- entity type-specific info (included in hash)
  "attrs" jsonb NULL,
  -- entity/group ids in scope (included in hash)
  "scope" int8[] NOT NULL,
  -- topology timestamp where this entity first appeared with this hash
  "first_seen" timestamptz NOT NULL,
  -- topology timestamp where this entity last appeared with this hash.
  -- this value is always correct - it is often several hours beyond the correct value, to
  -- accommodate the fact that this value is only updated periodically for entities that
  -- remain in the topology over several cycles
  "last_seen" timestamptz NOT NULL
);
CREATE UNIQUE INDEX "entity_entityByOid" ON "entity" USING btree ("oid", "hash");
CREATE INDEX "entity_entityByScopeOid" ON "entity" USING gin ("scope");

-- metric table - contains metric values for entities that apeared in the topology.
-- most metrics are values associated with commodities bought or sold by the entity
-- sold and bought commodities appear in separate records - one for buyer, one for seller
DROP TABLE IF EXISTS "metric";
CREATE TABLE "metric" (
  -- timestamp on the topology from which this metric was obtained
  "time" timestamptz NOT NULL,
  -- oid of the entity to which the metric applies
  "entity_oid" int8 NOT NULL,
  -- hash of that entity as it appeared in this topology
  "entity_hash" int8 NOT NULL,
  -- comomodity type
  "type" text NOT NULL,
  -- current utilization of comoodity in selling entity
  "current" float8 NULL,
  -- capacity of commodity in selling entity
  "capacity" float8 NULL,
  -- utilization of commodity in selling entity
  "utilization" float8 NULL,
  -- amount of commodity currently used by buying entity
  "consumed" float8 NULL,
  -- entity id of seller of this commodity to this buyer
  "provider_oid" int8 NULL
);
CREATE INDEX "metric_time" ON "metric" USING brin ("time");
CREATE INDEX "metric_index" ON "metric" USING btree ("entity_oid", "entity_hash", "type", "time" DESC);
SELECT create_hypertable('metric', 'time', chunk_time_interval => INTERVAL '2 days');
ALTER TABLE "metric" SET(
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid, entity_hash, type');
SELECT add_compress_chunks_policy('metric', INTERVAL '2 days');

