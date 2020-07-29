-- we are not currently able to make this change in a pre-existing schema without decompressing
-- existing compressed chunks. So while that's a problem we need to solve for future (rare)
-- schema changes, in this case we will blow away and remove compression configuration prior
-- to performing the schema change. THIS PATTERN SHOULD NOT BE COPIED ONCE XLR IS GA.

TRUNCATE TABLE metric;
SELECT remove_compress_chunks_policy('metric');
ALTER TABLE metric SET (timescaledb.compress=false);

-- now for the actual schema change

-- commodity key to distinguish among multiple commodity buys/sells of the same type.
-- this is null if either no commodity key appears in the topology entry, or if this particular
-- commodity type/entity type combination is configured for aggregation across keys
ALTER TABLE metric ADD COLUMN key text NULL;

-- and finally restore compression settings and policy (copied from V1.0 migration)
ALTER TABLE "metric" SET(
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid',
  timescaledb.compress_orderby = 'type, key'
);
SELECT add_compress_chunks_policy('metric', INTERVAL '2 days');
