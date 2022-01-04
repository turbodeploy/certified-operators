-- remove data and compression config from metric table
TRUNCATE TABLE metric;
SELECT remove_compression_policy('metric');
ALTER TABLE metric SET (timescaledb.compress=false);

-- change type column to use metric_type enum rather than text
ALTER TABLE metric ALTER COLUMN type TYPE metric_type USING type::metric_type;
-- peak amount of current used commodity in selling entity
ALTER TABLE metric ADD COLUMN "peak_current" float8 NULL;
-- peak amount of commodity currently used by buying entity
ALTER TABLE metric ADD COLUMN "peak_consumed" float8 NULL;
-- type of the entity the column entity_oid is referring to
ALTER TABLE metric ADD COLUMN "entity_type" entity_type NOT NULL;

-- create index for new metric table since old index was used for metric_old
CREATE INDEX "metric_time_new" ON "metric" USING brin ("time");
CREATE INDEX "metric_index_new" ON "metric" USING btree ("entity_oid", "type", "time" DESC);

-- compression setting and policy
ALTER TABLE "metric" SET(
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid',
  timescaledb.compress_orderby = 'type, key');
SELECT add_compression_policy('metric', INTERVAL '2 days');