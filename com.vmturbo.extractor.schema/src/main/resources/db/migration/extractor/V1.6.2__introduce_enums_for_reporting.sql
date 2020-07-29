-- We will now use enums for `type` columns in entity and metric tables, rather than
-- `text` columns

-- For entity type, we can use the `entity_type` enum introduced for search component in V1.2
-- migration, but we need to adapt it in two ways:
-- - missing entity types need to be added, since for reporting we don't exclude any entity types
-- - group types need to be renamed to conform with the group type names used in reporting

-- new entity_type enum values are added in V1.5.1, which contains an explanation of the reason
-- for splitting them out to their own migration

-- rename group types in entity_type
ALTER TYPE entity_type RENAME VALUE 'REGULAR' TO 'GROUP';
ALTER TYPE entity_type RENAME VALUE 'RESOURCE' TO 'RESOURCE_GROUP';
ALTER TYPE entity_type RENAME VALUE 'COMPUTE_HOST_CLUSTER' TO 'COMPUTE_CLUSTER';
ALTER TYPE entity_type RENAME VALUE 'COMPUTE_VIRTUAL_MACHINE_CLUSTER' TO 'K8S_CLUSTER';

-- New metric_type enum for metric.type column - includes all white-listed commodity types
-- and special-case values
DROP TYPE IF EXISTS metric_type;
CREATE TYPE metric_type AS ENUM (
    -- white-listed commodity types
    'ACTIVE_SESSIONS',
    'BALLOONING',
    'BUFFER_COMMODITY',
    'CONNECTION',
    'CPU',
    'CPU_ALLOCATION',
    'CPU_PROVISIONED',
    'DB_CACHE_HIT_RATE',
    'DB_MEM',
    'EXTENT',
    'FLOW',
    'FLOW_ALLOCATION',
    'HEAP',
    'IMAGE_CPU',
    'IMAGE_MEM',
    'IMAGE_STORAGE',
    'IO_THROUGHPUT',
    'MEM',
    'MEM_ALLOCATION',
    'MEM_PROVISIONED',
    'NET_THROUGHPUT',
    'POOL_CPU',
    'POOL_MEM',
    'POOL_STORAGE',
    'PORT_CHANEL',
    'Q1_VCPU',
    'Q2_VCPU',
    'Q3_VCPU',
    'Q4_VCPU',
    'Q5_VCPU',
    'Q6_VCPU',
    'Q7_VCPU',
    'Q8_VCPU',
    'Q16_VCPU',
    'Q32_VCPU',
    'Q64_VCPU',
    'QN_VCPU',
    'REMAINING_GC_CAPACITY',
    'RESPONSE_TIME',
    'SLA_COMMODITY',
    'STORAGE_ACCESS',
    'STORAGE_ALLOCATION',
    'STORAGE_AMOUNT',
    'STORAGE_LATENCY',
    'STORAGE_PROVISIONED',
    'SWAPPING',
    'THREADS',
    'TRANSACTION',
    'TRANSACTION_LOG',
    'VCPU',
    'VCPU_LIMIT_QUOTA',
    'VCPU_REQUEST',
    'VCPU_REQUEST_QUOTA',
    'VMEM',
    'VMEM_LIMIT_QUOTA',
    'VMEM_REQUEST',
    'VMEM_REQUEST_QUOTA',
    'VSTORAGE',
    'TOTAL_SESSIONS',
    -- special case for Q*_VCPU commodities. sold commodities are recorded normally, but bought
    -- commodities are transformed into sold commodities of type CPU_READY
    'CPU_READY'
);

-- Alter table definitions to use enums instead of existing text columns.

-- This migration truncates the metric table because otherwise the fact that it
-- is a hypertable with a compression configuration would cause schema alterations to fail.
-- This is not an approach we will be permitted to use after XLR is GA.

-- remove data and compression config from metric table
TRUNCATE TABLE metric;
SELECT remove_compress_chunks_policy('metric');
ALTER TABLE metric SET (timescaledb.compress=false);

-- make desired schema changes
ALTER TABLE entity ALTER COLUMN type TYPE entity_type USING type::entity_type;
ALTER TABLE entity ALTER COLUMN environment TYPE environment_type USING environment::environment_type;
ALTER TABLE entity ALTER COLUMN state TYPE entity_state USING state::entity_state;
ALTER TABLE metric ALTER COLUMN type TYPE metric_type USING type::metric_type;

-- and then restore compression settings and policy (copied from V1.4 migration)
ALTER TABLE "metric" SET(
  timescaledb.compress,
  timescaledb.compress_segmentby = 'entity_oid',
  timescaledb.compress_orderby = 'type, key'
);
SELECT add_compress_chunks_policy('metric', INTERVAL '2 days');
