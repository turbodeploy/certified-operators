-- This table stores entity scoping information as a consolidated table for child tables
-- focused on entity statistics
CREATE TABLE entity_cloud_scope (

    entity_oid              BIGINT          NOT NULL,
    account_oid             BIGINT          NOT NULL,
    region_oid              BIGINT          NOT NULL,
    -- The availability zone OID may be null, given Azure allows workloads to be either regional
    -- or zonal resources.
    availability_zone_oid   BIGINT          DEFAULT NULL,
    service_provider_oid    BIGINT          NOT NULL,
    creation_time           TIMESTAMP       NOT NULL,

    PRIMARY KEY (entity_oid)
);


-- An index is created for each scope attribute which may reasonably be used as the primary filter
-- in querying entity data. While an index on each may be relatively expensive for a single table,
-- the storage cost of indexing is expected to be amortized over several FK'd child tables.
CREATE INDEX idx_entity_cloud_scope_account_oid ON entity_cloud_scope(account_oid);
CREATE INDEX idx_entity_cloud_scope_region_oid ON entity_cloud_scope(region_oid);
CREATE INDEX idx_entity_cloud_scope_availability_zone_oid ON entity_cloud_scope(availability_zone_oid);
CREATE INDEX idx_entity_cloud_scope_service_provider_oid ON entity_cloud_scope(service_provider_oid);