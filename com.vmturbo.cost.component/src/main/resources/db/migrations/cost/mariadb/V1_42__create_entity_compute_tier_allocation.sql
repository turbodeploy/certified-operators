CREATE TABLE entity_compute_tier_allocation (

    start_time                   TIMESTAMP(3)    NOT NULL DEFAULT 0,
    end_time                     TIMESTAMP(3)    NOT NULL DEFAULT 0,
    entity_oid                   BIGINT          NOT NULL,
    os_type                      INT             NOT NULL,
    tenancy                      INT             NOT NULL,
    allocated_compute_tier_oid   BIGINT          NOT NULL,

    -- entity_oid should be first in PK to allow easy querying through cloud scope
    PRIMARY KEY (entity_oid, start_time),
    FOREIGN KEY (entity_oid)
        REFERENCES entity_cloud_scope(entity_oid)
        ON DELETE NO ACTION
        ON UPDATE NO ACTION
);

CREATE INDEX idx_entity_compute_tier_allocation_end_time ON entity_compute_tier_allocation(end_time);

