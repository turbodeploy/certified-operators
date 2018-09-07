-- This table store the entity with reserved instance coverage mapping, and this data only comes from
-- billing topology.
CREATE TABLE entity_to_reserved_instance_mapping (
    -- The time of last update
    snapshot_time                      TIMESTAMP        NOT NULL,

    -- The id of entity.
    entity_id                          BIGINT           NOT NULL,

    -- The id of reserved instance.
    reserved_instance_id               BIGINT           NOT NULL,

    -- The used coupons amount of the entity covered by the reserved instance.
    used_coupons                       FLOAT            NOT NULL,

    PRIMARY KEY (entity_id, reserved_instance_id)
);

-- This table store the utilization of reserved instance, which means for each reserved instance
-- what is the total coupons it has, and what is the amount of coupons which used by other entities.
CREATE TABLE reserved_instance_utilization_latest (
    -- The time of last update.
    snapshot_time                      TIMESTAMP       NOT NULL,

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The region id of reserved instance.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of reserved instance.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the reserved instance has.
    total_coupons                      FLOAT           NOT NULL,

    -- The amount of coupons which used by other entities.
    used_coupons                       FLOAT           NOT NULL,

    PRIMARY KEY (id, snapshot_time)
);

-- This table store the coverage information of reserved instance, which means for each entity,
-- what is the total coupons it has, and what is the amount of coupons covered by reserved instance.
CREATE TABLE reserved_instance_coverage_latest (
    -- The time of last update.
    snapshot_time                      TIMESTAMP       NOT NULL,

    -- The Id of the entity.
    entity_id                          BIGINT          NOT NULL,

    -- The region id of the entity.
    region_id                          BIGINT          NOT NULL,

    -- The availability zone id of the entity.
    availability_zone_id               BIGINT          NOT NULL,

    -- The business account id of the entity.
    business_account_id                BIGINT          NOT NULL,

    -- The total coupons which the entity has.
    total_coupons                      FLOAT           NOT NULL,

    -- The amount of coupons which covered by reserved instance.
    used_coupons                       FLOAT           NOT NULL,

    PRIMARY KEY (entity_id, snapshot_time)
);

