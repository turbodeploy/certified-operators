-- This table store projected entity to reserved instance mapping
CREATE TABLE plan_projected_entity_to_reserved_instance_mapping (

    -- The id of entity.
    entity_id                          BIGINT           NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT           NOT NULL,

    -- The id of reserved instance.
    reserved_instance_id               BIGINT           NOT NULL,

    -- The used coupons amount of the entity covered by the reserved instance.
    used_coupons                       FLOAT            NOT NULL,

    PRIMARY KEY (entity_id, plan_id, reserved_instance_id)
);

-- This table store the projected utilization of reserved instance, which means for each reserved instance
-- what is the total coupons it has, and what is the amount of coupons which used by other entities.
CREATE TABLE plan_projected_reserved_instance_utilization (

    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT           NOT NULL,

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

    PRIMARY KEY (id, plan_id)
);

-- This table store the projected coverage information of reserved instance, which means for each entity,
-- what is the total coupons it has, and what is the amount of coupons covered by reserved instance.
CREATE TABLE plan_projected_reserved_instance_coverage (

    -- The Id of the entity.
    entity_id                          BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT           NOT NULL,

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

    PRIMARY KEY (entity_id, plan_id)
);

-- This table stores all projected entity costs information.
CREATE TABLE plan_projected_entity_cost (
    -- The id of plan topology context.
    plan_id                         BIGINT           NOT NULL,

    -- The associated entity id
    associated_entity_id            BIGINT          NOT NULL,

    entity_cost                     BLOB             NOT NULL,

    PRIMARY KEY (plan_id, associated_entity_id)
);

