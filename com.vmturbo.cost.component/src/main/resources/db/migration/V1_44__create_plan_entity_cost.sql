-- The Cloud entity cost table. It can store calculated and predicted (from Market to Cost component)
-- entity cost, help persist to DB, and help calculate cost from REST API requests.

-- This table stores all entity costs information.
CREATE TABLE plan_entity_cost (
    -- The associated entity id
    associated_entity_id              BIGINT          NOT NULL,

    -- The timestamp at which the entity cost is calculated in Cost component.
    -- The time is UTC.
    created_time                      TIMESTAMP       NOT NULL,

    -- The associated entity type, e.g. vm_spend, app_spend
    associated_entity_type            INT             NOT NULL,

    -- The cost type, e.g. LICENSE, IP, STORAGE, COMPUTE
    cost_type                         INT             NOT NULL,

    -- The currency code, default to USD
    currency                          INT             NOT NULL,

    -- The cost amount for a given cost_type.
    -- DECIMAL(20, 7): 20 is the precision and 7 is the scale.
    amount                            DECIMAL(20, 7)  NOT NULL,

    cost_source                       INT(11)         NOT NULL DEFAULT 4,
    account_id                        BIGINT(20)      DEFAULT 0,
    region_id                         BIGINT(20)      DEFAULT 0,
    availability_zone_id              BIGINT(20)      DEFAULT 0,
    -- The id of plan topology context.
    plan_id                           BIGINT          NOT NULL,

    PRIMARY KEY (associated_entity_id, plan_id, cost_type, cost_source)
);
CREATE INDEX ec_pid ON plan_entity_cost (plan_id);
