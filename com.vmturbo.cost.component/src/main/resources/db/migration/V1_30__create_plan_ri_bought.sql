-- This table stores all bought reserved instance information for the plan.
CREATE TABLE plan_reserved_instance_bought (
    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT          NOT NULL,

    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id          BIGINT          NOT NULL,

    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info      BLOB            NOT NULL,

    -- Add one count column to reserved instance bought table, it means the number of instances bought
    -- of the reserved instance
    count                              INT             NOT NULL,

-- The foreign key referring to reserved instance spec table.
    FOREIGN  KEY (reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE,

    PRIMARY KEY (id)
);
