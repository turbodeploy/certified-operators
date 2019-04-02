-- This table stores all buy RI recommendations for plans and for real time.
CREATE TABLE buy_reserved_instance (
    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
    topology_context_id                BIGINT        NOT NULL,

    -- The business account id owns this reserved instance.
    business_account_id                BIGINT          NOT NULL,

    -- The region of the Buy RI
    region_id                          BIGINT           NOT NULL,

    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id          BIGINT          NOT NULL,

    -- The number of instances to buy of the reserved instance
    count                               INT             NOT NULL,

    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info      BLOB            NOT NULL,

    -- The foreign key referring to reserved instance spec table.
    FOREIGN  KEY (reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE,

    PRIMARY KEY (id)
);

-- Add an index to make finding market actions by their action plan or topology context
CREATE INDEX buy_reserved_instance_topology_context_id_idx ON buy_reserved_instance (topology_context_id);