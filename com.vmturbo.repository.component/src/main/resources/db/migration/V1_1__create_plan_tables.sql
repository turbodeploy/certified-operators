
-- Bookkeeping information about the topologies known to the repository.
-- Used to quickly look up the source/projected topology in a particular context and for
-- referential integrity.
CREATE TABLE topology_metadata (
    -- The OID of the topology, as broadcast by the topology processor.
    topology_oid BIGINT NOT NULL,

    -- The OID of the topology context this topology belongs to.
    context_oid BIGINT NOT NULL,

    -- The type of topology (see the TopologyType enum).
    topology_type TINYINT NOT NULL,

    -- Miscellaneous information about the topology, mainly to aid debugging.
    debug_info TEXT NOT NULL,

    -- The status of the topology, indicating whether it's fully ingested and ready for queries.
    status TINYINT NOT NULL,

    PRIMARY KEY (topology_oid)
);

-- We often want to look up topologies by context id and type.
CREATE INDEX topology_by_plan ON topology_metadata(context_oid, topology_type);

-- Information about each entity in a particular topology.
-- All information that is supported for queries is extracted into fields, and the rest of
-- the TopologyEntityDTO is compressed and stored as a blob.
CREATE TABLE plan_entity (
    -- The OID of the topology the entity belongs to.
    topology_oid BIGINT NOT NULL,

    -- The OID of the entity.
    entity_oid BIGINT NOT NULL,

    -- The type of the entity.
    entity_type SMALLINT NOT NULL,

    -- The origin of the entity, derived from the Origin of the TopologyEntityDTO.
    origin TINYINT NOT NULL,

    -- The state of the entity, derived from the EntityState of the TopologyEntityDTO.
    entity_state TINYINT NOT NULL,

    -- Whether or not this entity is placed (i.e. had providers for all its commodities bought).
    is_placed TINYINT DEFAULT TRUE,

    -- The environment type of the entity, derived from the EnvironmentType enum.
    environment_type TINYINT NOT NULL,

    -- The compressed entity blob.
    entity MEDIUMBLOB NOT NULL,

    -- The uncompressed size of the entity, to speed up decompression.
    entity_uncompressed_size INT NOT NULL,

    PRIMARY KEY (topology_oid, entity_oid),

    -- The foreign key is for consistency, but we don't rely on cascades
    -- for deletion.
    FOREIGN KEY (topology_oid) REFERENCES topology_metadata(topology_oid) ON DELETE CASCADE
);

-- We often want to look up entities by type. The index will help avoid a table scan.
CREATE INDEX plan_entity_type ON plan_entity(topology_oid, entity_type, entity_oid);

-- Stores the price index of plan entities.
-- The market calculates the source and projected price index and broadcasts it as part of the
-- projected topology. We save it separately so we can access it for queries on both the source
-- and projected topology entities.
CREATE TABLE price_index (
    -- The OID of the projected topology the price index came from.
    -- This is the primary key because the price index is ingested together with the
    -- projected topology.
    topology_oid BIGINT NOT NULL,

    entity_oid BIGINT NOT NULL,

    -- We do not currently store the source topology id here, because we can find the projected
    -- topology id associated with a source topology id in the topology metadata. We also can't
    -- have a foreign key constraint to the source topology id because the source topology is
    -- ingested at a different time (and not necessarily earlier) than the projected topology.

    -- The price index of the entity in the source topology.
    original DOUBLE NOT NULL,

    -- The price index of the entity in the projected topology.
    projected DOUBLE NOT NULL,

    PRIMARY KEY (topology_oid, entity_oid),

    -- The foreign keys are for consistency, but we don't rely on cascades
    -- for deletion.
    FOREIGN KEY (topology_oid, entity_oid) REFERENCES plan_entity(topology_oid, entity_oid),
    FOREIGN KEY (topology_oid) REFERENCES topology_metadata(topology_oid)
);

-- Information about the scope of a particular plan entity. The scope is all the entities related
-- to this entity, calculated using our supply chain traversal library.
CREATE TABLE plan_entity_scope (
    -- The OID of the topology.
    topology_oid BIGINT NOT NULL,

    -- The OID of the entity
    entity_oid BIGINT NOT NULL,

    -- The compressed supply chain of the entity.
    supply_chain MEDIUMBLOB  NOT NULL,

    -- The uncompressed size of the supply chain, to speed up decompression.
    supply_chain_uncompressed_size INT NOT NULL,

    PRIMARY KEY (topology_oid, entity_oid),

    -- The foreign keys are for consistency, but we don't rely on cascades
    -- for deletion.
    FOREIGN KEY (topology_oid) REFERENCES topology_metadata(topology_oid) ON DELETE CASCADE,
    FOREIGN KEY (topology_oid, entity_oid) REFERENCES plan_entity(topology_oid, entity_oid) ON DELETE CASCADE
);
