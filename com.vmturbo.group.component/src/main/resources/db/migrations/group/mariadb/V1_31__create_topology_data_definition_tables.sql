-- This script adds the tables needed to support Topology Data Definitions

-- persist OIDs for topology data definition objects here. The primary key is 'id',
-- 'identity_matching_attributes'.
CREATE TABLE topology_data_definition_oid (

  -- unique ID for this TargetSpec
  id BIGINT NOT NULL,

  -- identity matching attributes for the TargetSpec
  identity_matching_attributes text NOT NULL,

  -- the unique ID for this TargetSpec
  PRIMARY KEY (id)

) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Table containing information about Manual Topology Data Definitions
CREATE TABLE manual_topo_data_defs (
  -- The ID assigned to the definition. The id is required to be unique.
  id BIGINT NOT NULL UNIQUE,

  -- The name of the entity created by the definition. The name is not required to be unique.
  name VARCHAR(255) NOT NULL,

  -- The entity type created by the definition.
  -- This is the integer representation of the EntityType.
  entity_type INT NOT NULL,

  PRIMARY KEY(id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- Table containing information about Automatic Topology Data Definitions
CREATE TABLE auto_topo_data_defs (
  -- The ID assigned to the definition. The id is required to be unique.
  id BIGINT NOT NULL UNIQUE,

  -- The name of the entity created by the definition. The name is not required to be unique.
  name_prefix VARCHAR(255) NOT NULL,

  -- The entity type created by the definition.
  -- This is the integer representation of the EntityType.
  entity_type INT NOT NULL,

  -- The entity type of related entities.
  -- This is the integer representation of the EntityType.
  connected_entity_type INT NOT NULL,

  -- The tag key for which we will find values to create and associate created entities with.
  tag_key VARCHAR(255) NOT NULL,

  PRIMARY KEY(id)
) ENGINE=INNODB DEFAULT CHARSET=utf8;

-- Table to store static associated entities of a manual topology data definition.
CREATE TABLE man_data_defs_assoc_entities (
    -- id of the topology data definition
    definition_id BIGINT(20),
    -- entity type of associated entity
    entity_type INT(11),
    -- entity id of the associated entity
    entity_id BIGINT(20),
    PRIMARY KEY (definition_id, entity_id),
    CONSTRAINT fk_man_data_defs_assoc_entities_manual_topo_data_defs FOREIGN KEY (definition_id) REFERENCES manual_topo_data_defs (id) ON DELETE CASCADE
);

-- Table to store associated group ids of a manual topology data definition.
CREATE TABLE man_data_defs_assoc_groups (
    -- id of the topology data definition
    definition_id BIGINT(20),
    -- oid of the associated group
    group_oid BIGINT(20),
    -- entity type contained in the group
    entity_type INT(11),
    PRIMARY KEY (definition_id, group_oid),
    CONSTRAINT fk_man_data_defs_assoc_groups_manual_topo_data_defs FOREIGN KEY (definition_id) REFERENCES manual_topo_data_defs (id) ON DELETE CASCADE
);

-- Table to store blobs of entity filters that are associated with manual topology definitions
CREATE TABLE man_data_defs_dyn_connect_filters (
    -- id of the topology data definition
    definition_id BIGINT(20),
    -- entity type defined by filter
    entity_type INT(11),
    -- entity filter represented as a blob
    dynamic_connection_filters BLOB NOT NULL,
    PRIMARY KEY (definition_id),
    CONSTRAINT fk_man_data_defs_dyn_connect_filters_manual_topo_data_defs FOREIGN KEY
    (definition_id) REFERENCES manual_topo_data_defs (id) ON DELETE CASCADE
);

-- View that contains all topology data definitions
CREATE VIEW all_topo_data_defs  AS
  SELECT id, name AS name_or_prefix, 'MANUAL' AS type
  FROM manual_topo_data_defs
UNION
  SELECT id, name_prefix AS name_or_prefix, 'AUTO' AS type
  FROM auto_topo_data_defs;