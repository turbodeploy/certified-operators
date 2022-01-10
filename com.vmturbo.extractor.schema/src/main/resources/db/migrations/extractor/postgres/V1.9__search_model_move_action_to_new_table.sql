/*
** New schema for the search. Moved num_actions and severity to a separate table.
*/

-- entity table - contains info about entities/groups appearing in topologies.
-- it only contains entities of latest version, no duplicate oids are allowed.
DROP TABLE IF EXISTS search_entity;
CREATE TABLE search_entity (
    -- entity oid
    "oid" int8 NOT NULL,
    -- entity type (or group type)
    "type" entity_type NOT NULL,
    -- entity (or group) display name
    "name" text NOT NULL,
    -- entity environment type (null for group)
    "environment" environment_type NULL,
    -- entity state (null for group)
    "state" entity_state NULL,
    -- commodities, entity/group type-specific info, etc.
    "attrs" jsonb NULL
);

-- create index
CREATE INDEX idx_type ON search_entity USING btree (type);

-- action table for search entity
DROP TABLE IF EXISTS search_entity_action;
CREATE TABLE search_entity_action (
    -- entity oid
    "oid" int8 NOT NULL,
    -- number of actions for this entity (or group)
    "num_actions" int4 NULL,
    -- entity (or group) severity
    "severity" severity NULL
);
