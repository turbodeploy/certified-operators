/*
** Initial schema for the search.
*/

-- entity_type enum, which includes all entity types used for search
-- note: it also include group types since we want to put groups into same table
DROP TYPE IF EXISTS entity_type;
CREATE TYPE entity_type AS ENUM (
    -- these are for entity types
    'APPLICATION',
    'APPLICATION_COMPONENT',
    'BUSINESS_ACCOUNT',
    'BUSINESS_APPLICATION',
    'BUSINESS_TRANSACTION',
    'BUSINESS_USER',
    'CHASSIS',
    'CONTAINER',
    'CONTAINER_POD',
    'DATABASE',
    'DATABASE_SERVER',
    'DATACENTER',
    'DESKTOP_POOL',
    'DISK_ARRAY',
    'IO_MODULE',
    'NETWORK',
    'REGION',
    'PHYSICAL_MACHINE',
    'SERVICE',
    'STORAGE',
    'STORAGE_CONTROLLER',
    'SWITCH',
    'VIRTUAL_MACHINE',
    'VIEW_POD',
    'VIRTUAL_VOLUME',
    -- these are for group types
    'REGULAR',
    'RESOURCE',
    'COMPUTE_HOST_CLUSTER',
    'COMPUTE_VIRTUAL_MACHINE_CLUSTER',
    'STORAGE_CLUSTER',
    'BILLING_FAMILY'
);

-- environment_type enum, which includes all possible environment types for entity
DROP TYPE IF EXISTS environment_type;
CREATE TYPE environment_type AS ENUM (
    'UNKNOWN_ENV',
    'ON_PREM',
    'CLOUD',
    'HYBRID'
);

-- entity_state enum, which includes all possible states for entity
DROP TYPE IF EXISTS entity_state;
CREATE TYPE entity_state AS ENUM (
    'POWERED_ON',
    'POWERED_OFF',
    'SUSPENDED',
    'MAINTENANCE',
    'FAILOVER',
    'UNKNOWN'
);

-- entity severity enum, which includes all possible severities for entity
DROP TYPE IF EXISTS entity_severity;
CREATE TYPE entity_severity AS ENUM (
    'NORMAL',
    'MINOR',
    'MAJOR',
    'CRITICAL'
);

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
    -- entity (or group) severity
    "severity" entity_severity NULL,
    -- number of actions for this entity (or group)
    "num_actions" int4 NULL,
    -- commodities, entity/group type-specific info, etc.
    "attrs" jsonb NULL
);

-- create index
CREATE INDEX idx_type ON search_entity USING btree (type);