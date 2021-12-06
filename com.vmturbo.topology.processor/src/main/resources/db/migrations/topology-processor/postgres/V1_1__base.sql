-- This table contains the properties that are associated with an id assigned to an object by the
-- topology processor.
--
-- The main purpose is to persist assigned identities across restarts, and the structure of the
-- the table reflects that. If we need to query assigned identities directly we will need
-- to revisit this.
--
-- For more information on object identity see:
-- https://vmturbo.atlassian.net/wiki/spaces/Home/pages/78479578/Object+Identity
CREATE TABLE assigned_identity (
  -- The ID assigned to an entity identified by a particular set of identifying and heuristic
  -- properties.
  id BIGINT NOT NULL,

  -- A JSON object describing the properties associated with this ID.
  -- Using text so that we can go directly to the database when debugging. We can switch to
  -- a binary blob if text leads to performance issues.
  --
  -- The 65,000 characters allowed by text should be sufficient (instead of LONGTEXT). Most probes
  -- use only a few properties for object identities, and the values for each property are likely
  -- to be relatively small.
  properties TEXT NOT NULL,

  -- Associate probeId of the probe which discovered the entities.
  probe_id BIGINT NOT NULL,

  -- Store the protobuf Enum numeric value of the CommodityDTO.EntityType
  -- instead of string as it is more space efficient.
  -- We store a default MAX_SIGNED_VALUE for the existing rows inorder
  -- to identify if the existing column has been migrated to the proper type or not.
  -- The protobuf EnumType is represented by int in java.
  entity_type INT NOT NULL DEFAULT '2147483647'::INT,

  -- for stale oid management
  expired BOOLEAN NOT NULL DEFAULT FALSE,
  last_seen TIMESTAMP NOT NULL DEFAULT now(),

  PRIMARY KEY (id)
);
CREATE INDEX last_seen ON assigned_identity(last_seen);


CREATE TYPE entity_action_status AS ENUM (
    'queued',
    'in progress',
    'succeed',
    'failed'
);

CREATE TYPE entity_action_action_type AS ENUM (
    'activate',
    'move',
    'resize',
    'scale'
);

-- This tables stores entity controllable flag information. When there is action sent to probe,
-- it will create records with 'queued' status, and when the action is being executed by porbe, it
-- will update its status to 'in progress' and when the action is executed successfully, it will update
-- its status to 'succeed', and when the action is failed, it will deleted all related records.
CREATE TABLE entity_action (
  -- Id of a action, it should be a Move action id.
  action_id BIGINT NOT NULL,

  -- Id of entity id, it should be a provider entity of above action Id.
  entity_id BIGINT NOT NULL,

  -- There are three status: 'queued', 'in progress', 'succeed'. 1: 'queued' means this action has
  -- been sent to probe, but not start executed yet. 2: 'in progress' means this action is being
  -- executed now by probe. 3: 'succeed' means this action is executed successfully. 4: 'failed' means
  -- this action execution is failed by probe.
  status entity_action_status NOT NULL,

  -- The time when last update happens.
  update_time TIMESTAMP NOT NULL,

  -- Add action type into entity_action table, 'move' will be used for controllable logic,
  -- 'activate' will be used for suspend logic
  action_type entity_action_action_type NOT NULL,

  -- Use action id and entity id as combination key.
  PRIMARY KEY (action_id, entity_id)
);


-- persist OIDs for TargetSpec objects here. The primary key is 'id', 'identity_matching_attributes'.
CREATE TABLE targetspec_oid (

  -- unique ID for this TargetSpec
  id BIGINT NOT NULL,

  -- identity matching attributes for the TargetSpec
  identity_matching_attributes TEXT NOT NULL,

  -- the unique ID for this TargetSpec
  PRIMARY KEY (id)
);


-- In this table we persist the information for historical utilization, i.e. the values for
-- used and peak for each commodity bought and sold in the previous cycle.
CREATE TABLE historical_utilization (

  -- Only one record will be persisted, the id is dummy.
  id            BIGINT        NOT NULL,

  -- All the info are persisted as 1 binary object.
  info          BYTEA         NOT NULL,

  -- The dummy id.
  PRIMARY KEY (id)
);


-- support for stale oids
CREATE TABLE recurrent_operations (
  execution_time TIMESTAMP NOT NULL,
  operation_name VARCHAR(255),
  expiration_successful BOOLEAN NOT NULL DEFAULT FALSE,
  last_seen_update_successful BOOLEAN NOT NULL DEFAULT FALSE,
  expired_records INT NOT NULL DEFAULT 0,
  updated_records INT NOT NULL DEFAULT 0,
  errors TEXT DEFAULT NULL,

  PRIMARY KEY(execution_time, operation_name)
);


-- This tables stores oids of entities enter and exit maintenance mode.
CREATE TABLE entity_maintenance (
  -- Oid of a entity.
  entity_oid               BIGINT                                    NOT NULL,

  -- The time when an entity exits maintenance mode.
  exit_time                TIMESTAMP                                 NULL,

  -- Use entity_oid as primary key.
  PRIMARY KEY (entity_oid)
);