-- Prior to this migration we kept action-related data in two separate tables - action spec and
-- action. Drop those tables. Since they are not used for historical data it is safe to drop
-- them without significant data loss - we will repopulate recommendations soon after upgrade.
DROP TABLE IF EXISTS "action";
DROP TABLE IF EXISTS "action_spec";

DROP TYPE IF EXISTS terminal_state;
-- These are the terminal action states.
CREATE TYPE terminal_state AS ENUM (
    -- The action has completed successfully.
    'SUCCEEDED',

    -- The action failed to complete.
    'FAILED'
);


-- These are the latest actions recommended by the market.
-- The table is rotated at a configurable interval (not necessarily every broadcast), and all
-- actions are replaced with the "latest" action recommendations.
--
-- This table does not include actions that are in progress.
CREATE TABLE "pending_action" (
  -- The time the action was recommended.
  "recommendation_time" timestamptz NOT NULL,

  -- oid of the action in the system.
  "action_oid" int8 NOT NULL,

  -- The type of the action.
  "type" action_type NOT NULL,

  -- The severity of the action.
  "severity" severity NOT NULL,

  -- The category of the action.
  "category" action_category NOT NULL,

  -- The ID of the target entity.
  "target_entity_id" int8 NOT NULL,

  -- The IDs of the involved entities (includes the target entity).
  "involved_entities" int8[] NOT NULL,

  -- The text description of the action.
  "description" text NOT NULL,

  -- Savings (or, if negative, investment) of the action.
  "savings" float8 NOT NULL
);

CREATE UNIQUE INDEX "pending_action_byOid" ON "pending_action" USING btree ("action_oid");
CREATE INDEX "pending_action_byInvolvedEntities" ON "pending_action" USING gin ("involved_entities");
CREATE INDEX "pending_action_byType" ON "pending_action" USING btree ("type");

-- A completed action is an action that completed execution, whether it succeeded or failed.
-- This table does not contain "in progress" or "queued" actions.
CREATE TABLE "completed_action" (
  -- The time the action was recommended.
  "recommendation_time" timestamptz NOT NULL,

  -- The time the action was accepted for execution.
  "acceptance_time" timestamptz NOT NULL,

  -- The time the action was completed.
  "completion_time" timestamptz NOT NULL,

  -- oid of the action in the system.
  "action_oid" int8 NOT NULL,

  -- The type of the action.
  "type" action_type NOT NULL,

  -- The severity of the action.
  "severity" severity NOT NULL,

  -- The category of the action.
  "category" action_category NOT NULL,

  -- The ID of the target entity.
  "target_entity_id" int8 NOT NULL,

  -- The IDs of the involved entities (includes the target entity).
  "involved_entities" int8[] NOT NULL,

  -- The text description of the action.
  "description" text NOT NULL,

  -- Savings (or, if negative, investment) of the action.
  "savings" float8 NOT NULL,

  -- The final state of the action.
  "final_state" terminal_state NOT NULL,

  -- Message associated with the final state.
  -- Should contain an error message if the action failed.
  "final_message" text NOT NULL
);

CREATE UNIQUE INDEX "completed_action_byOid" ON "completed_action" USING btree ("action_oid", "completion_time");
CREATE INDEX "completed_action_byInvolvedEntities" ON "completed_action" USING gin ("involved_entities");
CREATE INDEX "completed_action_byType" ON "completed_action" USING btree ("type");

-- Make the completed actions table a hyper-table for easier time-based chunk management.
-- This table is not expected to be nearly as big as the metric table, so we don't use compression
-- and leave the default chunk interval (7 days).
SELECT create_hypertable('completed_action', 'completion_time');
