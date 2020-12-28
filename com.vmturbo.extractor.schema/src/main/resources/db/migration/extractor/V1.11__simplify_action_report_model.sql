
-- Prior to this migration we kept action-related data in two separate tables - action spec and
-- action. Drop those tables. Since they are not used for historical data it is safe to drop
-- them without significant data loss - we will repopulate recommendations soon after upgrade.
DROP TABLE IF EXISTS "action";
DROP TABLE IF EXISTS "action_spec";

-- These are the latest actions recommended by the market.
-- The table is rotated at a configurable interval (not necessarily every broadcast), and all
-- actions are replaced with the "latest" action recommendations.
CREATE TABLE "pending_action" (
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
