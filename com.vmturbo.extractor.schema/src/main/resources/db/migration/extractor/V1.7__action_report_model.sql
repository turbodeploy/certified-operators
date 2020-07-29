DROP TYPE IF EXISTS action_type;
CREATE TYPE action_type AS ENUM (
    -- An unknown action type
    'NONE',
    -- Start a service entity. May be recommended by the market as a move from nothing to something.
    'START',
    -- Move a service entity from one provider to another.
    'MOVE',
    -- Suspend a service entity. Suspended service entities may be subsequently restarted via a START command.
    'SUSPEND',
    -- Provision a new service entity.
    'PROVISION',
    -- Supply a service entity with a new configuration.
    'RECONFIGURE',
    -- Resize (either up or down) a service entity.
    'RESIZE',
    -- Activate a non-active service entity.
    'ACTIVATE',
    -- Deactivate an active service entity.
    'DEACTIVATE',
    -- Delete a wasted file associated with a Storage or StorageTier
    'DELETE',
    -- Buy a reserved instance of the specification.
    'BUY_RI',
    -- Scale a service entity. Currently this action type is used to represent Cloud Move actions
    -- for workloads when they are moved from one template to another. Market doesn't generate
    -- Scale actions but Action Orchestrator translates Cloud Moves to Scale actions.
    'SCALE',
    -- Reserved instance reallocation action (aka Accounting action). These actions are generated
    -- for VMs that are moved to reserved instances that became available because some other VMs were
    -- moved to different templates.
    'ALLOCATE'
);

-- We reuse the severity values from the search model, but rename them to be more generic.
ALTER TYPE entity_severity RENAME TO severity;

DROP TYPE IF EXISTS action_category;
CREATE TYPE action_category AS ENUM (
    -- Unknown
    'UNKNOWN',
    -- The action is required to ensure an entity has the resources it needs for optimal performance
    -- (e.g. sizing up in order to address overutilization).
    'PERFORMANCE_ASSURANCE',
    -- The action is required in order to improve the efficiency of the system - i.e. how well
    -- it utilizes resources (e.g. moving a VM to a less-utilized provider, or shutting down
    -- a PM if it's VMs can be moved to other hosts without sacrificing performance)
    'EFFICIENCY_IMPROVEMENT',
    -- This action is required to prevent future risks.
    'PREVENTION',
    -- This action is required in order to comply with a placement policy (e.g. a VM needs
    -- to be in a certain cluster).
    'COMPLIANCE'
);

DROP TYPE IF EXISTS action_state;
CREATE TYPE action_state AS ENUM (
    -- An action that is READY is one that has not yet been decided by the system.
    -- Changes in policy or capability that affect actions in the READY state will be applied to
    -- those actions and potentially cause them to be decided at that point.
    'READY',
    -- An action that has been CLEARED is one that was at one time recommended by the market but
    -- is now no longer. Actions in DISABLED mode are not immediately CLEARED. Instead, they
    -- stay in the READY state but are not shown in the UI or executed.
    'CLEARED',
    -- An action that has been rejected.
    'REJECTED',
    -- An action that has been accepted. An action can be accepted by turbo (manually by user or
    -- policy - AUTOMATIC mode) or by third-party orchestrator platform.
    'ACCEPTED',
    -- An accepted action that waits start of executing. Action can stays in queue because of,
    -- for example, the target associated with the action is busy processing
    -- many other actions and would be overwhelmed by sending additional requests.
    -- Action with execution window can be rolled back from QUEUED state if it wasn't executed during
    -- active period of execution window.
    'QUEUED',
    -- An action that is currently being executed.
    'IN_PROGRESS',
    -- A completed action that has completed successfully.
    'SUCCEEDED',
    -- A completed action that has completed but had errors.
    'FAILED',
    -- Actions in the PREP state are preparing to run. Depending on automation policies, this
    -- may involve invoking a workflow, or it may do nothing.
    'PRE_IN_PROGRESS',
    -- Actions in the POST state are cleaning up after execution. Depending on automation policies,
    -- this may involve invoking a workflow, or it may do nothing.
    -- Note: The POST state is entered regardless of whether the action succeeded or failed
    'POST_IN_PROGRESS'
);

-- Contains info about unique actions that appear in the system over time.
--
-- These are called "action spec"s because there may be multiple actions with the same spec
-- over a long period of time. For example, a MOVE of VM 1 from Host 2 to Host 3 may be recommended,
-- dropped, and then recommended again a day later. The "spec" of the action is the same, even
-- though the action is different.
DROP TABLE IF EXISTS "action_spec";
CREATE TABLE "action_spec" (
  -- The OID of the action spec, assigned by the action orchestrator.
  "spec_oid" int8 NOT NULL,

  -- The action spec table contains denormalized properties that may change without changing the
  -- spec id. We use the hash to track these changes.
  "hash" int8 NOT NULL,

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

  -- The timestamp where this action spec first appeared with this hash
  "first_seen" timestamptz NOT NULL,

  -- The timestamp where this action spec last appeared with this hash.
  -- this value is always correct - it may be several hours beyond the correct value, to
  -- accommodate the fact that this value is only updated periodically.
  "last_seen" timestamptz NOT NULL
);

CREATE UNIQUE INDEX "actionSpec_byOid" ON "action_spec" USING btree ("spec_oid", "hash");
CREATE INDEX "actionSpec_byInvolvedEntities" ON "action_spec" USING gin ("involved_entities");

DROP TABLE IF EXISTS "action";
CREATE TABLE "action" (
  -- The time that this action was recorded. This is not the time the action was FIRST recommended.
  "time" timestamptz NOT NULL,

  -- The state of the action.
  "state" action_state NOT NULL,

  -- oid of the actual action in the system.
  "action_oid" int8 NOT NULL,

  -- oid of the spec to which the metric applies.
  "action_spec_oid" int8 NOT NULL,

  -- hash of the spec to which the metric applies.
  "action_spec_hash" int8 NOT NULL,

  -- User who executed the action (if the action is executed).
  "user" text NULL
);

CREATE INDEX "action_time" ON "action" USING brin ("time");
CREATE INDEX "action_index" ON "action" USING btree ("action_spec_oid", "action_spec_hash", "state", "time" DESC);
