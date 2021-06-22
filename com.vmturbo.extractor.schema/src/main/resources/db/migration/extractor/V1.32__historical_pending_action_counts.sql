-- Stats about actions recommended by the market.
DROP TABLE IF EXISTS "pending_action_stats";
CREATE TABLE "pending_action_stats" (
    "time" timestamptz NOT NULL,

    -- Reference to a scope, which can be an entity or group. "0" for global environment.
    "scope_oid" int8 NOT NULL,

     -- The type of entities to which the stats belong within the scope.
     -- May be __NONE__ if the stats belong to the scope as a whole.
    "entity_type" entity_type NOT NULL,

     -- The environment type of entities to which the stats belong within the scope.
    "environment_type" environment_type NOT NULL,

     -- references action_group table
    "action_group" int4 NOT NULL,

    -- Total action count in the previous cycle/time range (at the time of the last record)
    "prior_action_count" int4 NOT NULL DEFAULT 0,

    -- Number of actions count cleared during this cycle (in the time between the last record's time and this record's time)
    "cleared_action_count" int4 NOT NULL DEFAULT 0,

    -- Number of new action counts generated during this cycle.
    "new_action_count" int4 NOT NULL DEFAULT 0,

    -- Number of entities involved in the actions.
    "involved_entity_count" int4 NOT NULL DEFAULT 0,

    -- Total savings for the actions.
    "savings" float4 NOT NULL DEFAULT 0,

    -- Total investments for the actions.
    "investments" float4 NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX "pas_uniq_key" ON "pending_action_stats" USING btree ("time", "scope_oid", "entity_type", "environment_type", "action_group" DESC);
CREATE INDEX "pas_ag" ON "pending_action_stats" USING btree ("action_group");

-- Make the completed actions table a hyper-table for easier time-based chunk management.
-- This table is not expected to be nearly as big as the metric table, so we don't use compression
-- and leave the default chunk interval (7 days).
SELECT create_hypertable('pending_action_stats', 'time');

-- remove retention policy if it exists
SELECT remove_retention_policy('pending_action_stats', if_exists => true);
-- add new retention policy, default to 12 months, and it only drop raw chunks, while keeping
-- data in the continuous aggregates
SELECT alter_job(
    -- add new policy which returns job id and use it as parameter of function alter_job_schedule
    add_retention_policy('pending_action_stats', INTERVAL '12 months'),
    -- set the drop_chunks background job to run every day (this is default interval)
    schedule_interval => INTERVAL '1 days',
    -- set the job to start from midnight of next day
    next_start => date_trunc('DAY', now()) + INTERVAL '1 days'
);

DROP TYPE IF EXISTS action_mode;
CREATE TYPE action_mode AS ENUM (
    -- The recommendation and execution of this action is disabled. The user
    -- credited with disabling a disabled action is the user that created the policy
    -- that assigned this mode to the action.
    'DISABLED',

    -- The recommendation will be shown to the user in the UI but cannot be executed
    -- by the system.
    'RECOMMEND',

    -- Indicates that the actions that have this mode must be approved in an external system before
    -- we will execute it (in addition to an execution windows required).
    -- EXTERNAL_APPROVAL is more conservative than MANUAL, as a result, this enum value must come
    -- before MANUAL so that the default enum comparison considers EXTERNAL_APPROVAL to be smaller.
    -- The number 4 is because protobuf requires a new integer for new enum values.
    'EXTERNAL_APPROVAL',

    -- The recommendation will be shown to the user in the UI and can be optionally
    -- executed or dismissed. The user credited with accepting or rejecting the action
    -- is the user that made the decision, not the user that set the policy.
    'MANUAL',

    -- The recommendation will be automatically executed by the system. The user credited
    -- with executing an automated action is the user that created the policy that
    -- assigned this mode to the action.
    'AUTOMATIC'
);

-- Action filtering criteria.
-- These are the characteristics of an action that are valuable to keep for the kinds of queries
-- and aggregations we want to run on historical pending action counts.
DROP TABLE IF EXISTS "action_group";
CREATE TABLE "action_group" (
    "id" int4 NOT NULL,

    -- The type of the actions (e.g. MOVE, RESIZE) in this group.
    "type" action_type NOT NULL,

    -- The category of the actions (e.g. PERFORMANCE_IMPROVEMENT) in this group.
    "category" action_category NOT NULL,

    -- The state of the actions in this group.
    "state" action_state NOT NULL,

    -- The mode of the actions in this group.
    "mode" action_mode NOT NULL,

    -- The collection of risks in the system that the actions in this group address.
    -- Example: "Improve infrastructure efficiency", "VCPU Congestion".
    "risks" text[] NOT NULL
);

CREATE UNIQUE INDEX "ag_by_id" ON "action_group" USING btree ("id");
