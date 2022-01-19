-- An action plan is a set of action recommendations produced by the market after it runs an analysis.
-- It is a collection of actions associated with a specific topology and topology context.
-- Action plans are immutable and their contents are replaced when a new plan arrives for the same
-- topology context.
DROP TABLE IF EXISTS action_plan;
CREATE TABLE action_plan (
    -- The id of the action plan
    id BIGINT NOT NULL,

    -- The id of the topology that the market analysed to produce this action plan.
    topology_id BIGINT NOT NULL,

    -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
    topology_context_id BIGINT NOT NULL,

    -- The time at which the action plan was created.
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The type of action plan (See enum ActionPlanType defined in
    -- com.vmturbo.common.protobuf/src/main/protobuf/action/ActionDTO.proto).
    -- Default is market action.
    action_plan_type SMALLINT DEFAULT 1,

    PRIMARY KEY (id)
);


-- A market action consists of the recommendation produced by a market analysis.
-- A market action is immutable.
-- It does not contain the mutable state associated with actually executing an action.
-- Market actions are grouped together into an action plan.
DROP TABLE IF EXISTS market_action;
CREATE TABLE market_action (
    -- The id of the action.
    id BIGINT NOT NULL,

    -- The id of the parent action plan.
    action_plan_id BIGINT NOT NULL,

    -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
    topology_context_id BIGINT NOT NULL,

    -- The actual recommendation made by the market.
    -- Changes are stored as raw protobuf written to disk.
    -- See com.vmturbo.common.protobuf/src/main/protobuf/action/ActionDTO.proto
    recommendation BYTEA NOT NULL,

    -- Adding description to market_action table to persist the action description when executing plans.
    description VARCHAR(510) DEFAULT NULL,

    -- Add the associated account ID to market actions.
    associated_account_id BIGINT DEFAULT NULL,

    -- Add the associated resource group ID to market actions.
    associated_resource_group_id BIGINT DEFAULT NULL,

    PRIMARY KEY (id),
    CONSTRAINT fk_action_plan_id FOREIGN KEY (action_plan_id) REFERENCES action_plan(id) ON DELETE CASCADE
);

-- Add an index to make finding market actions by their action plan or topology context
DROP INDEX IF EXISTS market_action_plan_id_idx;
CREATE INDEX market_action_plan_id_idx ON market_action (action_plan_id);

DROP INDEX IF EXISTS market_action_topology_context_id_idx;
CREATE INDEX market_action_topology_context_id_idx ON market_action (topology_context_id);


-- A action history consists of the executed com.vmturbo.action.orchestrator.action.Action.
-- A action history is immutable.
-- It does not contain ActionTranslation property in Action, since we don't need translation anymore.
-- Actions are grouped together into an action history.
DROP TABLE IF EXISTS action_history;
CREATE TABLE action_history (
    -- The id of the action.
    id BIGINT NOT NULL,

    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    update_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- The ID of the topology context. Used to differentiate, for example, between real and plan contexts.
    topology_context_id BIGINT NOT NULL,

    -- The actual recommendation made by the market.
    -- Changes are stored as raw protobuf written to disk.
    -- See com.vmturbo.common.protobuf/src/main/protobuf/action/ActionDTO.proto
    recommendation BYTEA NOT NULL,

    recommendation_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    action_decision BYTEA NOT NULL,

    execution_step BYTEA NOT NULL,

    current_state INT NOT NULL,

    user_name VARCHAR(255) NOT NULL,

    -- Adding action_detail_data to action_history table to persist the action description
    -- for accepted actions.
    action_detail_data BYTEA DEFAULT NULL,

    -- Add the associated account ID to action history table to persist the account associated with
    -- the action at the time it was executed.
    associated_account_id BIGINT DEFAULT NULL,

    -- Add the associated resource group ID to action history table to persist the resource group associated with
    -- the action at the time it was executed.
    associated_resource_group_id BIGINT DEFAULT NULL,

    recommendation_oid BIGINT,

    PRIMARY KEY (id)
);

-- Add an index to make finding market actions by their action plan or topology context
DROP INDEX IF EXISTS action_history_id_idx;
CREATE INDEX action_history_id_idx ON action_history (id);

DROP INDEX IF EXISTS action_history_topology_context_id_idx;
CREATE INDEX action_history_topology_context_id_idx ON action_history (topology_context_id);


-- the workflow table depends on workflow_oids, and so must be deleted first
DROP TABLE IF EXISTS workflow;
DROP TABLE IF EXISTS workflow_oid;

-- persist OIDs for discovered Workflow objects here. The primary key is 'id', 'target_id', and 'external_name'.
CREATE TABLE workflow_oid (

    -- unique ID for this Workflow
    id BIGINT NOT NULL,

    -- OID for the target from which this Workflow was discovered
    target_id BIGINT NOT NULL,

    -- The "external name" captured from the target - the name given by the user via the target UI
    external_name VARCHAR(255) NOT NULL,

    -- the unique ID for this Workflow
    PRIMARY KEY (id),

    -- the pair (target_id, external_name) must be unique
    UNIQUE (target_id, external_name)
);

DROP INDEX IF EXISTS workflow_target_id_idx;
CREATE INDEX workflow_target_id_idx ON workflow_oid (target_id);

DROP INDEX IF EXISTS workflow_target_id_and_external_name_idx;
CREATE INDEX workflow_target_id_and_external_name_idx ON workflow_oid (target_id, external_name);


-- a table to hold the 'workflowinfo' protobuf, indexed by the OID
CREATE TABLE workflow (
    -- the OID for the workflow
    id BIGINT NOT NULL,

    -- The binary form of the WorkflowInfo to be used (later) to extract the workflow parameters
    workflow_info BYTEA,

    -- When this Workflow was first discovered
    create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The most recent time this workflow was discovered
    last_update_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- the ID lives in the workflow_oids table; auto delete the workflow_param if the OID is deleted
    FOREIGN KEY (id) REFERENCES workflow_oid(id) ON DELETE CASCADE,

    -- ensure that the workflow row has a unique ID column
    PRIMARY KEY (id)
);


-- A management unit is an object that contains entities that the user wants to look at in an
-- aggregated way. For example, in on-prem environments a cluster is a typical management unit.
-- In cloud environments, a business account is a typical management unit.
--
-- We further divide the management units into subgroups, because we need to track actions on subsets
-- of a management unit. For example - VMs in a cluster.
DROP TABLE IF EXISTS mgmt_unit_subgroup;
CREATE TABLE mgmt_unit_subgroup (

    -- The ID of the management unit subgroup.
    -- This is completely local to the action orchestrator's action stats tables.
    id SERIAL,

    -- -------------------
    -- Management unit identification
    -- -------------------

    -- The type of management unit.
    mgmt_unit_type SMALLINT NOT NULL,

    -- The ID of the management unit.
    mgmt_unit_id BIGINT NOT NULL,

    -- -------------------
    -- Management unit sub-group identification
    -- -------------------

    -- The environment type (see: the EnvironmentType enum in common.protobuf).
    --
    -- The environment type is only relevant in "hybrid" management units - i.e. ones that
    -- have both on-prem and cloud entities.
    environment_type SMALLINT NOT NULL,

    -- The entity type for the actions in the action group.
    --
    -- A single management unit may have different entity types "in scope". For example, for clusters
    -- we may want information about actions on the hosts in the cluster, or about actions on the VMs
    -- in the cluster. Different management unit types will have different supported entity types.
    entity_type SMALLINT NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (mgmt_unit_type, mgmt_unit_id, environment_type, entity_type)
);


-- Each snapshot of all actions is bisected into action groups based on properties we want to
-- be able to count by.
DROP TABLE IF EXISTS action_group;
CREATE TABLE action_group (

    -- The ID of the action group.
    -- This is completely local to the action orchestrator's action stats tables.
    id SERIAL,

    -- The action type. See: ActionType in common.protobuf
    action_type SMALLINT NOT NULL,

    -- The category of the action. See: ActionCategory in common.protobuf.
    action_category SMALLINT NOT NULL,

    -- The mode of the action. See: ActionMode in common.protobuf.
    action_mode SMALLINT NOT NULL,

    -- The state of the action. See: ActionState in common.protobuf.
    action_state SMALLINT NOT NULL,

    -- This is used to group actions on.
    action_related_risk INTEGER DEFAULT 0,

    PRIMARY KEY (id),
    UNIQUE (action_type, action_category, action_mode, action_state, action_related_risk)
);


-- An action stat is an aggregation of actions in the system:
--    1. In a specific snapshot.
--         Action stats are recorded in "snapshots" whenever a new action plan comes in to the market.
--         All stats in a batch share the same snapshot time, which is roughly equivalent to the
--         time at which the action orchestrator receives the action plan.
--    2. Involving entities in a specific management unit subgroup.
--    3. In a specific action group.
DROP TABLE IF EXISTS action_stats_latest;
CREATE TABLE action_stats_latest (
    -- -------------------
    -- Snapshot identification
    -- -------------------

    -- The time of the snapshot.
    action_snapshot_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The management unit group specifying the entities involved.
    mgmt_unit_subgroup_id INTEGER NOT NULL,

    -- The action group specifying the kinds of actions being aggregated.
    action_group_id INTEGER NOT NULL,

    -- -------------------
    -- Stats for the (mgmt unit, action group) pair.
    -- -------------------

    -- The total number of actions of this particular action group in this particular
    -- management unit.
    total_action_count INTEGER NOT NULL,

    -- track the number of "new" actions for each market analysis to use in roll-up processing
    new_action_count INT DEFAULT 0,

    -- The total number of entities of the particular type affected by actions of this particular
    -- action group in this particular management unit.
    total_entity_count INTEGER NOT NULL,

    -- The total savings amount - in dollars - of the actions of this particular action group in
    -- this particular management unit. We keep savings distinct from investment so that we can
    -- render them separately.
    total_savings DECIMAL(20, 7) NOT NULL,

    -- The total investment amount - in dollars - of the actions of this particular action group in
    -- this particular management unit. We keep savings distinct from investment so that we can
    -- render them separately.
    total_investment DECIMAL(20, 7) NOT NULL,

    PRIMARY KEY (action_snapshot_time, mgmt_unit_subgroup_id, action_group_id),

    FOREIGN KEY (mgmt_unit_subgroup_id) REFERENCES mgmt_unit_subgroup(id),
    FOREIGN KEY (action_group_id) REFERENCES action_group(id)
);

-- Records about action snapshots stored by the action orchestrator. Each snapshot is associated
-- with an action plan coming from the market.
DROP TABLE IF EXISTS action_snapshot_latest;
CREATE TABLE action_snapshot_latest (
    -- The time of the snapshot. If there are any actions in this snapshot, the action_stats_latest
    -- table will rows with this snapshot_time.
    --
    -- If a particular action group or management unit has no rows with this snapshot_time in
    -- action_stats_latest, this means there were no actions in that scope in this action snapshot.
    action_snapshot_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The ID of the topology, primarily for debugging purposes.
    topology_id BIGINT NOT NULL,

    -- The time the snapshot was recorded. Primarily for debugging purposes.
    snapshot_recording_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    -- The number of actions. Primarily for debugging purposes.
    actions_count INTEGER NOT NULL,

    PRIMARY KEY (action_snapshot_time)
);


-- -------------
-- HOURLY ROLLUP
-- -------------

DROP TABLE IF EXISTS action_stats_by_hour;
CREATE TABLE action_stats_by_hour (
    -- The "hour" timestamp.
    hour_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    mgmt_unit_subgroup_id INTEGER NOT NULL,

    action_group_id INTEGER NOT NULL,

    -- To get the total for any of the stats, multiply the avg by the number of snapshots
    -- in the associated action_snapshot_hour record.
    avg_action_count DECIMAL(20, 7) NOT NULL,

    -- track the number of actions from the very first market analysis of the hour
    -- this is used in the roll-up from by_hour to by_day
    new_action_count INTEGER DEFAULT 0,
    prior_action_count INTEGER DEFAULT 0,
    min_action_count INTEGER NOT NULL,
    max_action_count INTEGER NOT NULL,

    avg_entity_count DECIMAL(20, 7) NOT NULL,
    min_entity_count INTEGER NOT NULL,
    max_entity_count INTEGER NOT NULL,

    avg_savings DECIMAL(20, 7) NOT NULL,
    min_savings DECIMAL(20, 7) NOT NULL,
    max_savings DECIMAL(20, 7) NOT NULL,

    avg_investment DECIMAL(20, 7) NOT NULL,
    min_investment DECIMAL(20, 7) NOT NULL,
    max_investment DECIMAL(20, 7) NOT NULL,

    PRIMARY KEY (hour_time, mgmt_unit_subgroup_id, action_group_id),

    FOREIGN KEY (mgmt_unit_subgroup_id) REFERENCES mgmt_unit_subgroup(id),
    FOREIGN KEY (action_group_id) REFERENCES action_group(id)
);

-- Information about the action snapshots rolled up into each hour.
DROP TABLE IF EXISTS action_snapshot_hour;
CREATE TABLE action_snapshot_hour (
    -- The "hour" timestamp.
    -- There should be an action_snapshot_hour row for each unique "hour" timestamp in
    -- action_stats_by_hour.
    hour_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The number of action snapshots rolled up into this hour.
    -- This is essentially the number of action_snapshot_latest entries that started with
    -- the hour represented by hour_time.
    num_action_snapshots INTEGER NOT NULL,

    hour_rollup_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    PRIMARY KEY (hour_time)
);


-- -------------
-- DAILY ROLLUP
-- -------------

DROP TABLE IF EXISTS action_stats_by_day;
CREATE TABLE action_stats_by_day (
    -- The "day" timestamp.
    day_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    mgmt_unit_subgroup_id INTEGER NOT NULL,

    action_group_id INTEGER NOT NULL,

    -- To get the total for any of the stats, multiply the avg by the number of snapshots
    -- in the associated action_snapshot_day record.

    avg_action_count DECIMAL(20, 7) NOT NULL,
    -- track the number of actions from the very first market analysis of the day
    -- this is used in the roll-up from by_day to by_month
    new_action_count INTEGER DEFAULT 0,
    prior_action_count INTEGER DEFAULT 0,
    min_action_count INTEGER NOT NULL,
    max_action_count INTEGER NOT NULL,

    avg_entity_count DECIMAL(20, 7) NOT NULL,
    min_entity_count INTEGER NOT NULL,
    max_entity_count INTEGER NOT NULL,

    avg_savings DECIMAL(20, 7) NOT NULL,
    min_savings DECIMAL(20, 7) NOT NULL,
    max_savings DECIMAL(20, 7) NOT NULL,

    avg_investment DECIMAL(20, 7) NOT NULL,
    min_investment DECIMAL(20, 7) NOT NULL,
    max_investment DECIMAL(20, 7) NOT NULL,

    PRIMARY KEY (day_time, mgmt_unit_subgroup_id, action_group_id),

    FOREIGN KEY (mgmt_unit_subgroup_id) REFERENCES mgmt_unit_subgroup(id),
    FOREIGN KEY (action_group_id) REFERENCES action_group(id)
);

-- Information about the action snapshots rolled up into each day.
DROP TABLE IF EXISTS action_snapshot_day;
CREATE TABLE action_snapshot_day (
    -- The "day" timestamp.
    -- There should be an action_snapshot_day row for each unique "day" timestamp in
    -- action_stats_by_day.
    day_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The total number of action snapshots rolled up into this day.
    -- This is essentially the number of action_snapshot_latest entries that started with
    -- the day represented by day_time.
    num_action_snapshots INTEGER NOT NULL,

    day_rollup_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    PRIMARY KEY (day_time)
);


-- -------------
-- MONTHLY ROLLUP
-- -------------

DROP TABLE IF EXISTS action_stats_by_month;
CREATE TABLE action_stats_by_month (
    -- The "month" timestamp.
    month_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    mgmt_unit_subgroup_id INTEGER NOT NULL,

    action_group_id INTEGER NOT NULL,

    -- To get the total for any of the stats, multiply the avg by the number of snapshots
    -- in the associated action_snapshot_month record.

    avg_action_count DECIMAL(20, 7) NOT NULL,

    -- Track the number of actions from the very first market analysis of the month
    -- this is used to return accurate action counts from monthly tables.
    new_action_count INTEGER DEFAULT 0,
    prior_action_count INTEGER DEFAULT 0,

    min_action_count INTEGER NOT NULL,
    max_action_count INTEGER NOT NULL,

    avg_entity_count DECIMAL(20, 7) NOT NULL,
    min_entity_count INTEGER NOT NULL,
    max_entity_count INTEGER NOT NULL,

    avg_savings DECIMAL(20, 7) NOT NULL,
    min_savings DECIMAL(20, 7) NOT NULL,
    max_savings DECIMAL(20, 7) NOT NULL,

    avg_investment DECIMAL(20, 7) NOT NULL,
    min_investment DECIMAL(20, 7) NOT NULL,
    max_investment DECIMAL(20, 7) NOT NULL,

    PRIMARY KEY (month_time, mgmt_unit_subgroup_id, action_group_id),

    FOREIGN KEY (mgmt_unit_subgroup_id) REFERENCES mgmt_unit_subgroup(id),
    FOREIGN KEY (action_group_id) REFERENCES action_group(id)
);

-- Information about the action snapshots rolled up into each month.
DROP TABLE IF EXISTS action_snapshot_month;
CREATE TABLE action_snapshot_month (
    -- The "month" timestamp.
    -- There should be an action_snapshot_month row for each unique "month" timestamp in
    -- action_stats_by_month.
    month_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- The total number of action snapshots rolled up into this month.
    -- This is essentially the number of action_snapshot_latest entries that started with
    -- the day represented by month_time.
    num_action_snapshots INTEGER NOT NULL,

    month_rollup_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',

    PRIMARY KEY (month_time)
);


-- Create tables for persisting accepted actions.
DROP TABLE IF EXISTS accepted_actions;
CREATE TABLE accepted_actions (
    recommendation_id BIGINT NOT NULL,
    accepted_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',
    latest_recommendation_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',
    accepted_by VARCHAR(255) NOT NULL,
    acceptor_type VARCHAR(255) NOT NULL,

    PRIMARY KEY (recommendation_id)
);

DROP TABLE IF EXISTS accepted_actions_policies;
CREATE TABLE accepted_actions_policies (
    recommendation_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,

    PRIMARY KEY (recommendation_id, policy_id),
    CONSTRAINT fk_accepted_actions_policies FOREIGN KEY (recommendation_id)
        REFERENCES accepted_actions(recommendation_id) ON DELETE CASCADE
);


-- Create tables for persisting rejected actions.
DROP TABLE IF EXISTS rejected_actions;
CREATE TABLE rejected_actions (
    recommendation_id BIGINT NOT NULL,
    rejected_time TIMESTAMP NOT NULL DEFAULT '0001-01-01 00:00:00',
    rejected_by VARCHAR(255) NOT NULL,
    rejector_type VARCHAR(255) NOT NULL,

    PRIMARY KEY (recommendation_id)
);

DROP TABLE IF EXISTS rejected_actions_policies;
CREATE TABLE rejected_actions_policies (
    recommendation_id BIGINT NOT NULL,
    policy_id BIGINT NOT NULL,

    PRIMARY KEY (recommendation_id, policy_id),
    CONSTRAINT fk_rejected_actions_policies FOREIGN KEY(recommendation_id)
        REFERENCES rejected_actions(recommendation_id) ON DELETE CASCADE
);


-- Create a separate table to hold strings for the risks related to the actions, to reduce the size
-- of the action_group table by repeating integers instead of strings.
-- (We expect to have a lot of identical values for action_related_risk, so repeating them inside
-- the action_group table could turn costly.)
-- The description is expected to be short (like "Underutilized VCPU", "VMem congestion", etc.)
DROP TABLE IF EXISTS related_risk_description;
CREATE TABLE related_risk_for_action (
    -- The ID of the risk related to the action
    -- This is completely local to the action orchestrator's action_group table.
    id SERIAL,

    -- We use the checksum to quickly determine if a particular set of risks already has
    -- an associated risk_id in the database.
    checksum CHAR(32) NOT NULL DEFAULT '',

    PRIMARY KEY (id),
    UNIQUE (checksum)
);


-- This table consolidates the existing recommendation_identity and
-- recommendation_identity_details tables into a single new recommendation_identity that uses
-- a hash computed from former action data to manage action identities.
-- The customer has run the migration procedure when they upgraded to 8.1.6.
DROP TABLE IF EXISTS recommendation_identity;
CREATE TABLE recommendation_identity (
    id BIGINT NOT NULL,
    action_hash BYTEA NOT NULL,
    PRIMARY KEY (id)
);


DROP TABLE IF EXISTS action_workflow_book_keeping;
CREATE TABLE action_workflow_book_keeping (
    -- the id of the action that needs to be sent to a workflow
    action_stable_id BIGINT NOT NULL,

    -- the id of the workflow that needs to receive this action
    workflow_id BIGINT NOT NULL,

    -- Milliseconds since epoch since the action identified by action_stable_id has not been
    -- recommended by the market. This timestamp is used for evaluating when an action should be
    -- considered cleared.
    -- NULL cleared_timestamp means that the most recent market cycle recommended the action.
    cleared_timestamp TIMESTAMP NULL DEFAULT NULL,

    -- the target entity of the action
    target_entity_id BIGINT NOT NULL,

    -- The name of the policy setting that the workflow was applied to on the target_entity_id.
    -- The longest setting at time of adding this column was 57 characters.
    setting_name varchar(255) NOT NULL,

    PRIMARY KEY (action_stable_id, workflow_id)
);


-- Create a new table to store risk descriptions associated with each risk.
-- There can be multiple rows with the same id and different description, because
-- a single action can have multiple risks.
DROP TABLE IF EXISTS related_risk_description;
CREATE TABLE related_risk_description (
    -- The id of the risk set.
    id INTEGER NOT NULL,

    -- The string describing the risk.
    risk_description VARCHAR(100) NOT NULL,

    FOREIGN KEY (id) REFERENCES related_risk_for_action(id) ON DELETE CASCADE,
    -- The risk_description comes first in the key because that's what we query for.
    PRIMARY KEY (risk_description, id)
);