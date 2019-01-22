-- A management unit is an object that contains entities that the user wants to look at in an
-- aggregated way. For example, in on-prem environments a cluster is a typical management unit.
-- In cloud environments, a business account is a typical management unit.
--
-- We further divide the management units into subgroups, because we need to track actions on subsets
-- of a management unit. For example - VMs in a cluster.
CREATE TABLE mgmt_unit_subgroup (

    -- The ID of the management unit subgroup.
    -- This is completely local to the action orchestrator's action stats tables.
    id INTEGER NOT NULL AUTO_INCREMENT,

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
    UNIQUE KEY (mgmt_unit_type, mgmt_unit_id, environment_type, entity_type)
);

-- Each snapshot of all actions is bisected into action groups based on properties we want to
-- be able to count by.
CREATE TABLE action_group (

    -- The ID of the action group.
    -- This is completely local to the action orchestrator's action stats tables.
    id INTEGER NOT NULL AUTO_INCREMENT,

    -- The action type. See: ActionType in common.protobuf
    action_type SMALLINT NOT NULL,

    -- The category of the action. See: ActionCategory in common.protobuf.
    action_category SMALLINT NOT NULL,

    -- The mode of the action. See: ActionMode in common.protobuf.
    action_mode SMALLINT NOT NULL,

    -- The state of the action. See: ActionState in common.protobuf.
    action_state SMALLINT NOT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY (action_type, action_category, action_mode, action_state)
);

-- An action stat is an aggregation of actions in the system:
--    1. In a specific snapshot.
--         Action stats are recorded in "snapshots" whenever a new action plan comes in to the market.
--         All stats in a batch share the same snapshot time, which is roughly equivalent to the
--         time at which the action orchestrator receives the action plan.
--    2. Involving entities in a specific management unit subgroup.
--    3. In a specific action group.
CREATE TABLE action_stats_latest (
    -- -------------------
    -- Snapshot identification
    -- -------------------

    -- The time of the snapshot.
    action_snapshot_time TIMESTAMP NOT NULL,

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
CREATE TABLE action_snapshot_latest (
    -- The time of the snapshot. If there are any actions in this snapshot, the action_stats_latest
    -- table will rows with this snapshot_time.
    --
    -- If a particular action group or management unit has no rows with this snapshot_time in
    -- action_stats_latest, this means there were no actions in that scope in this action snapshot.
    action_snapshot_time TIMESTAMP NOT NULL,

    -- The ID of the topology, primarily for debugging purposes.
    topology_id BIGINT NOT NULL,

    -- The time the snapshot was recorded. Primarily for debugging purposes.
    snapshot_recording_time TIMESTAMP NOT NULL,

    -- The number of actions. Primarily for debugging purposes.
    actions_count INTEGER NOT NULL,

    PRIMARY KEY (action_snapshot_time)
);

-- -------------
-- HOURLY ROLLUP
-- -------------

CREATE TABLE action_stats_by_hour (
    -- The "hour" timestamp.
    hour_time TIMESTAMP NOT NULL,

    mgmt_unit_subgroup_id INTEGER NOT NULL,

    action_group_id INTEGER NOT NULL,

    -- To get the total for any of the stats, multiply the avg by the number of snapshots
    -- in the associated action_snapshot_hour record.

    avg_action_count DECIMAL(20, 7) NOT NULL,
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
CREATE TABLE action_snapshot_hour (
    -- The "hour" timestamp.
    -- There should be an action_snapshot_hour row for each unique "hour" timestamp in
    -- action_stats_by_hour.
    hour_time TIMESTAMP NOT NULL,

    -- The number of action snapshots rolled up into this hour.
    -- This is essentially the number of action_snapshot_latest entries that started with
    -- the hour represented by hour_time.
    num_action_snapshots INTEGER NOT NULL,

    hour_rollup_time TIMESTAMP NOT NULL,

    PRIMARY KEY (hour_time)
);

-- -------------
-- DAILY ROLLUP
-- -------------

CREATE TABLE action_stats_by_day (
    -- The "day" timestamp.
    day_time TIMESTAMP NOT NULL,

    mgmt_unit_subgroup_id INTEGER NOT NULL,

    action_group_id INTEGER NOT NULL,

    -- To get the total for any of the stats, multiply the avg by the number of snapshots
    -- in the associated action_snapshot_day record.

    avg_action_count DECIMAL(20, 7) NOT NULL,
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
CREATE TABLE action_snapshot_day (
    -- The "day" timestamp.
    -- There should be an action_snapshot_day row for each unique "day" timestamp in
    -- action_stats_by_day.
    day_time TIMESTAMP NOT NULL,

    -- The total number of action snapshots rolled up into this day.
    -- This is essentially the number of action_snapshot_latest entries that started with
    -- the day represented by day_time.
    num_action_snapshots INTEGER NOT NULL,

    day_rollup_time TIMESTAMP NOT NULL,

    PRIMARY KEY (day_time)
);

-- -------------
-- MONTHLY ROLLUP
-- -------------

CREATE TABLE action_stats_by_month (
    -- The "month" timestamp.
    month_time TIMESTAMP NOT NULL,

    mgmt_unit_subgroup_id INTEGER NOT NULL,

    action_group_id INTEGER NOT NULL,

    -- To get the total for any of the stats, multiply the avg by the number of snapshots
    -- in the associated action_snapshot_month record.

    avg_action_count DECIMAL(20, 7) NOT NULL,
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
CREATE TABLE action_snapshot_month (
    -- The "month" timestamp.
    -- There should be an action_snapshot_month row for each unique "month" timestamp in
    -- action_stats_by_month.
    month_time TIMESTAMP NOT NULL,

    -- The total number of action snapshots rolled up into this month.
    -- This is essentially the number of action_snapshot_latest entries that started with
    -- the day represented by month_time.
    num_action_snapshots INTEGER NOT NULL,

    month_rollup_time TIMESTAMP NOT NULL,

    PRIMARY KEY (month_time)
);
