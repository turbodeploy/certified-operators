-- Create a separate table to hold strings for the risks related to the actions, to reduce the size
-- of the action_group table by repeating integers instead of strings.
-- (We expect to have a lot of identical values for action_related_risk, so repeating them inside
-- the action_group table could turn costly.)
-- The description is expected to be short (like "Underutilized VCPU", "VMem congestion", etc.)
CREATE TABLE related_risk_for_action (
    -- The ID of the risk related to the action
    -- This is completely local to the action orchestrator's action_group table.
    id INTEGER NOT NULL AUTO_INCREMENT,

    -- The string describing the risk related to the action
    risk_description VARCHAR(100) NOT NULL,

    PRIMARY KEY (id),
    UNIQUE KEY (risk_description)
);

-- Add a new column "action_risk", which is used to group actions on.
ALTER TABLE action_group ADD COLUMN action_related_risk INTEGER DEFAULT 0;

-- drop previous index and add action_risk to the unique key
ALTER TABLE action_group DROP INDEX action_type;
ALTER TABLE action_group ADD UNIQUE KEY action_group_key (action_type, action_category, action_mode,
 action_state, action_related_risk);