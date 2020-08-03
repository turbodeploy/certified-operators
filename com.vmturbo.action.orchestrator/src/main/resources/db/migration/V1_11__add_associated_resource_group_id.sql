-- Add the associated resource group ID to market actions.
ALTER TABLE market_action ADD COLUMN associated_resource_group_id BIGINT DEFAULT NULL AFTER
associated_account_id;

-- Add the associated resource group ID to action history table to persist the resource group associated with
-- the action at the time it was executed.
ALTER TABLE action_history ADD COLUMN associated_resource_group_id BIGINT DEFAULT NULL AFTER associated_account_id;
