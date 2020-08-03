-- Add the associated account ID to market actions.
ALTER TABLE market_action ADD COLUMN associated_account_id BIGINT DEFAULT NULL AFTER description;

-- Add the associated account ID to action history table to persist the account associated with
-- the action at the time it was executed.
ALTER TABLE action_history ADD COLUMN associated_account_id BIGINT DEFAULT NULL AFTER action_detail_data;
