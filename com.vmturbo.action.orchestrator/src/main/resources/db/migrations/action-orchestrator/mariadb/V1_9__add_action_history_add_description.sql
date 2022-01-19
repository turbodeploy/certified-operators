-- Adding action_detail_data to action_history table to persist the action description
-- for accepted actions.
ALTER TABLE action_history ADD COLUMN action_detail_data BLOB DEFAULT NULL AFTER user_name;