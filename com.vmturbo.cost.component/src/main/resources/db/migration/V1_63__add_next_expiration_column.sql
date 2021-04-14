-- Add a column to track the time of the next action to expire.  If there are no active actions,
-- we use a date far into the future.
ALTER TABLE entity_savings_state ADD COLUMN next_expiration_time DATETIME NOT NULL;

-- Update the existing rows with a date far into the future to prevent expiration
-- of existing actions.  This will be updated after the first periodic savings
-- calculation.
update entity_savings_state set next_expiration_time='9999-12-31T23:59:59';
