-- add index to `create_time` column for `action_history` table to speed up query

CREATE INDEX IF NOT EXISTS action_history_create_time_idx ON action_history(create_time);