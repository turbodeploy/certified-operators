-- This add the start and expiry columns to RI bought table.
-- This columns needs so we can more efficiently query for not expired RIs.
-- This also deletes the content of those tables. This should be fine as
-- their contents will be replaced with correct rows in the next broadcast cycle.

-- empty the content of tables
DELETE FROM reserved_instance_bought;
DELETE FROM entity_to_reserved_instance_mapping;

-- add new columns to tables
ALTER TABLE reserved_instance_bought ADD COLUMN start_time timestamp NOT NULL DEFAULT 0;
ALTER TABLE reserved_instance_bought ADD COLUMN expiry_time timestamp NOT NULL DEFAULT 0;