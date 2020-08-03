-- Drop unnecessary index.
DROP INDEX snapshot_time ON system_load;
-- Create a composite index on slice, property_type and snapshot_time for system_load
-- to reduce the time needed to get the system load info by cluster headroom plan.
CREATE INDEX slice_property_type_snapshot_time ON system_load(slice, property_type, snapshot_time);