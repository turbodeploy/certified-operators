-- Add one count column to reserved instance bought table, it means the number of instances bought
-- of the reserved instance
ALTER TABLE reserved_instance_bought ADD COLUMN discovery_time TIMESTAMP NOT NULL;

-- Update the existing rows with the epoch second 0
update reserved_instance_bought set discovery_time=from_unixtime(1);