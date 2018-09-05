-- Add one count column to reserved instance bought table, it means the number of instances bought
-- of the reserved instance
ALTER TABLE reserved_instance_bought ADD COLUMN count INT NOT NULL;