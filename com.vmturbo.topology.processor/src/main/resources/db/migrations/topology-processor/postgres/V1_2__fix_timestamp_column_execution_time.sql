-- Specify default timestamp value to match the behavior of the equivalent MariaDB table.

ALTER TABLE recurrent_operations
ALTER COLUMN execution_time
SET DEFAULT CURRENT_TIMESTAMP;
