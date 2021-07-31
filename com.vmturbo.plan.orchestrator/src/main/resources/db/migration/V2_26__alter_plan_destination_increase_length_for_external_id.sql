-- Alter Plan Destination table external_id column to have longer length
ALTER TABLE plan_destination MODIFY COLUMN external_id VARCHAR(80) NOT NULL;