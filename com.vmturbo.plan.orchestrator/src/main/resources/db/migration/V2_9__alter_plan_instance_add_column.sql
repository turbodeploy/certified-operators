-- Add column 'type' in plan_instance table to indicate the type of the project type
ALTER TABLE plan_instance ADD COLUMN type VARCHAR(20) DEFAULT NULL;

-- Add column 'status' in plan_instance table to indicate the status of the plan instance
ALTER TABLE plan_instance ADD COLUMN status VARCHAR(30) DEFAULT NULL;
