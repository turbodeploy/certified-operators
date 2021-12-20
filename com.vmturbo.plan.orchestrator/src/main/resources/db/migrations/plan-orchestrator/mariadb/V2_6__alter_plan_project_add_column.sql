-- Add column 'type' in plan_project table to indicate the type of the plan project (e.g. HEADROOM)
ALTER TABLE plan_project ADD COLUMN type VARCHAR(20) DEFAULT NULL;