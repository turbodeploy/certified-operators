-- Add column 'status' in plan_project table to indicate the status of the plan instance
ALTER TABLE plan_project ADD COLUMN status VARCHAR(30) DEFAULT 'UNAVAILABLE';
