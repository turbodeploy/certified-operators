-- Changing the scenario_spec column to scenario_info (in sync with renamed class)
ALTER TABLE scenario CHANGE COLUMN scenario_spec scenario_info BLOB NOT NULL