-- The "BLOB" datatype can fit objects up to 64kb.
-- This is not always big enough for the scenario or the plan instance (which has a scenario
-- snapshot embedded inside it), so we migratethem to MEDIUMBLOB which goes up to 16MB.

ALTER TABLE scenario MODIFY scenario_info MEDIUMBLOB NOT NULL;

ALTER TABLE plan_instance MODIFY plan_instance MEDIUMBLOB NOT NULL
