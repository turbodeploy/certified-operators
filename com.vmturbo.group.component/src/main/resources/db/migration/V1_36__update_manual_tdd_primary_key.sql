-- Manual TDD may have filters with many entity types
ALTER TABLE man_data_defs_dyn_connect_filters DROP PRIMARY KEY, ADD PRIMARY KEY(definition_id,entity_type);
