ALTER TABLE entity_to_reserved_instance_mapping DROP PRIMARY KEY;

ALTER TABLE entity_to_reserved_instance_mapping DROP COLUMN ri_source_coverage;

ALTER TABLE entity_to_reserved_instance_mapping ADD COLUMN ri_source_coverage ENUM('BILLING', 'SUPPLEMENTAL_COVERAGE_ALLOCATION') NOT NULL;

ALTER TABLE entity_to_reserved_instance_mapping ADD PRIMARY KEY (entity_id, reserved_instance_id, ri_source_coverage);
