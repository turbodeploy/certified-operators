-- If a column is created with data type timestamp, NOT NULL, with no default value and it is
-- the first timestamp column of the table, mysql will automatically assigned the
-- DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP attributes for this column.
-- (https://dev.mysql.com/doc/refman/5.6/en/upgrading-from-previous-series.html)
-- The "on update" attribute can cause issues if the timestamp is updated unintentionally
-- when the record is updated. This upgrade step removed the "ON UPDATE CURRENT_TIMESTAMP" attribute
-- on all timestamp columns.

ALTER TABLE	account_to_reserved_instance_mapping
CHANGE snapshot_time
snapshot_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	aggregation_meta_data
CHANGE last_aggregated
last_aggregated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	entity_cloud_scope
CHANGE creation_time
creation_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	entity_cost
CHANGE created_time
created_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	entity_to_reserved_instance_mapping
CHANGE snapshot_time
snapshot_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	hist_entity_reserved_instance_mapping
CHANGE snapshot_time
snapshot_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	plan_entity_cost
CHANGE created_time
created_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	price_table
CHANGE last_update_time
last_update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE	reserved_instance_coverage_by_month
CHANGE snapshot_time
snapshot_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
