/*OM-53802*/
alter table reserved_instance_coverage_by_day MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
alter table reserved_instance_coverage_by_hour MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
alter table reserved_instance_coverage_by_hour MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
alter table reserved_instance_coverage_latest MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;

alter table reserved_instance_utilization_by_day MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
alter table reserved_instance_utilization_by_hour MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
alter table reserved_instance_utilization_by_month MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
alter table reserved_instance_utilization_latest MODIFY COLUMN snapshot_time TIMESTAMP NOT NULL DEFAULT 0;
