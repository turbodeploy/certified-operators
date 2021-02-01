-- Altering types of used and capacity fields for RI from FLOAT to DOUBLE to increase precision.
ALTER TABLE account_to_reserved_instance_mapping
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE entity_to_reserved_instance_mapping
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE plan_projected_entity_to_reserved_instance_mapping
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE plan_projected_reserved_instance_coverage
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE plan_projected_reserved_instance_utilization
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_coverage_by_day
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_coverage_by_hour
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_coverage_by_month
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_coverage_latest
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_utilization_by_day
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_utilization_by_hour
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_utilization_by_month
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;

ALTER TABLE reserved_instance_utilization_latest
    MODIFY COLUMN total_coupons DOUBLE NOT NULL,
    MODIFY COLUMN used_coupons DOUBLE NOT NULL;
