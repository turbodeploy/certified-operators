-- Alter the location column in the buy RI demand table from availability zone to region_or_zone to
-- reflect storage of Azure demand.

ALTER TABLE compute_tier_type_hourly_by_week CHANGE COLUMN availability_zone region_or_zone_id BIGINT NOT NULL;