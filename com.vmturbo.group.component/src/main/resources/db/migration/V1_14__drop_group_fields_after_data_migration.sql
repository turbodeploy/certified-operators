-- This migration is a part of 1.12 migration.
-- This script remove redundant fields from grouping table after data migration (done in 1.13)

ALTER TABLE grouping DROP COLUMN entity_type;
ALTER TABLE grouping DROP COLUMN group_data;
ALTER TABLE grouping DROP COLUMN type;
