-- This script is adding special fields to tables storing discovered data to distinguish
-- whether the object has to be updated.

ALTER TABLE grouping ADD COLUMN hash BINARY(32) NULL;
ALTER TABLE setting_policy ADD COLUMN hash BINARY(32) NULL;
ALTER TABLE policy ADD COLUMN hash BINARY(32) NULL;
