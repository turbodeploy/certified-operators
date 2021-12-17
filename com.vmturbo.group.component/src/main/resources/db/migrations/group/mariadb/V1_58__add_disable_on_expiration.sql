-- Adds "delete upon associated schedule expiration" flags to the setting policy and schedule.

ALTER TABLE setting_policy ADD COLUMN delete_after_expiration BOOLEAN DEFAULT false;
ALTER TABLE schedule ADD COLUMN delete_after_expiration BOOLEAN DEFAULT false;

