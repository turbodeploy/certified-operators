ALTER TABLE setting_policy ADD COLUMN policy_type ENUM('user', 'default') NOT NULL;