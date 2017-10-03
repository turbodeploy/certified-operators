RENAME TABLE setting_policies TO setting_policy;

ALTER TABLE setting_policy ADD INDEX ix_setting_policy_name (name);