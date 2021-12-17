ALTER TABLE setting_policy CHANGE policy_type policy_type ENUM('user', 'default', 'discovered') NOT NULL;

-- target_id should only be present when policy_type is 'discovered'.
ALTER TABLE setting_policy ADD COLUMN target_id BIGINT;