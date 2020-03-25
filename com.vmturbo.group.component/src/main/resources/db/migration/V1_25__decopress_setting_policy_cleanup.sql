ALTER TABLE setting_policy DROP COLUMN setting_policy_data;
ALTER TABLE setting_policy MODIFY COLUMN enabled BOOLEAN NOT NULL;
ALTER TABLE setting_policy MODIFY COLUMN entity_type INT(11) NOT NULL;


