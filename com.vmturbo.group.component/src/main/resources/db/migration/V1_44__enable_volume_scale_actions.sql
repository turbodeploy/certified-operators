-- Volume Scale actions were disabled by default for private preview.
-- This migration changes the default setting value to "true" to enable them.
UPDATE setting_policy_setting
SET setting_value = 'true'
WHERE setting_name = 'enableScaleActions' AND policy_id = (
    SELECT id FROM setting_policy
    WHERE entity_type = 60 AND policy_type = 'default' );