-- Update default container pod provision and suspend mode to RECOMMEND
UPDATE setting_policy_setting
SET setting_value = 'RECOMMEND'
WHERE (setting_name='provision' OR setting_name='suspend')
  AND policy_id = (
      SELECT id FROM setting_policy
      WHERE entity_type = 41 AND policy_type = 'default');