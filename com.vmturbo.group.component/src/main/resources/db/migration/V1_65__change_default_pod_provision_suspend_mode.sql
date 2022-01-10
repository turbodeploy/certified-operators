-- Update default container pod provision and suspend mode to RECOMMEND
UPDATE setting_policy_setting sps
SET sps.setting_value = 'RECOMMEND'
WHERE (sps.setting_name='provision' OR sps.setting_name='suspend')
  AND sps.policy_id = (
      SELECT id FROM setting_policy
      WHERE entity_type = 41 AND policy_type = 'default');
