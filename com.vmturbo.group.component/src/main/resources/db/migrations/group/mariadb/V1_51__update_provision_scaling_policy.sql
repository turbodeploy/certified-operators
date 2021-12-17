-- Migration to change any scaling policy setting with the value PROVISION to HORIZONTAL_SCALE

UPDATE setting_policy_setting sps
SET sps.setting_value = CASE WHEN sps.setting_value = 'PROVISION' THEN 'HORIZONTAL_SCALE' ELSE sps.setting_value END
WHERE sps.setting_name = 'scalingPolicy'
