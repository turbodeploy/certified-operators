-- Changes default setting value from IGNORE to LEGACY
UPDATE setting_policy_setting sps SET sps.setting_value = 'LEGACY' WHERE
sps.setting_name = 'coreSocketRatioMode'
AND sps.setting_value = 'IGNORE';
