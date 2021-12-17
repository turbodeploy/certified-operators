-- Migration to change DBMemUtilization constraint value for Database Server Default policy settings
-- from 80% to 100%.

UPDATE setting_policy_setting sps
SET sps.setting_value = CASE WHEN sps.setting_value = 80.0 THEN 100.0 ELSE sps.setting_value END
WHERE sps.setting_name = 'dbmemUtilization'
AND sps.policy_id = (SELECT id FROM setting_policy WHERE name = 'Database Server Defaults')
