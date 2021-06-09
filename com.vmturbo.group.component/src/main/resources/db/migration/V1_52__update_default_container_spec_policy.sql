-- Migration to change resizeVcpuLimitMinThreshod constraint value for ContainerSpec Default policy settings
-- from 10 to 500.

UPDATE setting_policy_setting sps
SET sps.setting_value = CASE WHEN sps.setting_value = 10.0 THEN 500.0 ELSE sps.setting_value END
WHERE sps.setting_name = 'resizeVcpuLimitMinThreshold'
  AND sps.policy_id = (SELECT id FROM setting_policy WHERE name = 'Container Spec Defaults');

-- Migration to change resizeVcpuLimitBelowMinThreshold for ContainerSpec Default policy settings
-- from RECOMMEND to DISABLED.

UPDATE setting_policy_setting sps
SET sps.setting_value = CASE WHEN sps.setting_value = 'RECOMMEND' THEN 'DISABLED' ELSE sps.setting_value END
WHERE sps.setting_name = 'resizeVcpuLimitBelowMinThreshold'
  AND sps.policy_id = (SELECT id FROM setting_policy WHERE name = 'Container Spec Defaults');
