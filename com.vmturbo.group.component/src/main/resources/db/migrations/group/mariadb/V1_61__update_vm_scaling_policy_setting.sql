-- Migration to change value of coreSocketRatioMode setting to VCPUScalingUnits and changing the
-- base value of that setting from LEGACY to MHZ

UPDATE setting_policy_setting sps
SET sps.setting_value='MHZ', sps.setting_name='vcpuScalingUnits'
WHERE sps.setting_name='coreSocketRatioMode';

UPDATE settings sps
SET sps.setting_value='MHZ', sps.setting_name='vcpuScalingUnits'
WHERE sps.setting_name='coreSocketRatioMode';