-- Migration to change value of VCPUScalingUnits setting
-- base value of that setting from MHZ to SOCKETS

UPDATE setting_policy_setting sps
SET sps.setting_value='SOCKETS'
WHERE sps.setting_name='vcpuScalingUnits';

UPDATE settings sps
SET sps.setting_value='SOCKETS'
WHERE sps.setting_name='vcpuScalingUnits';