-- Migration to change value of VCPUScalingUnits setting
-- base value of that setting from MHZ to SOCKETS

UPDATE setting_policy_setting
SET setting_value='SOCKETS'
WHERE setting_name='vcpuScalingUnits';

UPDATE settings
SET setting_value='SOCKETS'
WHERE setting_name='vcpuScalingUnits';