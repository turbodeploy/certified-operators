-- Migration to change value of VCPUScalingUnits setting
-- base value of that setting from MHZ to SOCKETS

DO $$
DECLARE default_vm_policy_id BIGINT;
DECLARE default_vm_policy_scaling_unit VARCHAR;
DECLARE default_vm_policy_used_increment_vcpu VARCHAR;
BEGIN
SELECT id INTO default_vm_policy_id FROM setting_policy WHERE name = 'Virtual Machine Defaults';

SELECT setting_value INTO default_vm_policy_scaling_unit FROM setting_policy_setting WHERE policy_id = default_vm_policy_id AND setting_name = 'vcpuScalingUnits';
SELECT setting_value INTO default_vm_policy_used_increment_vcpu FROM setting_policy_setting WHERE policy_id = default_vm_policy_id AND setting_name = 'usedIncrement_VCPU';
UPDATE setting_policy_setting
SET setting_value ='SOCKETS'
WHERE default_vm_policy_scaling_unit = 'MHZ'
  AND default_vm_policy_used_increment_vcpu = '1800.0'
  AND policy_id = default_vm_policy_id
  AND setting_name = 'vcpuScalingUnits';
INSERT INTO setting_policy_setting
SELECT default_vm_policy_id, 'vcpuScalingUnits', 10, 'MHZ'
    WHERE  default_vm_policy_scaling_unit IS NULL AND default_vm_policy_used_increment_vcpu != '1800.0';

-- select a temporary table that contains all the user vm policy ids and their setting values.
CREATE TEMPORARY TABLE vm_user_policies AS (
SELECT setting_policy.id, setting_policy_setting.setting_name, setting_policy_setting.setting_value
FROM setting_policy
LEFT JOIN setting_policy_setting
ON setting_policy.id = setting_policy_setting.policy_id
WHERE setting_policy.entity_type = 10 AND setting_policy.policy_type = 'user');

-- From the table get all the policies that has MHZ set up.
-- These polices are set up by the user to specify scaling option.
-- Among these policies, if they don't have a vcpuScalingUnits, they are set up in old UI.
CREATE TEMPORARY TABLE user_vm_policies_with_scalingUnits AS (SELECT DISTINCT id from vm_user_policies WHERE setting_name = 'vcpuScalingUnits');
CREATE TEMPORARY TABLE vm_need_mhz_setting AS (
	SELECT DISTINCT id FROM vm_user_policies WHERE setting_name = 'usedIncrement_VCPU'
		AND
	id NOT IN (SELECT DISTINCT id from user_vm_policies_with_scalingUnits)
);

-- We need to add vcpuScalingUnits = MHZ for the above policies, as the user set it into MHz
-- value on purpose, we want to keep it after migration.
INSERT INTO setting_policy_setting
SELECT id, 'vcpuScalingUnits', 10, 'MHZ'
FROM vm_need_mhz_setting;

END $$
