-- Migration to change value of VCPUScalingUnits setting in VM default policy to SOCKETS.
-- We change this only if the current value in VM default policy is MHZ and 1800.

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
  AND policy_id = @default_vm_policy_id
  AND setting_name = 'vcpuScalingUnits' ;
END $$
