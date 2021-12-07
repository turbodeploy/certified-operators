-- Migration to delete settings that are no longer supported in vcpu scaling options

DELETE from setting_policy_setting WHERE setting_name IN ('usedIncrement_VCPU_Unit', 'usedIncrement_VCPU_Unit');

DELETE from settings WHERE setting_name IN ('usedIncrement_VCPU_Unit', 'usedIncrement_VCPU_Unit');