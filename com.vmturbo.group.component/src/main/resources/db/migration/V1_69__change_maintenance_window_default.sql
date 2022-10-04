-- Migration to change value of drsMaintenanceProtectionWindow setting in Host default policy to 30
SET @default_pm_policy_id = (SELECT id FROM setting_policy WHERE name = 'Host Defaults');
UPDATE setting_policy_setting
SET setting_value='30.0'
WHERE policy_id = @default_pm_policy_id
  AND setting_name = 'drsMaintenanceProtectionWindow'
  AND setting_value = 0;
