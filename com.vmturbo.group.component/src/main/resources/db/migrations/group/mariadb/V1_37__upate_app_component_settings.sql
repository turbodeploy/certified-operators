-- To change 4 setting values to 'RECOMMEND' for 'Application Component Defaults' policy.
UPDATE setting_policy_setting
SET setting_value = 'RECOMMEND'
WHERE policy_id = (SELECT id FROM setting_policy where entity_type=69 and policy_type='default')
AND setting_name IN ('resizeUpHeap','resizeDownHeap','provision','suspend');
