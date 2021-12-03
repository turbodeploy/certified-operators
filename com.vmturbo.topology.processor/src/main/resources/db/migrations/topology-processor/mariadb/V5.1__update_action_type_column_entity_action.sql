-- Update action type in entity_action table,
-- 'move' will be used for controllable logic,
-- 'activate' will be used for suspend logic
-- 'resize' (on-prem actions scale actions) will be used for setting resizeable flag on entity
-- 'scale' (cloud resize actions) are identified as 'scale' actions by probe
ALTER TABLE entity_action
CHANGE action_type action_type enum('activate','move', 'resize', 'scale') NOT NULL;