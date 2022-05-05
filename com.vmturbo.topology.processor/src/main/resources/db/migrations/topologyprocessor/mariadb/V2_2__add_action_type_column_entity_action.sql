-- Add action type into entity_action table, 'move' will be used for controllable logic,
-- 'activate' will be used for suspend logic

ALTER TABLE entity_action ADD action_type ENUM('activate', 'move') NOT NULL;
