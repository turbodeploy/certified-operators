-- The type of action plan (See enum ActionPlanType defined in
-- com.vmturbo.common.protobuf/src/main/protobuf/action/ActionDTO.proto).
-- Default is market action.
ALTER TABLE action_plan ADD COLUMN action_plan_type SMALLINT DEFAULT 1;