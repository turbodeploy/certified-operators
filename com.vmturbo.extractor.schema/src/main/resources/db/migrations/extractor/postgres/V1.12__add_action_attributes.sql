-- Additional (often type-specific) info for the action.
-- For example, in resizes this will contain the commodity type and from/to amounts.
ALTER TABLE "pending_action" ADD COLUMN "attrs" jsonb NULL;
ALTER TABLE "completed_action" ADD COLUMN "attrs" jsonb NULL;
