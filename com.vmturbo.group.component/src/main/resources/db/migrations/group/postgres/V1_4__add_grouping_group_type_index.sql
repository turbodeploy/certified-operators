-- Add `group_type` index which is used in dynamic group resolution
CREATE INDEX IF NOT EXISTS idx_grouping_type ON grouping (group_type);