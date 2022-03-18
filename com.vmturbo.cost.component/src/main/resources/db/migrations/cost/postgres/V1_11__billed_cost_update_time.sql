-- Time (of broadcast) when value (usage_amount or cost) values were changed.
ALTER TABLE billed_cost_daily ADD COLUMN last_updated BIGINT DEFAULT NULL;

-- Query by entity_id and last_updated field range.
CREATE INDEX idx_billed_cost_daily_eidlud ON billed_cost_daily(entity_id, last_updated);
