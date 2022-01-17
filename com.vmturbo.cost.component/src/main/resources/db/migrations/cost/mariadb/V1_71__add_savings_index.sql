-- Create indices on stats_time so that data deletion task can run faster.
CREATE INDEX idx_entity_savings_by_hour_stats_time ON entity_savings_by_hour(stats_time);
CREATE INDEX idx_entity_savings_by_day_stats_time ON entity_savings_by_day(stats_time);
CREATE INDEX idx_entity_savings_by_month_stats_time ON entity_savings_by_month(stats_time);

-- Clean up audit events table, now will have event_time index, and retention default
-- is reduced to 1 month from 1 year previously (too much data was being stored).
TRUNCATE TABLE entity_savings_audit_events;
CREATE INDEX idx_entity_savings_audit_events_event_time ON entity_savings_audit_events(event_time);
