-- Truncate table.
TRUNCATE TABLE entity_savings_audit_events;

-- Don't have json_valid check on column, that could slow insertions for large environments.
ALTER TABLE entity_savings_audit_events
MODIFY COLUMN event_info mediumtext COLLATE utf8_unicode_ci NOT NULL;

-- Remove json_valid from savings state table as well.
ALTER TABLE entity_savings_state
MODIFY COLUMN entity_state mediumtext COLLATE utf8_unicode_ci DEFAULT NULL;
