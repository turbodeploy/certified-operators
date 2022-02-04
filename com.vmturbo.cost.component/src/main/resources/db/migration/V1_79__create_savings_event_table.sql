
CREATE TABLE IF NOT EXISTS entity_savings_events (
  -- unique id for each event record, PK and auto incremented.
  event_id BIGINT NOT NULL AUTO_INCREMENT,

  -- Time when the event happened, in millis since epoch.
  event_created BIGINT NOT NULL,

  -- VM/DB oid whose state we are tracking now, or have ever tracked before, even deleted ones.
  entity_oid BIGINT NOT NULL,

  -- Type of entity event to track. Action recommendations/executions, topology events
  -- (creation/deletion/powerToggles etc.). Other possible events are group membership changes.
  event_type INT NOT NULL,

  -- Entity type for which the action or topology event applies to.
  entity_type INT NOT NULL,

  -- Additional info ('' if not applicable) about event where applicable, stored as a JSON string.
  event_info TEXT NOT NULL,

  PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_entity_savings_events_event_created on entity_savings_events(event_created);
