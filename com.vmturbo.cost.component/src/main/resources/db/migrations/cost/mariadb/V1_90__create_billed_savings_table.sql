-- Table to store savings daily stats if billed based savings feature flag is enabled.
CREATE TABLE IF NOT EXISTS billed_savings_by_day (
  entity_oid BIGINT(20) NOT NULL,
  stats_time DATETIME NOT NULL,
  stats_type INT NOT NULL,
  stats_value DOUBLE NOT NULL,
  samples int(11) NOT NULL,
  PRIMARY KEY (entity_oid, stats_time, stats_type),
  INDEX idx_billed_savings_by_day_stats_time (stats_time),
  CONSTRAINT fk_billed_savings_by_day_entity_oid FOREIGN KEY (entity_oid)
  REFERENCES entity_cloud_scope (entity_oid) ON DELETE NO ACTION ON UPDATE NO ACTION
);
