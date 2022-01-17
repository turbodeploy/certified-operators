-- Tables related to Cloud savings/investments feature.

-- Hourly stats table for entity savings.
CREATE TABLE entity_savings_stats_hourly (
  entity_id BIGINT(20) NOT NULL,
  stats_time DATETIME NOT NULL,
  stats_type INT NOT NULL,
  stats_value DECIMAL(20, 7) NOT NULL,
  PRIMARY KEY (entity_id, stats_time, stats_type)
);
