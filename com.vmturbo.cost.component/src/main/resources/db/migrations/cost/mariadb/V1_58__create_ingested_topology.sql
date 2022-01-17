CREATE TABLE IF NOT EXISTS ingested_live_topology (
  topology_id BIGINT(20) NOT NULL,
  creation_time TIMESTAMP NOT NULL,

  PRIMARY KEY (topology_id)
);

CREATE INDEX idx_ingested_live_topology_creation_time ON ingested_live_topology(creation_time);