-- This tables stores oids of entities enter and exit maintenance mode.
DROP TABLE IF EXISTS entity_maintenance;
CREATE TABLE entity_maintenance (
  -- Oid of a entity.
  entity_oid               BIGINT                                    NOT NULL,

  -- The time when an entity exits maintenance mode.
  exit_time                TIMESTAMP                                 NULL,

  -- Use entity_oid as primary key.
  PRIMARY KEY (entity_oid)
);