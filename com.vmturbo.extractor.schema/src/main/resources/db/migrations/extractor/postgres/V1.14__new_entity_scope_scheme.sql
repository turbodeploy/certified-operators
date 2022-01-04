-- ------------------------------------ENTITY TABLE-------------------------------------------
-- ------------------------------------ENTITY TABLE-------------------------------------------
-- ------------------------------------ENTITY TABLE-------------------------------------------

-- Rename old entity table to entity.old
ALTER TABLE IF EXISTS entity RENAME TO entity_old;

-- Create new entity table
CREATE TABLE entity (
  oid int8 primary key NOT NULL,
  type entity_type NOT NULL,
  name text NOT NULL,
  environment environment_type NULL,
  state entity_state NULL,
  -- entity type-specific info
  attrs jsonb null,
  -- topology timestamp when this entity first appeared
  first_seen timestamptz NOT NULL,
  -- topology timestamp where this entity last appeared
  -- this value is always correct - it is often several hours beyond the correct value, to
  -- accommodate the fact that this value is only updated periodically for entities that
  -- remain in the topology over several cycles
  last_seen timestamptz NOT NULL
);

-- entity table indexes
CREATE INDEX entity_type_idx ON entity USING btree (type);

-- ------------------------------------SCOPE TABLE-------------------------------------------
-- ------------------------------------SCOPE TABLE-------------------------------------------
-- ------------------------------------SCOPE TABLE-------------------------------------------

-- Rename old scope table to scope.old
ALTER TABLE IF EXISTS scope RENAME TO scope_old;

-- Create new scope table
CREATE TABLE scope (
  -- oid of entity whose scope is the subject of this record
  seed_oid int8 NOT NULL ,
  -- oid of an entity that is part of that scope
  scoped_oid int8 NOT NULL,
  -- entity type of scoped entity
  scoped_type entity_type NOT NULL,
  -- timestamp of topology in which this relationship first appeared (at all, or following gap)
  start timestamptz NOT NULL,
  -- timestamp of last topology in which this relationship appeared in this span, or
  -- 9999-12-31T23:59:59 if the current span is not yet known to have terminated
  finish timestamptz NOT NULL,
  -- multiple scoping relationships for same oids must have non-overlapping time spans
  -- due to the way these are updated, it suffices to ensure they have distinct start times
  PRIMARY KEY (seed_oid, scoped_oid, start)
);

-- scope table indexes
CREATE INDEX scope_seed_type_start ON scope USING btree (seed_oid, scoped_type, start);
