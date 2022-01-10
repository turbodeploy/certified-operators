-- Here we introduce a new approach to tracking entity scopes.
--
-- Rather than keeping lists of scoped entities in the `entity` table, we'll have a new
-- `scope` table that keeps pair-wise scope assoations in individual rows. Each row will
-- apply to a specific time period based on the topologies in which the relationships are
-- found, but unlike with the prior scope lists, each time range will be free of gaps; if
-- an association disappears and then later reappears, a new record will be created to represent
-- the second span, and likewise for any furture spans.
--
-- This migration introduces the new table but does not remove any of the prior scope-related
-- schema elements; both will be supported during a transition period to allow queries to be
-- converted on their own schedule. A second migration will remove the obsolete schema elements
-- once all queries have been converted.

DROP TABLE IF EXISTS scope;
CREATE TABLE scope (
  -- oid of entity whose scope is the subject of this record
  entity_oid int8 NOT NULL ,
  -- oid of an entity that is part of that scope
  scoped_oid int8 NOT NULL,
  -- entity type of scoped entity
  scoped_type entity_type NOT NULL,
  -- timestamp of topology in which this relationship first appeared (at all, or following  gap)
  start timestamptz NOT NULL,
  -- timestamp of last topology in which this relationship appeared in this span, or
  -- 9999-12-31T23:59:59 if the current span is not yet known to have terminated
  finish timestamptz NOT NULL,
  -- multiple scoping releationships for same oids must have non-overlapping time spans
  -- due to the way these are updated, it suffices to ensure they have distinct start times
  PRIMARY KEY (entity_oid, scoped_oid, start)
);

-- make it easy to zero in on entities by type
CREATE INDEX scope_scoped_type ON scope(scoped_type);
