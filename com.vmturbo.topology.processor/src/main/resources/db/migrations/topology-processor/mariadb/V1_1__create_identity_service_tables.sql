-- This table contains the properties that are associated with an id assigned to an object by the
-- topology processor.
--
-- The main purpose is to persist assigned identities across restarts, and the structure of the
-- the table reflects that. If we need to query assigned identities directly we will need
-- to revisit this.
--
-- For more information on object identity see:
-- https://vmturbo.atlassian.net/wiki/spaces/Home/pages/78479578/Object+Identity
CREATE TABLE assigned_identity (
  -- The ID assigned to an entity identified by a particular set of identifying and heuristic
  -- properties.
  id                   BIGINT        NOT NULL,

  -- A JSON object describing the properties associated with this ID.
  -- Using text so that we can go directly to the database when debugging. We can switch to
  -- a binary blob if text leads to performance issues.
  --
  -- The 65,000 characters allowed by text should be sufficient (instead of LONGTEXT). Most probes
  -- use only a few properties for object identities, and the values for each property are likely
  -- to be relatively small.
  properties           TEXT          NOT NULL,

  PRIMARY KEY (id)
);
