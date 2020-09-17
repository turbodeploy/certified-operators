-- This migration is logically part of V1.8, but flyway can't execute because the mixture of
-- transactional (the table creation) and non-transactional (adding a value to an enum) statements
-- in the same migraton.
--
-- We a `_NONE_` entity type, for use in the special record used in the `scope` table to record
-- the timestamp of the last processed topology (for use during recovery after a restart). That
-- record will always have `entity_oid` = `scoped_oid` = 0.
ALTER TYPE entity_type ADD VALUE IF NOT EXISTS '_NONE_';
