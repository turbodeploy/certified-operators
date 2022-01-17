-- Add a column to flag a state as updated if it was change during the last processing period.
-- The data type is a TINYINT which is mapped to a boolean in Java.
ALTER TABLE entity_savings_state ADD COLUMN updated TINYINT(1) DEFAULT 0 NOT NULL AFTER entity_oid;

-- Records will be searched by the updated flag. Hence add an index for this column.
CREATE INDEX idx_updated ON entity_savings_state (updated);