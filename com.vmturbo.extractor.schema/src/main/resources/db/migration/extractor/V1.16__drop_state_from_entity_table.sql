-- entity state is now tracked with historical values in historical_entity_attrs, so we'll drop
-- it out of entity table

ALTER TABLE entity DROP COLUMN state;
