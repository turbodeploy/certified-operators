-- We no longer want to enforce name uniqueness at the database level, because different
-- groups may have the same name (e.g. two groups with different entity types)
ALTER TABLE grouping DROP INDEX name;

-- The type of entities in the group.
-- We need this because we are going to enforce name uniqueness by name and type at the application
-- level.
-- We explicitly specify the default value as an "illegal" entity type. If we don't do this, the
-- default value will be 0 - which is a "SWITCH" - and we won't be able to distinguish
ALTER TABLE grouping
ADD COLUMN entity_type INT NOT NULL DEFAULT -1 AFTER type;
