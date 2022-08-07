-- Add foreign keys from entity_savings_by_day and entity_savings_by_month tables to reference the
-- entity_oid column in the entity_cloud_scope table so that the entries in the entity_cloud_scope
-- table will not be removed as long as there are savings data for the entities.
-- Make sure we don't have records in the savings table that don't have the correspond parent
-- records in the scope table before creating the foreign key.

DELETE FROM entity_savings_by_day WHERE entity_oid NOT IN (
    SELECT DISTINCT entity_oid FROM entity_cloud_scope
);

ALTER TABLE entity_savings_by_day
ADD CONSTRAINT fk_entity_savings_by_day_entity_oid
FOREIGN KEY (entity_oid)
REFERENCES entity_cloud_scope(entity_oid)
ON DELETE NO ACTION ON UPDATE NO ACTION;

DELETE FROM entity_savings_by_month WHERE entity_oid NOT IN (
    SELECT DISTINCT entity_oid FROM entity_cloud_scope
);

ALTER TABLE entity_savings_by_month
ADD CONSTRAINT fk_entity_savings_by_month_entity_oid
FOREIGN KEY (entity_oid)
REFERENCES entity_cloud_scope(entity_oid)
ON DELETE NO ACTION ON UPDATE NO ACTION;
