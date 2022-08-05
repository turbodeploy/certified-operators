-- Add foreign keys from entity_savings_by_day and entity_savings_by_month tables to reference the
-- entity_oid column in the entity_cloud_scope table so that the entries in the entity_cloud_scope
-- table will not be removed as long as there are savings data for the entities.
-- Ensure all we don't have records in the savings table that don't have the correspond parent
-- record in the scope table before creating the foreign key.


DELETE FROM entity_savings_by_day WHERE entity_oid IN (
  SELECT DISTINCT entity_savings_by_day.entity_oid
  FROM entity_cloud_scope
  RIGHT JOIN entity_savings_by_day
  ON entity_cloud_scope.entity_oid = entity_savings_by_day.entity_oid
  WHERE entity_cloud_scope.entity_oid IS NULL
);

ALTER TABLE entity_savings_by_day
ADD FOREIGN KEY fk_entity_savings_by_day_entity_oid (entity_oid) REFERENCES entity_cloud_scope(entity_oid)
ON DELETE NO ACTION ON UPDATE NO ACTION;

DELETE FROM entity_savings_by_month WHERE entity_oid IN (
  SELECT DISTINCT entity_savings_by_month.entity_oid
  FROM entity_cloud_scope
  RIGHT JOIN entity_savings_by_month
  ON entity_cloud_scope.entity_oid = entity_savings_by_month.entity_oid
  WHERE entity_cloud_scope.entity_oid IS NULL
);

ALTER TABLE entity_savings_by_month
ADD FOREIGN KEY fk_entity_savings_by_month_entity_oid (entity_oid) REFERENCES entity_cloud_scope(entity_oid)
ON DELETE NO ACTION ON UPDATE NO ACTION;
