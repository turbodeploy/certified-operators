-- add column to store entity types
ALTER TABLE entity_cloud_scope ADD COLUMN entity_type int(11) AFTER entity_oid;

-- add column to store resource group OID
ALTER TABLE entity_cloud_scope ADD COLUMN resource_group_oid bigint(20) AFTER service_provider_oid;

-- add search index for resource_group_oid column
CREATE INDEX idx_entity_cloud_scope_resource_group_oid ON entity_cloud_scope (resource_group_oid);

-- All entities in the entity_cloud_scope table are VMs at this point. Set entity type of all records
-- to 10 which is the entity type number for VMs.
UPDATE entity_cloud_scope SET entity_type=10;

-- Now that all records have a value for entity_type, make the entity_type column NOT NULL.
ALTER TABLE entity_cloud_scope MODIFY COLUMN entity_type int(11) NOT NULL;

-- We will add a foreign key constraint from the entity_savings_state table to the entity_cloud_scope
-- table. We need to clear the entity_savings_state table because there can be entities in the
-- entity_savings_state table that don't have the corresponding record in the  entity_cloud_scope
-- table.
TRUNCATE TABLE entity_savings_state;

-- Add the foreign key constraint from the entity_savings_state to entity_cloud_scope table.
ALTER TABLE entity_savings_state ADD CONSTRAINT `fk_entity_savings_state_entity_oid` FOREIGN KEY (`entity_oid`)
REFERENCES `entity_cloud_scope` (`entity_oid`)
ON DELETE NO ACTION
ON UPDATE NO ACTION;
