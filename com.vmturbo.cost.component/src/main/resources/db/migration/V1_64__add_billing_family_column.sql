-- add column to store billing family OID
ALTER TABLE entity_cloud_scope ADD COLUMN billing_family_oid bigint(20);

-- add search index for billing_family_oid column
CREATE INDEX idx_entity_cloud_scope_billing_family_oid ON entity_cloud_scope (billing_family_oid);

-- Set the updated flag of the states to 1 so their billing family can be determined on the first
-- run of the savings process after the pod starts up.
UPDATE entity_savings_state SET updated = 1;
