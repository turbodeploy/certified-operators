-- This and V1.5.2 are part of the same migration, but they are split because Flyway requires
-- 'mixed' mode migration if both `ADD VALUE` and `RENAME VALUE` alterations appear in a single
-- migration, due to postgres restrictions on `ADD VALUE` in the context of a surrounding transaction.

-- This migration fills out the `entity_type` enum to cover all known entity types, not just those
-- needed by search api. This must happen before the changes made in V1.5.2, so that converion
-- of existing entity.type values from text to enum may fail

-- missing entity types
ALTER TYPE entity_type ADD VALUE 'ACTION_MANAGER';
ALTER TYPE entity_type ADD VALUE 'APPLICATION_SERVER';
ALTER TYPE entity_type ADD VALUE 'AVAILABILITY_ZONE';
ALTER TYPE entity_type ADD VALUE 'BUSINESS';
ALTER TYPE entity_type ADD VALUE 'BUSINESS_ENTITY';
ALTER TYPE entity_type ADD VALUE 'CLOUD_SERVICE';
ALTER TYPE entity_type ADD VALUE 'COMPUTE_RESOURCE';
ALTER TYPE entity_type ADD VALUE 'COMPUTE_TIER';
ALTER TYPE entity_type ADD VALUE 'DATABASE_SERVER_TIER';
ALTER TYPE entity_type ADD VALUE 'DATABASE_TIER';
ALTER TYPE entity_type ADD VALUE 'DESIRED_RESERVED_INSTANCE';
ALTER TYPE entity_type ADD VALUE 'DISTRIBUTED_VIRTUAL_PORTGROUP';
ALTER TYPE entity_type ADD VALUE 'DPOD';
ALTER TYPE entity_type ADD VALUE 'HCI_PHYSICAL_MACHINE';
ALTER TYPE entity_type ADD VALUE 'HYPERVISOR_SERVER';
ALTER TYPE entity_type ADD VALUE 'INFRASTRUCTURE';
ALTER TYPE entity_type ADD VALUE 'INTERNET';
ALTER TYPE entity_type ADD VALUE 'IP';
ALTER TYPE entity_type ADD VALUE 'LICENSING_SERVICE';
ALTER TYPE entity_type ADD VALUE 'LOAD_BALANCER';
ALTER TYPE entity_type ADD VALUE 'LOGICAL_POOL';
ALTER TYPE entity_type ADD VALUE 'MAC';
ALTER TYPE entity_type ADD VALUE 'MOVER';
ALTER TYPE entity_type ADD VALUE 'NETWORKING_ENDPOINT';
ALTER TYPE entity_type ADD VALUE 'OPERATOR';
ALTER TYPE entity_type ADD VALUE 'PORT';
ALTER TYPE entity_type ADD VALUE 'PROCESSOR_POOL';
ALTER TYPE entity_type ADD VALUE 'RESERVED_INSTANCE';
ALTER TYPE entity_type ADD VALUE 'RESERVED_INSTANCE_SPECIFICATION';
ALTER TYPE entity_type ADD VALUE 'RIGHT_SIZER';
ALTER TYPE entity_type ADD VALUE 'SAVINGS';
ALTER TYPE entity_type ADD VALUE 'SERVICE_ENTITY_TEMPLATE';
ALTER TYPE entity_type ADD VALUE 'SERVICE_PROVIDER';
ALTER TYPE entity_type ADD VALUE 'STORAGE_TIER';
ALTER TYPE entity_type ADD VALUE 'STORAGE_VOLUME';
ALTER TYPE entity_type ADD VALUE 'THIS_ENTITY';
ALTER TYPE entity_type ADD VALUE 'THREE_TIER_APPLICATION';
ALTER TYPE entity_type ADD VALUE 'VIRTUAL_APPLICATION';
ALTER TYPE entity_type ADD VALUE 'VLAN';
ALTER TYPE entity_type ADD VALUE 'VM_SPEC';
ALTER TYPE entity_type ADD VALUE 'VPOD';
ALTER TYPE entity_type ADD VALUE 'WEB_SERVER';
