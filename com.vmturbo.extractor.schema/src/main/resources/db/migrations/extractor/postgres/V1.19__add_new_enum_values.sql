-- these values showed up in the source types on which our db enum types are based, and they're
-- being added to the db types to keep in sync

ALTER TYPE entity_type ADD VALUE IF NOT EXISTS 'CLOUD_COMMITMENT';
ALTER TYPE entity_type ADD VALUE IF NOT EXISTS 'CONTAINER_PLATFORM_CLUSTER';
ALTER TYPE action_state ADD VALUE IF NOT EXISTS 'FAILING';

