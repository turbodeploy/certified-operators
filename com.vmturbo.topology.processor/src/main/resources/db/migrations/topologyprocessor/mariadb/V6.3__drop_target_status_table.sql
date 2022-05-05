-- Drop target_status table. Were decided that we don't need to persist this information because it
-- is not critical, updated after every discovery. We should also be consistent with health details
-- that don't make sense to persist because, after the component restart, they are not actual.

DROP TABLE IF EXISTS target_status;