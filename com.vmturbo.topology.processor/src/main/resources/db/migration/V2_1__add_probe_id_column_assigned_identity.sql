-- Associate probeId of the probe which discovered the entities.

ALTER TABLE assigned_identity ADD probe_id BIGINT NOT NULL;
