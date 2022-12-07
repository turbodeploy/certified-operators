-- Mapping between oid references in Billing data generated via EntityIdentifyingPropertyValues (alias_id) and the
-- entity oid from an actual topology (real_id).
-- Oids from these two sources are different (even though they refer to the same entity) because they're generated from
-- different entity identifying properties. Notable examples of this scenario are Azure Virtual Machines and Databases.
CREATE TABLE IF NOT EXISTS oid_mapping (
   real_id                          BIGINT      NOT NULL,
   alias_id                         BIGINT      NOT NULL,
   -- first_discovered_time_ms_utc has 2 use cases:
   -- 1. While replacing scope_ids for billing data, it will be used to calculate the inclusive start interval for
   -- records to be updated. If there are multiple real_id mappings for a given alias_id, first_discovered_time_ms_utc
   -- of the later mapping will be used as the exclusive end interval for the earlier mapping.
   -- 2. When there are multiple records with the same alias_id, it will be used to determine the record to use for
   -- extracting the real_id for billing data based on the sample time.
   first_discovered_time_ms_utc     BIGINT      NOT NULL,
   PRIMARY KEY (real_id, alias_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

-- Completed replacements by the real_ids in cost datasets. A record is added to this table only when all
-- the relevant dataset records have been updated with the real_id. Once recorded here, the real_id must be used for all
-- subsequent upserts to the datasets whenever a sample with the associated alias_id is received if it falls within the
-- relevant time interval.
CREATE TABLE IF NOT EXISTS scope_id_replacement_log (
   real_id                          BIGINT      NOT NULL,
   -- identifier of the log that tracks scope_id replacements for a specific cost dataset.
   replacement_log_id               SMALLINT    NOT NULL,
   PRIMARY KEY (real_id, replacement_log_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;