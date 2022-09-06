
-- Create the cloud scope table
CREATE TABLE IF NOT EXISTS cloud_scope (
   scope_id               BIGINT          NOT NULL,
   -- provided as metadata, scope_id by itself is expected to be unique
   scope_type             SMALLINT         NOT NULL,
   resource_id            BIGINT           DEFAULT NULL,
   resource_type          SMALLINT         DEFAULT NULL,
   account_id             BIGINT           DEFAULT NULL,
   region_id              BIGINT           DEFAULT NULL,
   cloud_service_id       BIGINT           DEFAULT NULL,
   availability_zone_id   BIGINT           DEFAULT NULL,
   resource_group_id      BIGINT           DEFAULT NULL,
   -- regardless of the scope_type, service provider must always be set
   service_provider_id    BIGINT           NOT NULL,
   update_ts              TIMESTAMP        NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

   PRIMARY KEY (scope_id),

   INDEX cloud_scope_account_id_idx (account_id),
   INDEX cloud_scope_region_id_idx (region_id),
   INDEX cloud_scope_zone_id_idx (availability_zone_id),
   INDEX cloud_scope_rg_id_idx (resource_group_id)
)

PARTITION BY HASH(scope_id)
PARTITIONS 50;