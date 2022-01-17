-- Introduce new table “account_to_reserved_instance_mapping”
-- snapshot_time – the last time this mapping was updated
-- business_account_id – Account Id
-- reserved_instance_id – Reserved Instance OID
-- used_coupons – number of coupons used ( daily avg as sent by billing probe)
-- ri_source_coverage – SUPPLEMENTAL expected only for discovered account
-- Mirror of the entity_to_reserved_instance_mapping table for current usage at an account granularity for undiscovered accounts.
-- Stores current usage at an account level for the undiscovered accounts populated with the contents of the
-- AccountRICoverageUpload data sent by TP for undiscovered accounts. This will have one row per account per RI.

DROP TABLE IF EXISTS account_to_reserved_instance_mapping;

CREATE TABLE account_to_reserved_instance_mapping (
  snapshot_time         TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  business_account_oid  BIGINT(20) NOT NULL,
  reserved_instance_id  BIGINT(20) NOT NULL,
  used_coupons          FLOAT NOT NULL,
  ri_source_coverage    ENUM('BILLING','SUPPLEMENTAL_COVERAGE_ALLOCATION') COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (business_account_oid, reserved_instance_id, ri_source_coverage)
);

-- Introduce new table “hist_entity_reserved_instance_mapping”
-- snapshot_time – the last time this mapping was updated
-- entity_id – Entity Id
-- reserved_instance_id – Reserved Instance OID
-- used_coupons – number of coupons used ( daily avg as sent by billing probe)
-- start_usage_time  - the billing window start time
-- end_usage_time – the billing window end time
-- Stores all usage of discovered entities of any RI.
-- Populated from the data coming in from the EntityRICoverageUpload with the usage start and usage end.
-- Each row is identified by the entity id, RI id and the usage start. Each entity may have multiple historical
-- RI coverage periods (same or different RI) and each such record is uniquely determined by entity id, reserved
-- instance id and the start time of the coverage period.

DROP TABLE IF EXISTS hist_entity_reserved_instance_mapping;

CREATE TABLE hist_entity_reserved_instance_mapping (
  snapshot_time         TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  entity_id             BIGINT(20) NOT NULL,
  reserved_instance_id  BIGINT(20) NOT NULL,
  start_usage_time      TIMESTAMP NOT NULL,
  end_usage_time        TIMESTAMP NOT NULL,
  PRIMARY KEY (entity_id, reserved_instance_id, start_usage_time)
);

