CREATE TABLE IF NOT EXISTS daily_cloud_commitment_utilization (
   sample_date                DATE            NOT NULL,
   cloud_commitment_id        BIGINT          NOT NULL,
   -- The purchasing account of the cloud commitment
   account_id                 BIGINT          NOT NULL,
   -- The region in which the cloud commitment was purchased
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   utilization_amount         DOUBLE           NOT NULL,
   commitment_capacity        DOUBLE           NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              TINYINT         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_date, cloud_commitment_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_daily_cloud_commitment_utilization_cc_id ON daily_cloud_commitment_utilization(cloud_commitment_id);
CREATE INDEX idx_daily_cloud_commitment_utilization_acct_id ON daily_cloud_commitment_utilization(account_id);
CREATE INDEX idx_daily_cloud_commitment_utilization_region_id ON daily_cloud_commitment_utilization(region_id);