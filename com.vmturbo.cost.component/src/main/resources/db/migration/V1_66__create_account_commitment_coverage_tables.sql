CREATE TABLE IF NOT EXISTS hourly_account_commitment_coverage (
   sample_time                TIMESTAMP       NOT NULL,
   account_id                 BIGINT          NOT NULL,
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   cloud_service_id           BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   covered_amount             DOUBLE          NOT NULL,
   coverage_capacity          DOUBLE          NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              TINYINT         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_time, account_id, region_id, cloud_service_id, service_provider_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_hourly_account_commitment_coverage_acct_id on hourly_account_commitment_coverage(account_id);
CREATE INDEX idx_hourly_account_commitment_coverage_region_id on hourly_account_commitment_coverage(region_id);
CREATE INDEX idx_hourly_account_commitment_coverage_svc_provider_id on hourly_account_commitment_coverage(service_provider_id);

CREATE TABLE IF NOT EXISTS daily_account_commitment_coverage (
   sample_date                DATE            NOT NULL,
   account_id                 BIGINT          NOT NULL,
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   cloud_service_id           BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   covered_amount             DOUBLE          NOT NULL,
   coverage_capacity          DOUBLE          NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              TINYINT         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_date, account_id, region_id, cloud_service_id, service_provider_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_daily_account_commitment_coverage_acct_id on daily_account_commitment_coverage(account_id);
CREATE INDEX idx_daily_account_commitment_coverage_region_id on daily_account_commitment_coverage(region_id);
CREATE INDEX idx_daily_account_commitment_coverage_svc_provider_id on daily_account_commitment_coverage(service_provider_id);

CREATE TABLE IF NOT EXISTS monthly_account_commitment_coverage (
   sample_date                DATE            NOT NULL,
   account_id                 BIGINT          NOT NULL,
   region_id                  BIGINT          NOT NULL DEFAULT 0,
   cloud_service_id           BIGINT          NOT NULL DEFAULT 0,
   service_provider_id        BIGINT          NOT NULL,
   covered_amount             DOUBLE          NOT NULL,
   coverage_capacity          DOUBLE          NOT NULL,
   -- Type will indicate coupons, spend, etc.
   coverage_type              TINYINT         NOT NULL,
   -- Subtype is applicable to only specific types e.g. spend, in which sub type
   -- represents the currency
   coverage_sub_type          INT             NOT NULL DEFAULT 0,

   PRIMARY KEY (sample_date, account_id, region_id, cloud_service_id, service_provider_id, coverage_type, coverage_sub_type)
);

CREATE INDEX idx_monthly_account_commitment_coverage_acct_id on monthly_account_commitment_coverage(account_id);
CREATE INDEX idx_monthly_account_commitment_coverage_region_id on monthly_account_commitment_coverage(region_id);
CREATE INDEX idx_monthly_account_commitment_coverage_svc_provider_id on monthly_account_commitment_coverage(service_provider_id);