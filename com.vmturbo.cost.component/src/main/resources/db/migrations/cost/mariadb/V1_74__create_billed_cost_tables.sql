-- Create cost_tag table. This table stores tags discovered from billing reports of cloud providers. For maximum
-- generality we support collation allowing case sensitivity and trailing spaces for both keys and values as at least
-- one cloud provider allows these properties.
CREATE TABLE IF NOT EXISTS cost_tag (
   tag_key           VARCHAR(255)      NOT NULL,
   tag_value         VARCHAR(255)      NOT NULL,
   tag_id            BIGINT            NOT NULL AUTO_INCREMENT PRIMARY KEY,
   CONSTRAINT unique_constraint_key_value UNIQUE (tag_key, tag_value)
) CHARACTER SET utf8 COLLATE utf8_bin;

-- Create cost_tag_grouping table. This table stores tag groups discovered from billing reports. A tag group is a set of
-- tags associated with one or more cloud resources.
CREATE TABLE IF NOT EXISTS cost_tag_grouping (
   tag_group_id      BIGINT            NOT NULL,
   tag_id            BIGINT            NOT NULL,

   PRIMARY KEY (tag_group_id, tag_id),
   FOREIGN KEY (tag_id) REFERENCES cost_tag(tag_id)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION
);

-- Create billed_cost_hourly table. This table stores billed cost items for billing reports discovered at an hourly
-- granularity.
CREATE TABLE IF NOT EXISTS billed_cost_hourly (
   line_item_id          BIGINT            AUTO_INCREMENT PRIMARY KEY,
   sample_time           TIMESTAMP         NOT NULL DEFAULT current_timestamp,
   entity_id             BIGINT            NOT NULL DEFAULT 0,
   entity_type           SMALLINT          NOT NULL DEFAULT 2047,
   account_id            BIGINT            NOT NULL DEFAULT 0,
   region_id             BIGINT            NOT NULL DEFAULT 0,
   cloud_service_id      BIGINT            NOT NULL DEFAULT 0,
   service_provider_id   BIGINT            NOT NULL,
   tag_group_id          BIGINT            NOT NULL DEFAULT 0,
   price_model           SMALLINT          NOT NULL,
   cost_category         SMALLINT          NOT NULL,
   provider_id           BIGINT            NOT NULL DEFAULT 0,
   provider_type         SMALLINT          NOT NULL DEFAULT 2047,
   commodity_type        SMALLINT          NOT NULL DEFAULT 2047,
   usage_amount          DOUBLE            NOT NULL,
   unit                  SMALLINT          NOT NULL,
   currency              SMALLINT          NOT NULL,
   cost                  DOUBLE            NOT NULL,

   CONSTRAINT unique_constraint_billing_item UNIQUE (
      sample_time, entity_id, entity_type, account_id,
      region_id, cloud_service_id, service_provider_id, tag_group_id,
      price_model, cost_category, commodity_type)
);

CREATE INDEX idx_billed_cost_hourly_entity_id ON billed_cost_hourly(entity_id);
CREATE INDEX idx_billed_cost_hourly_account_id ON billed_cost_hourly(account_id);
CREATE INDEX idx_billed_cost_hourly_region_id ON billed_cost_hourly(region_id);

-- Create billed_cost_daily table. This table stores billed cost items for billing reports discovered at a daily
-- granularity. This table will also contain records that are periodically rolled up from the hourly table.
CREATE TABLE IF NOT EXISTS billed_cost_daily (
   line_item_id          BIGINT            AUTO_INCREMENT PRIMARY KEY,
   sample_time           TIMESTAMP         NOT NULL DEFAULT current_timestamp,
   entity_id             BIGINT            NOT NULL DEFAULT 0,
   entity_type           SMALLINT          NOT NULL DEFAULT 2047,
   account_id            BIGINT            NOT NULL DEFAULT 0,
   region_id             BIGINT            NOT NULL DEFAULT 0,
   cloud_service_id      BIGINT            NOT NULL DEFAULT 0,
   service_provider_id   BIGINT            NOT NULL,
   tag_group_id          BIGINT            NOT NULL DEFAULT 0,
   price_model           SMALLINT          NOT NULL,
   cost_category         SMALLINT          NOT NULL,
   provider_id           BIGINT            NOT NULL DEFAULT 0,
   provider_type         SMALLINT          NOT NULL DEFAULT 2047,
   commodity_type        SMALLINT          NOT NULL DEFAULT 2047,
   usage_amount          DOUBLE            NOT NULL,
   unit                  SMALLINT          NOT NULL,
   currency              SMALLINT          NOT NULL,
   cost                  DOUBLE            NOT NULL,

   CONSTRAINT unique_constraint_billing_item UNIQUE (
      sample_time, entity_id, entity_type, account_id,
      region_id, cloud_service_id, service_provider_id, tag_group_id,
      price_model, cost_category, commodity_type)
);

CREATE INDEX idx_billed_cost_daily_entity_id ON billed_cost_daily(entity_id);
CREATE INDEX idx_billed_cost_daily_account_id ON billed_cost_daily(account_id);
CREATE INDEX idx_billed_cost_daily_region_id ON billed_cost_daily(region_id);

-- Create billed_cost_monthly table. This table stores billed cost items for billing reports discovered at a monthly
-- granularity. This table will also contain records that are periodically rolled up from the daily table.
CREATE TABLE IF NOT EXISTS billed_cost_monthly (
   line_item_id          BIGINT            AUTO_INCREMENT PRIMARY KEY,
   sample_time           TIMESTAMP         NOT NULL DEFAULT current_timestamp,
   entity_id             BIGINT            NOT NULL DEFAULT 0,
   entity_type           SMALLINT          NOT NULL DEFAULT 2047,
   account_id            BIGINT            NOT NULL DEFAULT 0,
   region_id             BIGINT            NOT NULL DEFAULT 0,
   cloud_service_id      BIGINT            NOT NULL DEFAULT 0,
   service_provider_id   BIGINT            NOT NULL,
   tag_group_id          BIGINT            NOT NULL DEFAULT 0,
   price_model           SMALLINT          NOT NULL,
   cost_category         SMALLINT          NOT NULL,
   provider_id           BIGINT            NOT NULL DEFAULT 0,
   provider_type         SMALLINT          NOT NULL DEFAULT 2047,
   commodity_type        SMALLINT          NOT NULL DEFAULT 2047,
   usage_amount          DOUBLE            NOT NULL,
   unit                  SMALLINT          NOT NULL,
   currency              SMALLINT          NOT NULL,
   cost                  DOUBLE            NOT NULL,

   CONSTRAINT unique_constraint_billing_item UNIQUE (
      sample_time, entity_id, entity_type, account_id,
      region_id, cloud_service_id, service_provider_id, tag_group_id,
      price_model, cost_category, commodity_type)
);

CREATE INDEX idx_billed_cost_monthly_entity_id ON billed_cost_monthly(entity_id);
CREATE INDEX idx_billed_cost_monthly_account_id ON billed_cost_monthly(account_id);
CREATE INDEX idx_billed_cost_monthly_region_id ON billed_cost_monthly(region_id);