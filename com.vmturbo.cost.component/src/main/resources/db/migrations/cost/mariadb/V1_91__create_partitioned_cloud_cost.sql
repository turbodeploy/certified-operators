
CREATE TABLE IF NOT EXISTS cloud_cost_hourly (
   sample_ms_utc         BIGINT            NOT NULL,
   scope_id              BIGINT            NOT NULL,
   tag_group_id          BIGINT            NOT NULL DEFAULT 0,
   billing_family_id     BIGINT            DEFAULT NULL,

   price_model           SMALLINT          NOT NULL,
   cost_category         SMALLINT          NOT NULL,
   provider_id           BIGINT            NOT NULL DEFAULT 0,
   provider_type         SMALLINT          DEFAULT NULL,

   commodity_type        SMALLINT          NOT NULL DEFAULT 2047,

   usage_amount          DOUBLE            NOT NULL,
   currency              SMALLINT          NOT NULL,
   cost                  DOUBLE            NOT NULL,

   PRIMARY KEY (sample_ms_utc, scope_id, tag_group_id, cost_category, price_model, provider_id, commodity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE (sample_ms_utc DIV 1000) (
     PARTITION start VALUES LESS THAN (0),
     PARTITION future VALUES LESS THAN MAXVALUE
);

CREATE TABLE IF NOT EXISTS cloud_cost_daily (
   sample_ms_utc         BIGINT            NOT NULL,
   scope_id              BIGINT            NOT NULL,
   tag_group_id          BIGINT            NOT NULL DEFAULT 0,
   billing_family_id     BIGINT            DEFAULT NULL,

   price_model           SMALLINT          NOT NULL,
   cost_category         SMALLINT          NOT NULL,
   provider_id           BIGINT            NOT NULL DEFAULT 0,
   provider_type         SMALLINT          DEFAULT NULL,

   commodity_type        SMALLINT          NOT NULL DEFAULT 2047,

   usage_amount          DOUBLE            NOT NULL,
   currency              SMALLINT          NOT NULL,
   cost                  DOUBLE            NOT NULL,

   PRIMARY KEY (sample_ms_utc, scope_id, tag_group_id, cost_category, price_model, provider_id, commodity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
PARTITION BY RANGE (sample_ms_utc DIV 1000) (
     PARTITION start VALUES LESS THAN (0),
     PARTITION future VALUES LESS THAN MAXVALUE
);