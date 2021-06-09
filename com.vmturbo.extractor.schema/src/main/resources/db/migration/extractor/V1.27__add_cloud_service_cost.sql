

DROP TABLE IF EXISTS cloud_service_cost;
-- Stores cost of each cloud service (e.g Amazon EC2 service).
CREATE TABLE cloud_service_cost (
  -- Record creation time.
  time timestamptz NOT NULL,
  -- OID of the business account that the cloud service cost belongs to.
  account_oid bigint NOT NULL,
  -- OID of the cloud service for which cost is being tracked.
  cloud_service_oid bigint NOT NULL,
  -- $/hr cost for the cloud service.
  cost float4 NOT NULL,
  -- PK to make update work if duplicate key found during insert.
  PRIMARY KEY (time, account_oid, cloud_service_oid)
);

-- set up hypertable configuration
SELECT create_hypertable('cloud_service_cost', 'time', chunk_time_interval => INTERVAL '1 month');
SELECT add_retention_policy('cloud_service_cost', INTERVAL '12 months');

