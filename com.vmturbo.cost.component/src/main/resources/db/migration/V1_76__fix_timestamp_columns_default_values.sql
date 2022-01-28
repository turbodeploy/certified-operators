-- If a column is created with data type timestamp, NOT NULL, with no default value and it is
-- the first timestamp column of the table, MariaDB will automatically assign the
-- DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP attributes for this column.
-- (https://mariadb.com/kb/en/timestamp/#automatic-values)
-- The "on update" attribute can cause issues if the timestamp is updated unintentionally
-- when the record is updated. Also, the "on update" behavior can not be easily replicated to Postgres,
-- which leads to the two databases having different behavior. The "ON UPDATE CURRENT_TIMESTAMP" attribute
-- can be removed by specifying a DEFAULT value, in this case CURRENT_TIMESTAMP.

ALTER TABLE	ingested_live_topology
CHANGE creation_time
creation_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE hourly_account_commitment_coverage
CHANGE sample_time
sample_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE hourly_cloud_commitment_utilization
CHANGE sample_time
sample_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;
