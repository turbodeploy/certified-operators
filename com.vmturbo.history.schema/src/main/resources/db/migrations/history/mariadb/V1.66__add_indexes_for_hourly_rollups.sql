-- With OM-82749, we no longer create a fixed number (8) of tasks to upsert all the records
-- involved in rollups for each table in parallel. Instead, we now create a collection of tasks
-- that are based on the number of records being rolled up, and for very large topologies this
-- means we create a great many tasks that are individually much smaller for large tables like
-- vm_stats. This magnifies an existing performance issue, namely that selecting the records that
-- will participate in a given upsert operation for hourly rollups requires scanning all the
-- records with a given snapshot_time value. Here we replace the existing `snapshot_time` index by
-- an index on `snapshot_time` and the first 8 characters of `hour_key`. This index will still
-- support time bounds efficiently, but it is now also used for hourly rollup upserts, dramatically
-- speeding those operations.
--
-- The eight scans performed earlier were apparently not very significant, but the much greater
-- number of scans for large topologies are.

CREATE INDEX snapshot_time_hour_key ON app_component_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON app_component_stats_latest;
CREATE INDEX snapshot_time_hour_key ON app_server_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON app_server_stats_latest;
CREATE INDEX snapshot_time_hour_key ON app_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON app_stats_latest;
CREATE INDEX snapshot_time_hour_key ON bu_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON bu_stats_latest;
CREATE INDEX snapshot_time_hour_key ON business_app_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON business_app_stats_latest;
CREATE INDEX snapshot_time_hour_key ON business_transaction_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON business_transaction_stats_latest;
CREATE INDEX snapshot_time_hour_key ON ch_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON ch_stats_latest;
CREATE INDEX snapshot_time_hour_key ON cnt_spec_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON cnt_spec_stats_latest;
CREATE INDEX snapshot_time_hour_key ON cnt_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON cnt_stats_latest;
CREATE INDEX snapshot_time_hour_key ON container_cluster_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON container_cluster_stats_latest;
CREATE INDEX snapshot_time_hour_key ON cpod_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON cpod_stats_latest;
CREATE INDEX snapshot_time_hour_key ON da_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON da_stats_latest;
CREATE INDEX snapshot_time_hour_key ON db_server_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON db_server_stats_latest;
CREATE INDEX snapshot_time_hour_key ON db_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON db_stats_latest;
CREATE INDEX snapshot_time_hour_key ON desktop_pool_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON desktop_pool_stats_latest;
CREATE INDEX snapshot_time_hour_key ON dpod_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON dpod_stats_latest;
CREATE INDEX snapshot_time_hour_key ON ds_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON ds_stats_latest;
CREATE INDEX snapshot_time_hour_key ON iom_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON iom_stats_latest;
CREATE INDEX snapshot_time_hour_key ON load_balancer_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON load_balancer_stats_latest;
CREATE INDEX snapshot_time_hour_key ON lp_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON lp_stats_latest;
CREATE INDEX snapshot_time_hour_key ON nspace_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON nspace_stats_latest;
CREATE INDEX snapshot_time_hour_key ON pm_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON pm_stats_latest;
CREATE INDEX snapshot_time_hour_key ON sc_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON sc_stats_latest;
CREATE INDEX snapshot_time_hour_key ON service_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON service_stats_latest;
CREATE INDEX snapshot_time_hour_key ON sw_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON sw_stats_latest;
CREATE INDEX snapshot_time_hour_key ON vdc_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON vdc_stats_latest;
CREATE INDEX snapshot_time_hour_key ON view_pod_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON view_pod_stats_latest;
CREATE INDEX snapshot_time_hour_key ON virtual_app_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON virtual_app_stats_latest;
CREATE INDEX snapshot_time_hour_key ON virtual_volume_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON virtual_volume_stats_latest;
CREATE INDEX snapshot_time_hour_key ON vm_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON vm_stats_latest;
CREATE INDEX snapshot_time_hour_key ON vpod_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON vpod_stats_latest;
CREATE INDEX snapshot_time_hour_key ON wkld_ctl_stats_latest (snapshot_time, hour_key(8));
DROP INDEX snapshot_time ON wkld_ctl_stats_latest;
