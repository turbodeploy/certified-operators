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

CREATE INDEX app_component_stats_latest_snapshot_time_hour_key ON app_component_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX app_component_stats_latest_snapshot_time_idx;
CREATE INDEX app_server_stats_latest_snapshot_time_hour_key ON app_server_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX app_server_stats_latest_snapshot_time_idx;
CREATE INDEX app_stats_latest_snapshot_time_hour_key ON app_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX app_stats_latest_snapshot_time_idx;
CREATE INDEX bu_stats_latest_snapshot_time_hour_key ON bu_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX bu_stats_latest_snapshot_time_idx;
CREATE INDEX business_app_stats_latest_snapshot_time_hour_key ON business_app_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX business_app_stats_latest_snapshot_time_idx;
CREATE INDEX business_transaction_stats_latest_snapshot_time_hour_key ON business_transaction_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX business_transaction_stats_latest_snapshot_time_idx;
CREATE INDEX ch_stats_latest_snapshot_time_hour_key ON ch_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX ch_stats_latest_snapshot_time_idx;
CREATE INDEX cnt_spec_stats_latest_snapshot_time_hour_key ON cnt_spec_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX cnt_spec_stats_latest_snapshot_time_idx;
CREATE INDEX cnt_stats_latest_snapshot_time_hour_key ON cnt_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX cnt_stats_latest_snapshot_time_idx;
CREATE INDEX container_cluster_stats_latest_snapshot_time_hour_key ON container_cluster_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX container_cluster_stats_latest_snapshot_time_idx;
CREATE INDEX cpod_stats_latest_snapshot_time_hour_key ON cpod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX cpod_stats_latest_snapshot_time_idx;
CREATE INDEX da_stats_latest_snapshot_time_hour_key ON da_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX da_stats_latest_snapshot_time_idx;
CREATE INDEX db_server_stats_latest_snapshot_time_hour_key ON db_server_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX db_server_stats_latest_snapshot_time_idx;
CREATE INDEX db_stats_latest_snapshot_time_hour_key ON db_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX db_stats_latest_snapshot_time_idx;
CREATE INDEX desktop_pool_stats_latest_snapshot_time_hour_key ON desktop_pool_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX desktop_pool_stats_latest_snapshot_time_idx;
CREATE INDEX dpod_stats_latest_snapshot_time_hour_key ON dpod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX dpod_stats_latest_snapshot_time_idx;
CREATE INDEX ds_stats_latest_snapshot_time_hour_key ON ds_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX ds_stats_latest_snapshot_time_idx;
CREATE INDEX iom_stats_latest_snapshot_time_hour_key ON iom_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX iom_stats_latest_snapshot_time_idx;
CREATE INDEX load_balancer_stats_latest_snapshot_time_hour_key ON load_balancer_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX load_balancer_stats_latest_snapshot_time_idx;
CREATE INDEX lp_stats_latest_snapshot_time_hour_key ON lp_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX lp_stats_latest_snapshot_time_idx;
CREATE INDEX nspace_stats_latest_snapshot_time_hour_key ON nspace_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX nspace_stats_latest_snapshot_time_idx;
CREATE INDEX pm_stats_latest_snapshot_time_hour_key ON pm_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX pm_stats_latest_snapshot_time_idx;
CREATE INDEX sc_stats_latest_snapshot_time_hour_key ON sc_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX sc_stats_latest_snapshot_time_idx;
CREATE INDEX service_stats_latest_snapshot_time_hour_key ON service_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX service_stats_latest_snapshot_time_idx;
CREATE INDEX sw_stats_latest_snapshot_time_hour_key ON sw_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX sw_stats_latest_snapshot_time_idx;
CREATE INDEX vdc_stats_latest_snapshot_time_hour_key ON vdc_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX vdc_stats_latest_snapshot_time_idx;
CREATE INDEX view_pod_stats_latest_snapshot_time_hour_key ON view_pod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX view_pod_stats_latest_snapshot_time_idx;
CREATE INDEX virtual_app_stats_latest_snapshot_time_hour_key ON virtual_app_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX virtual_app_stats_latest_snapshot_time_idx;
CREATE INDEX virtual_volume_stats_latest_snapshot_time_hour_key ON virtual_volume_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX virtual_volume_stats_latest_snapshot_time_idx;
CREATE INDEX vm_stats_latest_snapshot_time_hour_key ON vm_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX vm_stats_latest_snapshot_time_idx;
CREATE INDEX vpod_stats_latest_snapshot_time_hour_key ON vpod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX vpod_stats_latest_snapshot_time_idx;
CREATE INDEX wkld_ctl_stats_latest_snapshot_time_hour_key ON wkld_ctl_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX wkld_ctl_stats_latest_snapshot_time_idx;

-- we also update the procedure created in migration V1.0 to create new entity-stats table groups,
-- to reflect the index changes applied above
-- Create a SP to generate entity stats tables. This will be retained for use when creating new
-- entity stats tables in the future, in order to promote consistency across entity types.
CREATE OR REPLACE PROCEDURE build_entity_stats_tables(prefix text)
AS $$
BEGIN
  -- create latest table and indexes
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_latest ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) COLLATE ci NOT NULL, producer_uuid varchar(20) COLLATE ci,'
    'property_type varchar(80) COLLATE ci, property_subtype varchar(36) COLLATE ci,'
    'relation smallint, commodity_key varchar(80) COLLATE ci,'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float,'
    'hour_key char(32) COLLATE ci)'
    'PARTITION BY RANGE (snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_snapshot_time_hour_key_idx '
    'ON %1$I_stats_latest(snapshot_time, left(hour_key, 8))', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_uuid_idx '
    'ON %1$I_stats_latest(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_property_name_idx '
    'ON %1$I_stats_latest(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_latest_property_subtype_idx '
    'ON %1$I_stats_latest(property_subtype)', prefix);
  -- create hourly rollup table
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_by_hour ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) COLLATE ci NOT NULL, producer_uuid varchar(20) COLLATE ci,'
    'property_type varchar(80) COLLATE ci, property_subtype varchar(36) COLLATE ci,'
    'relation smallint, commodity_key varchar(80) COLLATE ci,'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float, samples integer,'
    'hour_key char(32) COLLATE ci NOT NULL,'
    'PRIMARY KEY(hour_key, snapshot_time))'
    'PARTITION BY RANGE (snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_snapshot_time_idx '
    'ON %1$I_stats_by_hour(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_uuid_idx '
    'ON %1$I_stats_by_hour(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_property_name_idx '
    'ON %1$I_stats_by_hour(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_hour_property_subtype_idx '
    'ON %1$I_stats_by_hour(property_subtype)', prefix);
  -- create daily rollup table
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_by_day ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) COLLATE ci NOT NULL, producer_uuid varchar(20) COLLATE ci,'
    'property_type varchar(80) COLLATE ci, property_subtype varchar(36) COLLATE ci,'
    'relation smallint, commodity_key varchar(80) COLLATE ci,'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float, samples integer,'
    'day_key char(32) COLLATE ci,'
    'PRIMARY KEY(day_key, snapshot_time))'
    'PARTITION BY RANGE (snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_snapshot_time_idx '
    'ON %1$I_stats_by_day(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_uuid_idx '
    'ON %1$I_stats_by_day(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_property_name_idx '
    'ON %1$I_stats_by_day(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_day_property_subtype_idx '
    'ON %1$I_stats_by_day(property_subtype)', prefix);
  -- create monthly rollup table
  EXECUTE format('CREATE TABLE IF NOT EXISTS %1$I_stats_by_month ('
    'snapshot_time timestamp(3) NOT NULL, uuid varchar(20) COLLATE ci NOT NULL, producer_uuid varchar(20) COLLATE ci,'
    'property_type varchar(80) COLLATE ci, property_subtype varchar(36) COLLATE ci,'
    'relation smallint, commodity_key varchar(80) COLLATE ci,'
    'capacity float, effective_capacity float,'
    'avg_value float, min_value float, max_value float, samples integer,'
    'month_key char(32) COLLATE ci,'
    'PRIMARY KEY(month_key, snapshot_time))'
    'PARTITION BY RANGE (snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_snapshot_time_idx '
    'ON %1$I_stats_by_month(snapshot_time)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_uuid_idx '
    'ON %1$I_stats_by_month(uuid)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_property_name_idx '
    'ON %1$I_stats_by_month(property_type)', prefix);
  EXECUTE format('CREATE INDEX IF NOT EXISTS %1$I_stats_by_month_property_subtype_idx '
    'ON %1$I_stats_by_month(property_subtype)', prefix);
END;
$$ LANGUAGE plpgsql;
