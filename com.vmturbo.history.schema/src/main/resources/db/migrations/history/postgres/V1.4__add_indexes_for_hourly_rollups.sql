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

CREATE INDEX IF NOT EXISTS app_component_stats_latest_snapshot_time_hour_key ON app_component_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS app_component_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  app_server_stats_latest_snapshot_time_hour_key ON app_server_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS app_server_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  app_stats_latest_snapshot_time_hour_key ON app_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS app_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  bu_stats_latest_snapshot_time_hour_key ON bu_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS bu_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  business_app_stats_latest_snapshot_time_hour_key ON business_app_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS business_app_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  business_transaction_stats_latest_snapshot_time_hour_key ON business_transaction_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS business_transaction_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  ch_stats_latest_snapshot_time_hour_key ON ch_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS ch_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  cnt_spec_stats_latest_snapshot_time_hour_key ON cnt_spec_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS cnt_spec_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  cnt_stats_latest_snapshot_time_hour_key ON cnt_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS cnt_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  container_cluster_stats_latest_snapshot_time_hour_key ON container_cluster_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS container_cluster_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  cpod_stats_latest_snapshot_time_hour_key ON cpod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS cpod_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  da_stats_latest_snapshot_time_hour_key ON da_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS da_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  db_server_stats_latest_snapshot_time_hour_key ON db_server_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS db_server_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  db_stats_latest_snapshot_time_hour_key ON db_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS db_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  desktop_pool_stats_latest_snapshot_time_hour_key ON desktop_pool_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS desktop_pool_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  dpod_stats_latest_snapshot_time_hour_key ON dpod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS dpod_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  ds_stats_latest_snapshot_time_hour_key ON ds_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS ds_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  iom_stats_latest_snapshot_time_hour_key ON iom_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS iom_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  load_balancer_stats_latest_snapshot_time_hour_key ON load_balancer_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS load_balancer_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  lp_stats_latest_snapshot_time_hour_key ON lp_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS lp_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  nspace_stats_latest_snapshot_time_hour_key ON nspace_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS nspace_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  pm_stats_latest_snapshot_time_hour_key ON pm_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS pm_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  sc_stats_latest_snapshot_time_hour_key ON sc_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS sc_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  service_stats_latest_snapshot_time_hour_key ON service_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS service_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  sw_stats_latest_snapshot_time_hour_key ON sw_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS sw_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  vdc_stats_latest_snapshot_time_hour_key ON vdc_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS vdc_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  view_pod_stats_latest_snapshot_time_hour_key ON view_pod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS view_pod_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  virtual_app_stats_latest_snapshot_time_hour_key ON virtual_app_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS virtual_app_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  virtual_volume_stats_latest_snapshot_time_hour_key ON virtual_volume_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS virtual_volume_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  vm_stats_latest_snapshot_time_hour_key ON vm_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS vm_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS  vpod_stats_latest_snapshot_time_hour_key ON vpod_stats_latest (snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS vpod_stats_latest_snapshot_time_idx;
CREATE INDEX IF NOT EXISTS wkld_ctl_stats_latest_snapshot_time_hour_key ON wkld_ctl_stats_latest(snapshot_time, left(hour_key, 8));
DROP INDEX IF EXISTS wkld_ctl_stats_latest_snapshot_time_idx;

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
