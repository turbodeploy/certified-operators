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

-- a function to create an index if one does not already exist, which is not supported by some
-- of our supported versions of MySQL
DROP PROCEDURE IF EXISTS _create_index_if_not_exists;
DELIMITER $$
CREATE PROCEDURE _create_index_if_not_exists(
    tbl_name text, idx_name text, modifiers text, defn text)
BEGIN
  SELECT EXISTS(SELECT * FROM information_schema.statistics
     WHERE table_schema COLLATE utf8_unicode_ci = database()
       AND table_name COLLATE utf8_unicode_ci = tbl_name
       AND index_name COLLATE utf8_unicode_ci = idx_name)
  INTO @exists;

  IF NOT @exists THEN
    SET @sql = concat('CREATE ', modifiers, ' INDEX `', idx_name,
        '` ON `', tbl_name, '`(', defn, ')');
    select @sql;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END $$
DELIMITER ;

-- a function to drop an index if it exists
DROP PROCEDURE IF EXISTS _drop_index_if_exists;
DELIMITER $$
CREATE PROCEDURE _drop_index_if_exists(tbl_name text, idx_name text)
BEGIN
  SELECT EXISTS(SELECT * FROM information_schema.statistics
     WHERE table_schema COLLATE utf8_unicode_ci = database()
       AND table_name COLLATE utf8_unicode_ci = tbl_name
       AND index_name COLLATE utf8_unicode_ci = idx_name)
  INTO @exists;

  IF @exists THEN
    SET @sql = concat('DROP INDEX ', idx_name, ' ON ', tbl_name);
    select @sql;
    PREPARE stmt FROM @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END $$
DELIMITER ;

-- fix each entity_stats table
CALL _create_index_if_not_exists('app_component_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('app_component_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('app_server_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('app_server_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('app_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('app_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('bu_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('bu_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('business_app_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('business_app_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('business_transaction_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('business_transaction_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('ch_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('ch_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('cnt_spec_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('cnt_spec_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('cnt_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('cnt_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('container_cluster_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('container_cluster_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('cpod_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('cpod_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('da_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('da_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('db_server_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('db_server_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('db_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('db_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('desktop_pool_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('desktop_pool_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('dpod_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('dpod_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('ds_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('ds_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('iom_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('iom_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('load_balancer_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('load_balancer_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('lp_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('lp_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('nspace_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('nspace_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('pm_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('pm_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('sc_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('sc_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('service_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('service_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('sw_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('sw_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('vdc_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('vdc_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('view_pod_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('view_pod_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('virtual_app_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('virtual_app_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('virtual_volume_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('virtual_volume_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('vm_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('vm_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('vpod_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('vpod_stats_latest', 'snapshot_time');
CALL _create_index_if_not_exists('wkld_ctl_stats_latest', 'snapshot_time_hour_key', '','snapshot_time, hour_key(8)');
CALL _drop_index_if_exists('wkld_ctl_stats_latest', 'snapshot_time');

-- drop the stored procs we created
DROP PROCEDURE _create_index_if_not_exists;
DROP PROCEDURE _drop_index_if_exists;
