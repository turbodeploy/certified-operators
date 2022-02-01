-- simplifications of market-stats tables:
-- * Add a primary key so rollups can be simplified to simple upsert statements
--   This will require a new proxy key similar to those used in entity-stats tables, which will
--   need to be back-filled into existing records as part of this migration.
-- * Remove partitioning
--   The sizes of the market-stats tables simply don't justify the complexity and overhead of
--   partitioning. Retention processing will be via batched deletes.
-- We'll also remove and index that covers the columns that will be part of the proxy key, which
-- currently exists solely in service of the existing rollup stored proc. That stored proc and
-- any associated events will be removed as well.

DROP PROCEDURE IF EXISTS _exec;
DELIMITER //
CREATE PROCEDURE _exec(sql_stmt text)
BEGIN
    -- we assume that errors executing any of our options are due to the statement having
    -- previously been executed, so preconditions are not met. This will happen when this
    -- migration is executed a second time after we retire POSTGRES_PRIMARY_DB feature flag.
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN END;
    SET @stmt = sql_stmt;
    PREPARE stmt FROM @stmt;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
END //
DELIMITER ;

DROP PROCEDURE IF EXISTS _fix_market_stats_table;
DELIMITER //
CREATE PROCEDURE _fix_market_stats_table(suffix text, has_aggregated boolean)
BEGIN
   SET @table = concat('market_stats_', suffix);
   -- 'aggregated' column will no longer be needed
   IF has_aggregated THEN
       CALL _exec(concat('ALTER TABLE ', @table, ' DROP COLUMN aggregated'));
   END IF;
   -- add new proxy key (128-bit MD5 checksum, in 32 hex digits)
   CALL _exec(concat('ALTER TABLE ', @table, ' ADD COLUMN time_series_key char(32)'));
   -- backfill proxy key values for existing records (Java code will replicate this calculation
   -- for new insertions)
   CALL _exec(concat('UPDATE ', @table, ' SET time_series_key = md5(concat(',
        'coalesce(cast(topology_context_id AS char), \'-\'),',
        'coalesce(entity_type, \'-\'),',
        'coalesce(property_type, \'-\'),',
        'coalesce(property_subtype, \'-\'),',
        'coalesce(cast(environment_type AS char), \'-\')))'));
   -- make our primary key columns NOT NULL
   CALL _exec(concat('ALTER TABLE ', @table, ' MODIFY COLUMN time_series_key char(32) NOT NULL'));
   CALL _exec(concat('ALTER TABLE ', @table, ' MODIFY COLUMN snapshot_time datetime NOT NULL'));
   -- define the new primary key
   CALL _exec(concat('ALTER TABLE ', @table, ' ADD PRIMARY KEY (snapshot_time, time_series_key)'));
   -- remove partitioning from the table
   CALL _exec(concat('ALTER TABLE ', @table, ' REMOVE PARTITIONING'));
END //
DELIMITER ;

-- fix all the market-stats tables
CALL _fix_market_stats_table('latest', true);
CALL _fix_market_stats_table('by_hour', false);
CALL _fix_market_stats_table('by_day', false);
CALL _fix_market_stats_table('by_month', false);

-- rollups will be performed in Java code now, no longer need the stored proc
CALL _exec ('DROP PROCEDURE market_aggregate');

-- drop indexes that are no longer needed since they specifically supported the stored proc
CALL _exec('DROP INDEX market_latest_idx ON market_stats_latest');
CALL _exec('DROP INDEX market_stats_by_hour_idx ON market_stats_by_hour');
-- yes, the indexes on by_day and by_month include 'by_hour' in their name
CALL _exec('DROP INDEX market_stats_by_hour_idx ON market_stats_by_day');
CALL _exec('DROP INDEX market_stats_by_hour_idx ON market_stats_by_month');

DROP PROCEDURE _fix_market_stats_table;
DROP PROCEDURE _exec;
