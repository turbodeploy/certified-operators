-- Create "template" tables for that can be used as a basis for new partitions by the pg_partman
-- plugin. This is the mechanism we'll use to configure attributes on partition tables that
-- are not among those that are not (yet) inherited from the parent table - currently storage
-- properties. We only need one set to cover all the entity-stats tables.
--
-- Autovacuuming is disabled for these tables because we never delete records from them. The sole
-- source of dead storage are the updates, and we are doing our best to ensure that all our nearly
-- all updates are HOT. This implies that table storage will be approximately double  what would
-- otherwise be required, and this space will be reclaimed when the partition is dropped for
-- retention policies.

CREATE TABLE _latest_partition_template (LIKE vm_stats_latest);
CREATE TABLE _by_hour_partition_template (LIKE vm_stats_by_hour)
    WITH (fillfactor=50, autovacuum_enabled=false);
CREATE TABLE _by_day_partition_template (LIKE vm_stats_by_day)
    WITH (fillfactor=50, autovacuum_enabled=false);
CREATE TABLE _by_month_partition_template (LIKE vm_stats_by_month)
    WITH (fillfactor=50, autovacuum_enabled=false);

-- set fill factors for non-partitioned rollup tables, using a less aggressive fill factor and
-- without disabling auto-vacuum

ALTER TABLE cluster_stats_by_hour SET (fillfactor=75);
ALTER TABLE cluster_stats_by_day SET (fillfactor=75);
ALTER TABLE cluster_stats_by_month SET (fillfactor=75);

ALTER TABLE market_stats_by_hour SET (fillfactor=75);
ALTER TABLE market_stats_by_day SET (fillfactor=75);
ALTER TABLE market_stats_by_month SET (fillfactor=75);

ALTER TABLE ri_stats_by_hour SET (fillfactor=75);
ALTER TABLE ri_stats_by_day SET (fillfactor=75);
ALTER TABLE ri_stats_by_month SET (fillfactor=75);
