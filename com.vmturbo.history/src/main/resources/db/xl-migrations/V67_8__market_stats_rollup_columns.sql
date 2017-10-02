-- Prepare tables for anyncronous stats rollup

-- Add aggregated column to market_stats_latest table;
alter table market_stats_latest add aggregated boolean not null default false;


update market_stats_latest set aggregated=true;


-- Create index on aggregated column

create index market_aggr_idx on app_stats_latest (aggregated);

alter table market_stats_by_hour modify entity_type varchar(80) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_hour modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_hour modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_hour COLLATE=utf8_general_ci;

alter table market_stats_by_day modify entity_type varchar(80) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_day modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_day modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_day COLLATE=utf8_general_ci;

alter table market_stats_by_month modify entity_type varchar(80) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_month modify property_type varchar(36) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_month modify property_subtype varchar(36) character set utf8 collate utf8_general_ci default null;
alter table market_stats_by_month COLLATE=utf8_general_ci;

-- Create unique compound indexes for each rollup table.  Significantly improves performances of finding and updating stats
create index market_stats_by_hour_idx on market_stats_by_hour (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation);
create index market_stats_by_hour_idx on market_stats_by_day (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation);
create index market_stats_by_hour_idx on market_stats_by_month (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation);
create index market_latest_idx on market_stats_latest(snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation);

UPDATE version_info SET version=67.8 WHERE id=1;