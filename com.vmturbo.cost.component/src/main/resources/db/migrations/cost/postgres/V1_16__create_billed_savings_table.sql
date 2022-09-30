-- Table to store savings daily stats if billed based savings feature flag is enabled.
CREATE TABLE IF NOT EXISTS billed_savings_by_day (
    entity_oid bigint NOT NULL,
    stats_time timestamp NOT NULL DEFAULT '0001-01-01 00:00:00.000',
    stats_type int NOT NULL,
    stats_value double precision NOT NULL,
    samples int NOT NULL,
    PRIMARY KEY (entity_oid, stats_time, stats_type)
);
CREATE INDEX idx_billed_savings_by_day_stats_time ON billed_savings_by_day(stats_time);
