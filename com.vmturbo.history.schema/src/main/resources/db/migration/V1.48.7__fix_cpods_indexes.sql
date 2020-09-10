ALTER TABLE cpod_stats_by_hour ADD INDEX snapshot_time_idx (snapshot_time);
ALTER TABLE cpod_stats_by_hour ADD INDEX property_type_idx (property_type);
ALTER TABLE cpod_stats_by_hour ADD INDEX uuid_idx (uuid);

ALTER TABLE cpod_stats_by_day ADD INDEX snapshot_time_idx (snapshot_time);
ALTER TABLE cpod_stats_by_day ADD INDEX property_type_idx (property_type);
ALTER TABLE cpod_stats_by_day ADD INDEX uuid_idx (uuid);

ALTER TABLE cpod_stats_by_month ADD INDEX snapshot_time_idx (snapshot_time);
ALTER TABLE cpod_stats_by_month ADD INDEX property_type_idx (property_type);
ALTER TABLE cpod_stats_by_month ADD INDEX uuid_idx (uuid);
