-- Fix inconsistencies in stats data types OM-49770

ALTER TABLE da_stats_by_day
  MODIFY snapshot_time DATETIME;

ALTER TABLE da_stats_by_month
  MODIFY snapshot_time DATETIME;

ALTER TABLE cpod_stats_by_day
  MODIFY snapshot_time DATETIME;

ALTER TABLE cpod_stats_by_month
  MODIFY snapshot_time DATETIME;

ALTER TABLE lp_stats_by_day
  MODIFY snapshot_time DATETIME;

ALTER TABLE lp_stats_by_month
  MODIFY snapshot_time DATETIME;
