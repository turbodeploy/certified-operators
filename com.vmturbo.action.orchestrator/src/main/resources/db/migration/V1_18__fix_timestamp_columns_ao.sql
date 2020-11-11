-- If a column is created with data type timestamp, NOT NULL, with no default value and it is
-- the first timestamp column of the table, mysql will automatically assigned the
-- DEFAULT CURRENT_TIMESTAMP and ON UPDATE CURRENT_TIMESTAMP attributes for this column.
-- (https://dev.mysql.com/doc/refman/5.6/en/upgrading-from-previous-series.html)
-- The "on update" attribute can cause issues if the timestamp is updated unintentionally
-- when the record is updated. This upgrade step removed the "ON UPDATE CURRENT_TIMESTAMP" attribute
-- on all timestamp columns.

ALTER TABLE action_history
CHANGE create_time
create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_plan
CHANGE create_time
create_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_snapshot_day
CHANGE day_time
day_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_snapshot_hour
CHANGE hour_time
hour_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_snapshot_latest
CHANGE action_snapshot_time
action_snapshot_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_snapshot_month
CHANGE month_time
month_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_stats_by_day
CHANGE day_time
day_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_stats_by_hour
CHANGE hour_time
hour_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_stats_by_month
CHANGE month_time
month_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE action_stats_latest
CHANGE action_snapshot_time
action_snapshot_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP;
