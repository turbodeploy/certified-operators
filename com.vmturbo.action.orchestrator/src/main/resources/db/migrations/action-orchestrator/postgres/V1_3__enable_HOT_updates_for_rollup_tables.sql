-- We'll set fill factor for rollup tables, which are subject to updates to many records during
-- the records' lifetimes, so that those updates can be HOT (Heap-Only-Tuple), with all activity
-- for each record limited to that record's existing storage page.
ALTER TABLE action_snapshot_day SET (fillfactor=75);
ALTER TABLE action_snapshot_month SET (fillfactor=75);
ALTER TABLE action_stats_by_day SET (fillfactor=75);
ALTER TABLE action_stats_by_month SET (fillfactor=75);


