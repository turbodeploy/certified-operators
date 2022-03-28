-- We'll set fill factor for rollup tables, which are subject to updates to many records during
-- the records' lifetimes, so that those updates can be HOT (Heap-Only-Tuple), with all activity
-- for each record limited to that record's existing storage page.

ALTER TABLE entity_cost_by_hour SET (fillfactor=75);
-- note that entity_savings_by_hour is not the target of updates or upserts, only straight inserts
ALTER TABLE reserved_instance_utilization_by_hour SET (fillfactor=75);
ALTER TABLE reserved_instance_coverage_by_hour SET (fillfactor=75);

ALTER TABLE entity_cost_by_day SET (fillfactor=75);
ALTER TABLE entity_savings_by_day SET (fillfactor=75);
ALTER TABLE reserved_instance_utilization_by_day SET (fillfactor=75);
ALTER TABLE reserved_instance_coverage_by_day SET (fillfactor=75);

ALTER TABLE entity_cost_by_month SET (fillfactor=75);
ALTER TABLE entity_savings_by_month SET (fillfactor=75);
ALTER TABLE reserved_instance_utilization_by_month SET (fillfactor=75);
ALTER TABLE reserved_instance_coverage_by_month SET (fillfactor=75);


