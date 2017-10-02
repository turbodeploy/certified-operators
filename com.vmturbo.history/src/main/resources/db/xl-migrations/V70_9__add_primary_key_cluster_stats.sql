-- ADD primary key constraints for cluster_stats_by_day, cluster_stats_by_month
-- we assume that this primary key does not previously exist.
alter table cluster_stats_by_day add primary key(recorded_on, internal_name, property_type, property_subtype);

alter table cluster_stats_by_month add primary key(recorded_on, internal_name, property_type, property_subtype);

UPDATE version_info SET version=70.9 WHERE id=1;
