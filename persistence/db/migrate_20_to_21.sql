
use vmtdb ;

start transaction ;

update snapshots set property_value = property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;

update snapshots_stats_by_hour set property_value = property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_hour set avg_property_value = avg_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_hour set min_property_value = min_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_hour set max_property_value = max_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;

update snapshots_stats_by_day set property_value = property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_day set avg_property_value = avg_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_day set min_property_value = min_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_day set max_property_value = max_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;

update snapshots_stats_by_month set property_value = property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_month set avg_property_value = avg_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_month set min_property_value = min_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;
update snapshots_stats_by_month set max_property_value = max_property_value/20.0 where property_name in ('StorageAccess/used','StorageAccess/capacity') ;


drop view if exists snapshots_storage_used_by_day_per_se_group ;
drop view if exists snapshots_vm_count_by_day_per_se_group ;


delete from version_info ;
insert into version_info values (1,21) ;

commit ;

alter table report_subscriptions drop foreign key fk_report_subscriptions_standard_report_standard_report_id ;

