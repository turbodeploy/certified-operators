
use vmtdb ;

start transaction ;

update snapshots set property_value = property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;

update snapshots_stats_by_hour set property_value = property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_hour set avg_property_value = avg_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_hour set min_property_value = min_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_hour set max_property_value = max_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;

update snapshots_stats_by_day set property_value = property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_day set avg_property_value = avg_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_day set min_property_value = min_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_day set max_property_value = max_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;

update snapshots_stats_by_month set property_value = property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_month set avg_property_value = avg_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_month set min_property_value = min_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;
update snapshots_stats_by_month set max_property_value = max_property_value/1024/1024
  where property_type = 'StorageAmount' and property_subtype in ('used','capacity','conf','disk','iso','log','snapshot','swap','unused') ;


delete from version_info ;
insert into version_info values (1,22) ;

commit ;
