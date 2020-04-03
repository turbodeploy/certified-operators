delete from retention_policies where policy_name = 'timeslot_retention_hours';
insert into retention_policies VALUES('timeslot_retention_hours', 721, 'HOURS');

delete from vm_stats_latest where property_subtype = 'percentileUtilization';
delete from vm_stats_by_hour where property_subtype = 'percentileUtilization';
delete from vm_stats_by_day where property_subtype = 'percentileUtilization';
delete from vm_stats_by_month where property_subtype = 'percentileUtilization';

delete from bu_stats_latest where property_subtype = 'percentileUtilization';
delete from bu_stats_by_hour where property_subtype = 'percentileUtilization';
delete from bu_stats_by_day where property_subtype = 'percentileUtilization';
delete from bu_stats_by_month where property_subtype = 'percentileUtilization';
