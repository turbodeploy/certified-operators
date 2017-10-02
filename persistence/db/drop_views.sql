

use vmtdb ;


/*
    ---------------------------------------------------------------
        Drop all Views
    ---------------------------------------------------------------
*/

/* old views */

drop view if exists snapshot_time_last_hour  ;
drop view if exists snapshots_last_hour  ;
drop view if exists snapshots_as_decimals ;
drop view if exists snapshots_summary_by_hour ;
drop view if exists snapshots_summary_by_day ;
drop view if exists snapshots_capacities_by_hour ;
drop view if exists snapshots_capacity_by_hour ;
drop view if exists snapshots_capacities_by_day ;
drop view if exists snapshots_capacity_by_day ;
drop view if exists snapshots_summary_by_month ;
drop view if exists snapshots_utilization_range_counts_by_hour;
drop view if exists snapshots_utilization_range_counts_by_day ;
drop view if exists entity_groups ;
drop view if exists entity_group_assns ;
drop view if exists entity_group_members ;
drop view if exists snapshots_per_se_group ;
drop view if exists snapshots_stats_by_hour_per_se_group ;
drop view if exists snapshots_capacity_by_day_per_se_group ;
drop view if exists snapshots_stats_by_day_per_se_group ;
drop view if exists snapshots_stats_by_month_per_se_group ;
drop view if exists snapshots_stats_utilization_range_counts_by_hour_per_se_group ;
drop view if exists snapshots_stats_utilization_range_counts_by_day_per_se_group ;
drop view if exists snapshots_storage_used_by_day_per_vm_group ;
drop view if exists snapshots_vm_count_by_day_per_pm_group ;


/* new views */

drop view if exists pm_summary_stats_by_day ;
drop view if exists vm_summary_stats_by_day ;
drop view if exists ds_summary_stats_by_day ;
drop view if exists sw_summary_stats_by_day ;
drop view if exists da_summary_stats_by_day ;
drop view if exists sc_summary_stats_by_day ;
drop view if exists pm_instances ;
drop view if exists vm_instances ;
drop view if exists ds_instances ;
drop view if exists pm_util_stats_yesterday ;
drop view if exists pm_util_info_yesterday ;
drop view if exists vm_util_stats_yesterday ;
drop view if exists vm_util_info_yesterday ;
drop view if exists ds_util_stats_yesterday ;
drop view if exists ds_util_info_yesterday ;
drop view if exists pm_capacity_by_hour ;
drop view if exists pm_capacity_by_day ;
drop view if exists vm_capacity_by_hour ;
drop view if exists vm_capacity_by_day ;
drop view if exists ds_capacity_by_hour ;
drop view if exists ds_capacity_by_day ;
drop view if exists user_pm_stats_by_hour ;
drop view if exists user_vm_stats_by_hour ;
drop view if exists user_vm_stats_by_hour_per_group ;
drop view if exists user_ds_stats_by_hour ;
drop view if exists user_pm_stats_by_day ;
drop view if exists user_vm_stats_by_day ;
drop view if exists user_vm_stats_by_day_per_group ;
drop view if exists user_ds_stats_by_day ;
drop view if exists entity_groups ;
drop view if exists pm_groups ;
drop view if exists vm_groups ;
drop view if exists ds_groups ;
drop view if exists app_groups ;
drop view if exists entity_group_assns ;
drop view if exists pm_group_assns ;
drop view if exists vm_group_assns ;
drop view if exists ds_group_assns ;
drop view if exists entity_group_members ;
drop view if exists pm_group_members ;
drop view if exists vm_group_members ;
drop view if exists vm_group_members_agg ;
drop view if exists ds_group_members ;
drop view if exists pm_capacity_by_day_per_pm_group ;
drop view if exists vm_capacity_by_day_per_vm_group ;
drop view if exists ds_capacity_by_day_per_ds_group ;
drop view if exists pm_stats_by_hour_per_pm_group ;
drop view if exists vm_stats_by_hour_per_vm_group ;
drop view if exists vm_stats_by_hour_per_vm_group_agg ;
drop view if exists ds_stats_by_hour_per_ds_group ;
drop view if exists pm_stats_by_day_per_pm_group ;
drop view if exists vm_stats_by_day_per_vm_group ;
drop view if exists vm_stats_by_day_per_vm_group_agg ;
drop view if exists ds_stats_by_day_per_ds_group ;
drop view if exists active_vms ;
drop view if exists vm_storage_used_by_day_per_vm_group ;
drop view if exists pm_vm_count_by_day_per_pm_group ;
