

use vmtdb ;

start transaction ;

/* add new on-demand reports */
INSERT INTO `on_demand_reports` VALUES (10,'VirtualMachine','Group','Individual VM Monthly Summary by VM Group','vm_group_individual_monthly_summary','Monthly Virtual Machine Group Summary Breakdown by Individual VM','Capacity Management/VMs','Individual VM Monthly Summary');
INSERT INTO `on_demand_reports` VALUES (11,'VirtualMachine','Group','Month Based VM Group Over Under Provisioning','vm_group_daily_over_under_prov_grid_30_days','Month Based VM Group Over Under Provisioning','Capacity Management/VMs','Month Based VM Group Over Under Provisioning');

delete from version_info ;
insert into version_info values (1,34) ;


commit ;
