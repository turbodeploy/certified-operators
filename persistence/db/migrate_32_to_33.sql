

use vmtdb ;

start transaction ;

/*Update text of over/under provisioning report*/
delete from standard_reports where id=181;
INSERT INTO `standard_reports` VALUES (181,'Individual VM Monthly Summary','This report shows a monthly summary of Virtual Machines CPU, Memory and Storage measurements','End of the month CPU, Memory, Storage summary','Capacity Management/VMs','monthly_individual_vm_summary','Monthly', NULL);

/*Insert new reports*/
INSERT INTO `standard_reports` VALUES (182,'Monthly Top Bottom 15 Hosts Capacity','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','Host 15 Top Bottom Capacity','Capacity Management/Hosts','monthly_30_days_pm_top_bottom_capacity_grid','Monthly', NULL);
INSERT INTO `standard_reports` VALUES (183,'Monthly Top Bottom 15 VMs Capacity','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','VM 15 Top Bottom Capacity','Capacity Management/VMs','monthly_30_days_vm_top_bottom_capacity_grid','Monthly', NULL);
INSERT INTO `standard_reports` VALUES (184,'VM Over/Under Provisioning 30 Days','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory','VM Over/Under Provisioning 30 Days','Capacity Management/VMs','daily_vm_over_under_prov_grid_30_days','Daily', NULL);

/*Update text of over/under provisioning report*/
delete from standard_reports where id=148;
INSERT INTO `standard_reports` VALUES (148,'VM Over/Under Provisioning 90 Days','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory','VM Over/Under Provisioning 90 Days','Capacity Management/VMs','daily_vm_over_under_prov_grid','Daily',NULL);

delete from version_info ;
insert into version_info values (1,33) ;


commit ;
