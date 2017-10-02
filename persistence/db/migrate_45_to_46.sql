

use vmtdb ;

start transaction ;

INSERT INTO `on_demand_reports` VALUES (12,'VirtualMachine','Group','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','vm_group_30_days_vm_top_bottom_capacity_grid','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','Capacity Management/VMs','VM 15 Top Bottom Capacity');

delete from version_info where id=1;
insert into version_info values (1,46) ;

commit ;
