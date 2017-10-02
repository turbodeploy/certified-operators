use vmtdb ;

start transaction ;

INSERT INTO `on_demand_reports` VALUES (13,'VirtualMachine','Group','Month Based VM Group Over Under Provisioning','vm_group_daily_over_under_prov_grid_given_days','Month Based VM Group Over Under Provisioning','Capacity Management/VMs','Month Based VM Group Over Under Provisioning');

delete from version_info where id=1;
insert into version_info values (1,48) ;

commit ;
