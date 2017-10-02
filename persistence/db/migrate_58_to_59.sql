use vmtdb ;

start transaction ;

DELETE from on_demand_reports where id=14;
INSERT INTO `on_demand_reports` VALUES (14,'PhysicalMachine','Group','Monthly Individual Cluster Summary ','pm_group_monthly_individual_cluster_summary','Monthly Individual Cluster Summary','Performance Management/Hosts','Host Group Summary');

DELETE from on_demand_reports where id=15;
INSERT INTO `on_demand_reports` VALUES (15,'PhysicalMachine','Group','Host Top Bottom Capacity Grid Per Cluster','pm_group_pm_top_bottom_capacity_grid_per_cluster','Host Top Bottom Capacity Grid Per Cluster','Performance Management/Hosts','Host Group Summary');

/*
 * PERSIST VERSION INFO TO THE DB 
 */
delete from version_info where id=1;
insert into version_info values (1,59) ;

commit ;