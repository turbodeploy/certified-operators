use vmtdb ;

source verify_db_charset_and_collation.sql;

/*
 * PERSIST VERSION INFO TO THE DB 
 */
start transaction ;

delete from standard_reports where id=190;
INSERT INTO standard_reports VALUES (190, 'Monthly VMem Capacity', 'This report shows the VMem capacity over the last month, that was assigned to the VMs in the environment. Where the capacity for a VM was increased or decreased during the month, the higher value is used for the entire month. This report also lists all the clusters that were active in the month.', 'VMem Capacity', 'Capacity Management/VMs', 'monthly_VMem_capacity_cost', 'Monthly', NULL);

delete from version_info where id=1;
insert into version_info values (1,55) ;

commit ;
