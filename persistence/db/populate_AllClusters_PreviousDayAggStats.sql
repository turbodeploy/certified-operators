use vmtdb ;

/**
 * Iterated over the current clusters and calls previous day aggregation
 * for each of them
 */
delimiter $$
drop procedure if exists populate_AllClusters_PreviousDayAggStats$$
create procedure populate_AllClusters_PreviousDayAggStats()
begin
	DECLARE done INT DEFAULT 0;
	DECLARE cur_clsuter_internal_name varchar(250);
	
	/* Iterate over all of the available clusters in cluster-memebrs */
	DECLARE clusters_iterator CURSOR FOR SELECT DISTINCT internal_name FROM cluster_members_yesterday WHERE group_type='PhysicalMachine';
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN clusters_iterator;

	REPEAT
	FETCH clusters_iterator INTO cur_clsuter_internal_name;
	IF NOT done THEN
		call clusterAggPreviousDay(cur_clsuter_internal_name);
	END IF;
	UNTIL done END REPEAT;

	CLOSE clusters_iterator;
end $$
delimiter ;

