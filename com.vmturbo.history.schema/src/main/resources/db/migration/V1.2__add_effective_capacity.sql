-- Add "effective_capacity" to stats tables where the column isn't already created

DROP PROCEDURE IF EXISTS `addEffectiveCapacity`;
DELIMITER //
CREATE PROCEDURE `addEffectiveCapacity`(IN tablename CHAR(100))
BEGIN
	-- add the column if we hit at error running the check query below
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
	    SET @alterStmt = concat("ALTER TABLE ", tablename, " ADD effective_capacity decimal(15,3) DEFAULT NULL;");
		PREPARE alterTable FROM @alterStmt;
	    EXECUTE alterTable;
    	DEALLOCATE PREPARE alterTable;
	END;
	-- check if the column exists -- this query should error out if the column doesn't exist yet.
    SET @checkStmt = concat("SELECT effective_capacity FROM ", tablename, " LIMIT 1;");
	PREPARE checkIfExists FROM @checkStmt;
    EXECUTE checkIfExists;
   	DEALLOCATE PREPARE checkIfExists;
END //
DELIMITER ;

-- run it on our tables
CALL addEffectiveCapacity('app_stats_latest');
CALL addEffectiveCapacity('app_stats_by_day');
CALL addEffectiveCapacity('app_stats_by_hour');
CALL addEffectiveCapacity('app_stats_by_month');

CALL addEffectiveCapacity('ch_stats_latest');
CALL addEffectiveCapacity('ch_stats_by_day');
CALL addEffectiveCapacity('ch_stats_by_hour');
CALL addEffectiveCapacity('ch_stats_by_month');

CALL addEffectiveCapacity('cnt_stats_latest');
CALL addEffectiveCapacity('cnt_stats_by_day');
CALL addEffectiveCapacity('cnt_stats_by_hour');
CALL addEffectiveCapacity('cnt_stats_by_month');

CALL addEffectiveCapacity('cpod_stats_latest');
CALL addEffectiveCapacity('cpod_stats_by_day');
CALL addEffectiveCapacity('cpod_stats_by_hour');
CALL addEffectiveCapacity('cpod_stats_by_month');

CALL addEffectiveCapacity('da_stats_latest');
CALL addEffectiveCapacity('da_stats_by_day');
CALL addEffectiveCapacity('da_stats_by_hour');
CALL addEffectiveCapacity('da_stats_by_month');

CALL addEffectiveCapacity('dpod_stats_latest');
CALL addEffectiveCapacity('dpod_stats_by_day');
CALL addEffectiveCapacity('dpod_stats_by_hour');
CALL addEffectiveCapacity('dpod_stats_by_month');

CALL addEffectiveCapacity('ds_stats_latest');
CALL addEffectiveCapacity('ds_stats_by_day');
CALL addEffectiveCapacity('ds_stats_by_hour');
CALL addEffectiveCapacity('ds_stats_by_month');

CALL addEffectiveCapacity('iom_stats_latest');
CALL addEffectiveCapacity('iom_stats_by_day');
CALL addEffectiveCapacity('iom_stats_by_hour');
CALL addEffectiveCapacity('iom_stats_by_month');

CALL addEffectiveCapacity('lp_stats_latest');
CALL addEffectiveCapacity('lp_stats_by_day');
CALL addEffectiveCapacity('lp_stats_by_hour');
CALL addEffectiveCapacity('lp_stats_by_month');

CALL addEffectiveCapacity('market_stats_latest');
CALL addEffectiveCapacity('market_stats_by_day');
CALL addEffectiveCapacity('market_stats_by_hour');
CALL addEffectiveCapacity('market_stats_by_month');

CALL addEffectiveCapacity('pm_stats_latest');
CALL addEffectiveCapacity('pm_stats_by_day');
CALL addEffectiveCapacity('pm_stats_by_hour');
CALL addEffectiveCapacity('pm_stats_by_month');

CALL addEffectiveCapacity('ri_stats_latest');
CALL addEffectiveCapacity('ri_stats_by_day');
CALL addEffectiveCapacity('ri_stats_by_hour');
CALL addEffectiveCapacity('ri_stats_by_month');

CALL addEffectiveCapacity('sc_stats_latest');
CALL addEffectiveCapacity('sc_stats_by_day');
CALL addEffectiveCapacity('sc_stats_by_hour');
CALL addEffectiveCapacity('sc_stats_by_month');

CALL addEffectiveCapacity('sw_stats_latest');
CALL addEffectiveCapacity('sw_stats_by_day');
CALL addEffectiveCapacity('sw_stats_by_hour');
CALL addEffectiveCapacity('sw_stats_by_month');

CALL addEffectiveCapacity('vdc_stats_latest');
CALL addEffectiveCapacity('vdc_stats_by_day');
CALL addEffectiveCapacity('vdc_stats_by_hour');
CALL addEffectiveCapacity('vdc_stats_by_month');

CALL addEffectiveCapacity('vm_stats_latest');
CALL addEffectiveCapacity('vm_stats_by_day');
CALL addEffectiveCapacity('vm_stats_by_hour');
CALL addEffectiveCapacity('vm_stats_by_month');

CALL addEffectiveCapacity('vpod_stats_latest');
CALL addEffectiveCapacity('vpod_stats_by_day');
CALL addEffectiveCapacity('vpod_stats_by_hour');
CALL addEffectiveCapacity('vpod_stats_by_month');

-- done with the procedure
DROP PROCEDURE `addEffectiveCapacity`;
