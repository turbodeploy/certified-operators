--    Update "hist_utilization" table:
--        - table requires additional field to be used with timeslot - `property_slot` int(11) null default null
--        - table needs a covering index added on (oid, producer_oid, property_type_id, property_subtype_id, property_slot, commodity_key)
--
--    See OM-45742 for details.
DROP TABLE IF EXISTS `hist_utilization`;
CREATE TABLE `hist_utilization` (
`oid` BIGINT(20) NOT NULL,
`producer_oid` BIGINT(20) NOT NULL,
`property_type_id` INT(11) NOT NULL,
`property_subtype_id` INT(11) NOT NULL,
`commodity_key` VARCHAR(80) NOT NULL COLLATE 'utf8_unicode_ci',
`value_type` INT(11) NOT NULL,
`property_slot` INT(11) NOT NULL,
`utilization` DECIMAL(15,3) NULL DEFAULT NULL,
`capacity` DECIMAL(15,3) NULL DEFAULT NULL,
INDEX `hist_util_index` (`oid`, `producer_oid`, `property_type_id`, `property_subtype_id`, `value_type`, `property_slot`))
COLLATE='utf8_unicode_ci'
ENGINE=InnoDB;

DELETE FROM `retention_policies` WHERE policy_name='timeslot_retention_hours';
INSERT INTO `retention_policies`
(policy_name, retention_period)
VALUES('timeslot_retention_hours', 720);

--    Stored procedures updates:
--        - Modify procedure purge_expired_hours - pass additional filter for property_type and name of retention_policy as arguments.
--        - Modify trigger_purge_expired.
DROP PROCEDURE IF EXISTS `purge_expired_hours`;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_hours`(IN statspref CHAR(10),
IN filter_for_purge CHAR(72),
IN ret_name CHAR(50))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_hour
             WHERE snapshot_time<(SELECT date_sub(current_timestamp, interval retention_period hour)
             FROM retention_policies WHERE policy_name=''',ret_name,''') ',filter_for_purge,' limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;

-- Modified purge_expired_hours call with passing of parameters.
DROP PROCEDURE IF EXISTS `trigger_purge_expired`;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `trigger_purge_expired`()
  BEGIN
    -- purge latest table records
    call purge_expired_latest('app');
    call purge_expired_latest('ch');
    call purge_expired_latest('cnt');
    call purge_expired_latest('dpod');
    call purge_expired_latest('ds');
    call purge_expired_latest('iom');
    call purge_expired_latest('pm');
    call purge_expired_latest('sc');
    call purge_expired_latest('sw');
    call purge_expired_latest('vdc');
    call purge_expired_latest('vm');
    call purge_expired_latest('vpod');
    call purge_expired_latest('bu');

    -- purge _by_hour records
    call purge_expired_hours('app', '', 'retention_hours');
    call purge_expired_hours('ch', '', 'retention_hours');
    call purge_expired_hours('cnt', '', 'retention_hours');
    call purge_expired_hours('dpod', '', 'retention_hours');
    call purge_expired_hours('ds', '', 'retention_hours');
    call purge_expired_hours('iom', '', 'retention_hours');
    call purge_expired_hours('pm', '', 'retention_hours');
    call purge_expired_hours('sc', '', 'retention_hours');
    call purge_expired_hours('sw', '', 'retention_hours');
    call purge_expired_hours('vdc', 'AND property_type NOT IN (''PoolCPU'', ''PoolMem'', ''PoolStorage'')', 'retention_hours');
    call purge_expired_hours('vdc', 'AND property_type IN (''PoolCPU'', ''PoolMem'', ''PoolStorage'')', 'timeslot_retention_hours');
    call purge_expired_hours('vm', '', 'retention_hours');
    call purge_expired_hours('vpod', '', 'retention_hours');
    call purge_expired_hours('bu', 'AND property_type NOT IN (''PoolCPU'', ''PoolMem'', ''PoolStorage'')', 'retention_hours');
    call purge_expired_hours('bu', 'AND property_type IN (''PoolCPU'', ''PoolMem'', ''PoolStorage'')', 'timeslot_retention_hours');

    -- purge _by_days records
    call purge_expired_days('app');
    call purge_expired_days('ch');
    call purge_expired_days('cnt');
    call purge_expired_days('dpod');
    call purge_expired_days('ds');
    call purge_expired_days('iom');
    call purge_expired_days('pm');
    call purge_expired_days('sc');
    call purge_expired_days('sw');
    call purge_expired_days('vdc');
    call purge_expired_days('vm');
    call purge_expired_days('vpod');
    call purge_expired_days('bu');

    -- purge _by_months records
    call purge_expired_months('app');
    call purge_expired_months('ch');
    call purge_expired_months('cnt');
    call purge_expired_months('dpod');
    call purge_expired_months('ds');
    call purge_expired_months('iom');
    call purge_expired_months('pm');
    call purge_expired_months('sc');
    call purge_expired_months('sw');
    call purge_expired_months('vdc');
    call purge_expired_months('vm');
    call purge_expired_months('vpod');
    call purge_expired_months('bu');
  END ;;
DELIMITER ;
