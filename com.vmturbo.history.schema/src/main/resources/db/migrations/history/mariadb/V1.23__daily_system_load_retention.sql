-- Implements daily retention policy for system_load table

DELIMITER $$

DELETE FROM `retention_policies` WHERE `policy_name` = 'systemload_retention_days';

$$

INSERT INTO `retention_policies` VALUES ('systemload_retention_days', 30);

$$

DROP EVENT IF EXISTS `purge_expired_systemload_data`;

$$

-- Event to remove system load records which are expired according to value configured
-- for retention_policies.retention_days
CREATE EVENT `purge_expired_systemload_data` ON
SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON
COMPLETION NOT PRESERVE ENABLE DO
BEGIN
    CALL `purge_expired_data`('system_load', 'snapshot_time', 'systemload_retention_days', 'day');
END

$$
DELIMITER ;
