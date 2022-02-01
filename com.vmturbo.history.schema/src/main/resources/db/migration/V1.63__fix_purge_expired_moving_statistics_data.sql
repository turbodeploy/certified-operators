-- Fix the existing event by dropping and recreating it, the only change is 'start_timestamp <'
-- It should purge the data before retention period rather than after.

DELIMITER //

DROP EVENT IF EXISTS purge_expired_moving_statistics_data //
CREATE EVENT purge_expired_moving_statistics_data
ON SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON COMPLETION PRESERVE ENABLE
DO BEGIN
    SELECT `retention_period` INTO @retention_days FROM `retention_policies` WHERE `policy_name` = "moving_statistics_retention_days";
    SELECT max(`start_timestamp`) INTO @start_timestamp FROM `moving_statistics_blobs`;
    DELETE FROM `moving_statistics_blobs` WHERE start_timestamp < @start_timestamp - @retention_days * 24 * 60 * 60 * 1000;
END
//

DELIMITER ;
