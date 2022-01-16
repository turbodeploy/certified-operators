DELIMITER //

-- Creating moving statistics tables:
DROP TABLE IF EXISTS `moving_statistics_blobs` //
CREATE TABLE `moving_statistics_blobs` (
    `start_timestamp` bigint (20) PRIMARY KEY,
    `data` longblob NULL,
    `chunk_index` int not null default '0',
    INDEX(`start_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci //

-- Setting moving statistics retention days value to 2.
DELETE FROM `retention_policies` WHERE `policy_name` = 'moving_statistics_retention_days';
//
INSERT INTO `retention_policies` VALUES ('moving_statistics_retention_days', 2, 'DAYS');
//

-- Creating an event to call purging moving statistics data
DROP EVENT IF EXISTS purge_expired_moving_statistics_data //
CREATE EVENT purge_expired_moving_statistics_data
ON SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON COMPLETION PRESERVE ENABLE
DO BEGIN
    SELECT `retention_period` INTO @retention_days FROM `retention_policies` WHERE `policy_name` = "moving_statistics_retention_days";
    SELECT max(`start_timestamp`) INTO @start_timestamp FROM `moving_statistics_blobs`;
    DELETE FROM `moving_statistics_blobs` WHERE start_timestamp > @start_timestamp - @retention_days * 24 * 60 * 60 * 1000;
END
//

DELIMITER ;
