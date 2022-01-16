-- Migration to adjust all inconsistency and mistakes found out during percentile
-- integration testing
-- Please, check https://vmturbo.atlassian.net/browse/OM-51955 for more details
-- Please, check  https://vmturbo.atlassian.net/wiki/spaces/XD/pages/1006174930/Change%2Bto%2Bpercentiles%2Bbackend%2Bin%2BXL%2Bvs%2Bclassic for design

DELIMITER $$

-- Change start_timestamp column type from timestamp to long.

DROP TABLE IF EXISTS `percentile_blobs`;

$$

CREATE TABLE `percentile_blobs` (
    `start_timestamp` bigint (20) PRIMARY KEY,
    `aggregation_window_length` bigint(20) NOT NULL DEFAULT '0',
    `data` longblob NULL,
    INDEX(`start_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

$$

-- Adjust SP to purge outdated percentile data relying on the timestamp of last maintenance
-- operation and don't purge total record (i.e. which start_timestamp is equal to 0)

DROP PROCEDURE IF EXISTS `purge_percentile_data`;

$$

CREATE DEFINER = CURRENT_USER PROCEDURE `purge_percentile_data`(
IN policyName CHAR(50),
IN periodName CHAR(20)) MODIFIES SQL DATA
BEGIN
SET @purge_sql = concat('DELETE FROM
	`percentile_blobs`
WHERE
	`start_timestamp` != 0
	AND `start_timestamp` < (
	SELECT
		date_sub(
		(SELECT
			`aggregation_window_length`
		FROM
			`percentile_blobs`
		WHERE
			`start_timestamp` = 0),
			INTERVAL `retention_period` ',periodName,')
	FROM
		`retention_policies`
	WHERE
		`policy_name` = ',policyName,')');
PREPARE stmt FROM @purge_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END

$$

DROP EVENT IF EXISTS `purge_expired_percentile_data`;

$$

-- Event to remove percentile blobs which are expired according to value configured
-- for retention_policies.retention_days
CREATE EVENT `purge_expired_percentile_data` ON
SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON
COMPLETION PRESERVE ENABLE DO
BEGIN
    CALL `purge_percentile_data`('percentile_retention_days', 'day');
END

$$

-- Change percentile retention days value to 91.
DELETE FROM `retention_policies` WHERE `policy_name` = 'percentile_retention_days';

$$

INSERT INTO `retention_policies` VALUES ('percentile_retention_days', 91);

$$

DELIMITER ;
