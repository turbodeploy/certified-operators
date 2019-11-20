-- Fixed https://vmturbo.atlassian.net/browse/OM-52765 caused by MariaDB specific issue
-- described in https://jira.mariadb.org/browse/MDEV-12137

DELIMITER $$

DROP PROCEDURE IF EXISTS `purge_percentile_data`;

$$

CREATE DEFINER = CURRENT_USER PROCEDURE `purge_percentile_data`(
IN policyName CHAR(50),
IN periodName CHAR(20)) MODIFIES SQL DATA
BEGIN
SET @purge_sql = concat('
DELETE
	pb
FROM
	`percentile_blobs` pb
JOIN (
	SELECT
		date_sub(
		FROM_UNIXTIME((SELECT `aggregation_window_length` FROM `percentile_blobs` WHERE `start_timestamp` = 0) / 1000),
		INTERVAL `retention_period` ',periodName,') as oldestRecord
	FROM
		`retention_policies`
	WHERE
		`policy_name` = "',policyName,'" ) ds ON
	FROM_UNIXTIME(`start_timestamp` / 1000) < ds.oldestRecord
WHERE
	`start_timestamp` != 0;');
PREPARE stmt FROM @purge_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END

$$

DELIMITER ;
