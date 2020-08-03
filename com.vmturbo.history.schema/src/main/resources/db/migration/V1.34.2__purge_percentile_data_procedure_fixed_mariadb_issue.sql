-- NOTE: This migration previously appeared as V1.27 in the 7.21 branch, prior to unifying that
-- branch with 7.17. It conflicted with another migration, also at V1.27, in the 7.17 branch,
-- which was likewise renumbered to V1.34.1. There's now a callback in history component that
-- removes the migrations table record for V1.27, to avoid migration errors due to previously
-- applied migrations disappearing.

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
