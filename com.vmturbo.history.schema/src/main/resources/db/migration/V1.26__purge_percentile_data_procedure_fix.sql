-- Fixed both stored procedures triggered by MySQL event and failed due to different reasons:
-- purge_expired_months_cluster: Unknown column ‘name’ in ‘where clause’;
-- purge_expired_percentile_data: Unknown column ‘percentile_retention_days’ in ‘where clause’.
-- For more details, please, consider: https://vmturbo.atlassian.net/browse/OM-52623

DELIMITER $$

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
		`policy_name` = "',policyName,'")');
PREPARE stmt FROM @purge_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END

$$

DROP PROCEDURE IF EXISTS `purge_expired_months_cluster`;

$$

CREATE DEFINER = CURRENT_USER PROCEDURE `purge_expired_months_cluster`()
    MODIFIES SQL DATA
BEGIN
    SET @purge_sql=concat('DELETE FROM cluster_stats_by_month
             where recorded_on<(select date_sub(current_timestamp, interval retention_period month)
             from retention_policies where policy_name="retention_months") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
END

$$

DELIMITER ;
