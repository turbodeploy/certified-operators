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
        UNIX_TIMESTAMP(date_sub(
        (SELECT
            DISTINCT FROM_UNIXTIME(`aggregation_window_length`/1000)
        FROM
            `percentile_blobs`
        WHERE
            `start_timestamp` = 0),
            INTERVAL `retention_period` ',periodName,')) * 1000
    FROM
        `retention_policies`
    WHERE
        `policy_name` = "',policyName,'")');
PREPARE stmt FROM @purge_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
END

$$

DELIMITER ;
