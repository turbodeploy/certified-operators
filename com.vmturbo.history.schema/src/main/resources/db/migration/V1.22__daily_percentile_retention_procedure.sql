-- Implements daily retention policy for percentile_blobs table according to
-- https://vmturbo.atlassian.net/wiki/spaces/XD/pages/1006174930/Change+to+percentiles+backend+in+XL+vs+classic#ChangetopercentilesbackendinXLvsclassic-Historycomponent
-- design page

DELIMITER $$

DROP PROCEDURE IF EXISTS `purge_expired_data`;

$$

-- Common procedure to remove expired data according to retention policy,
-- described in retention_policies table
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_data`(IN tableName CHAR(40), IN timestampColumn CHAR(40), IN policyName CHAR(50), IN periodName CHAR(20))
MODIFIES SQL DATA
BEGIN
    SET @currentTime = current_timestamp;
    SET @purge_sql=concat('DELETE FROM ',tableName,'
             where ',timestampColumn,'<(select date_sub("',@currentTime,'", interval retention_period ',periodName,')
             from retention_policies where policy_name="',policyName,'") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
END

$$

DELETE FROM `retention_policies` WHERE `policy_name` = 'percentile_retention_days';

$$

INSERT INTO `retention_policies` VALUES ('percentile_retention_days',90);

$$

DROP EVENT IF EXISTS `purge_expired_percentile_data`;

$$

-- Event to remove percentile blobs which are expired according to value configured
-- for retention_policies.retention_days
CREATE EVENT `purge_expired_percentile_data` ON
SCHEDULE EVERY 1 DAY STARTS addtime(curdate(), '1 00:00:00') ON
COMPLETION NOT PRESERVE ENABLE DO
BEGIN
    CALL `purge_expired_data`('percentile_blobs', 'start_timestamp', 'percentile_retention_days', 'day');
END

$$
DELIMITER ;
