-- Table structure for `account_expenses_retention_policies`, general to the cost component.
DROP TABLE IF EXISTS account_expenses_retention_policies;

CREATE TABLE account_expenses_retention_policies (
  policy_name varchar(50) DEFAULT NULL,
  retention_period int(11) DEFAULT NULL
);

INSERT INTO account_expenses_retention_policies VALUES ('retention_days',60),('retention_months',24);

DROP PROCEDURE IF EXISTS purge_expired_data;
DELIMITER $$
-- Common procedure to remove expired data according to retention policy,
-- described in account_expenses_retention_policies table
CREATE PROCEDURE purge_expired_data(IN tableName CHAR(40), IN dateColumn CHAR(40), IN policyName CHAR(50), IN periodName CHAR(20))
MODIFIES SQL DATA
BEGIN
    SET @currentDate = UTC_DATE();
    SET @purge_sql=concat('DELETE FROM ',tableName,'
             WHERE ',dateColumn,'< (SELECT date_sub("',@currentDate,'", INTERVAL retention_period ',periodName,')
                                         FROM account_expenses_retention_policies
                                         WHERE policy_name="',policyName,'") limit 1000');
    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
END $$
DELIMITER ;