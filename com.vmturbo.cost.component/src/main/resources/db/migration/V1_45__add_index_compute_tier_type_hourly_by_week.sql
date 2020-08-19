-- Add an index to the compute tier hourly by weekly table
DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_MIG_1_45;
DELIMITER //
CREATE PROCEDURE ADD_INDEX_IF_NOT_EXISTS_MIG_1_45(
        IN tableName VARCHAR(255),   -- name of table to add index to
        IN columnName VARCHAR(255),  -- name of column to add index on
        IN indexName VARCHAR(255)  -- name of index to add
    )
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SET @msg = (SELECT CONCAT("Index ", indexName, " is already in place in table ", tableName, "."));
          SELECT @msg as '';
      END;

      SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " ADD INDEX ", indexName, "(", columnName ,");"));
      PREPARE alterIfNotExists FROM @preparedStatement;
EXECUTE alterIfNotExists;
DEALLOCATE PREPARE alterIfNotExists;
END//
DELIMITER ;

CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_45('compute_tier_type_hourly_by_week', 'account_id', 'ctthbw_acctid');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_45('compute_tier_type_hourly_by_week', 'compute_tier_id', 'ctthbw_computetierid');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_45('compute_tier_type_hourly_by_week', 'region_or_zone_id', 'ctthbw_regionid');
