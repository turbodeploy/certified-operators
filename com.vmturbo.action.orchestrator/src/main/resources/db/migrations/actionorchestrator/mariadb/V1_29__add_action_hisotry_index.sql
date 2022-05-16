-- add index to `create_time` column for `action_history` table to speed up query

DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_V__29;

DELIMITER //
        CREATE PROCEDURE ADD_INDEX_IF_NOT_EXISTS_V__29(
              IN tableName VARCHAR(255),   -- name of table to add index to
              IN columnName VARCHAR(255),  -- name of column to add index on
              IN indexName VARCHAR(255)  -- name of index to add
          )
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
            BEGIN
                SET @msg = (SELECT CONCAT("Index ", indexName, " is already in place in table ", tableName, "."));
                SELECT @msg as '';
            END;

            SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " ADD INDEX ", indexName, "(", columnName ,");"));
            PREPARE alterIfNotExists FROM @preparedStatement;
            EXECUTE alterIfNotExists;
            DEALLOCATE PREPARE alterIfNotExists;
        END//

DELIMITER ;

CALL ADD_INDEX_IF_NOT_EXISTS_V__29('action_history', 'create_time', 'action_history_create_time_idx');
DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_V__29;