DROP PROCEDURE IF EXISTS DROP_PRIMARY_KEY_V__34;

DELIMITER //
    CREATE PROCEDURE DROP_PRIMARY_KEY_V__34()
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            select "No Primary Key found in entity_cost table." as '';
        END;

        SET @preparedStatement = (SELECT('ALTER TABLE entity_cost DROP PRIMARY KEY'));
        PREPARE dropPrimaryKey FROM @preparedStatement;
        EXECUTE dropPrimaryKey;
        DEALLOCATE PREPARE dropPrimaryKey;
    END//

DELIMITER ;

call DROP_PRIMARY_KEY_V__34();
-- add primary key constraint to entity_cost table.
ALTER TABLE entity_cost ADD PRIMARY KEY (associated_entity_id, created_time, cost_type, cost_source);

-- Dropping temporary procedures
DROP PROCEDURE IF EXISTS DROP_PRIMARY_KEY_V__34;