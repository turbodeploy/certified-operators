-- This add the start and expiry columns to RI bought table.
-- This columns needs so we can more efficiently query for not expired RIs.
-- This also deletes the content of those tables. This should be fine as
-- their contents will be replaced with correct rows in the next broadcast cycle.

-- empty the content of tables
DELETE FROM reserved_instance_bought;
DELETE FROM entity_to_reserved_instance_mapping;

DROP PROCEDURE IF EXISTS `ADD_COLUMN_V__33`;
-- add new columns to tables
DELIMITER //
    CREATE PROCEDURE ADD_COLUMN_V__33()
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT CONCAT('Failed to ', @operation, ' on table ', @table);
        END;
        SET @table='reserved_instance_bought';
        SET @operation='add `start_time` column';
        ALTER TABLE reserved_instance_bought ADD COLUMN start_time timestamp NOT NULL DEFAULT 0;
        SET @operation='add `expiry_time` column';
        ALTER TABLE reserved_instance_bought ADD COLUMN expiry_time timestamp NOT NULL DEFAULT 0;
    END//
DELIMITER ;

call ADD_COLUMN_V__33();
DROP PROCEDURE IF EXISTS `ADD_COLUMN_V__33`;
