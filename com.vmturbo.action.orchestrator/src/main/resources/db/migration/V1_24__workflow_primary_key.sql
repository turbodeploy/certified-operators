-- Drop the foreign key and the unique index on id
SET FOREIGN_KEY_CHECKS = 0;

-- Drop foreign key if exists
DROP PROCEDURE IF EXISTS PROC_DROP_FOREIGN_KEY;
DELIMITER $$
CREATE PROCEDURE PROC_DROP_FOREIGN_KEY()
BEGIN
    IF EXISTS(
        SELECT * FROM information_schema.table_constraints
        WHERE
            table_schema    = database()     AND
            table_name      = 'workflow'      AND
            constraint_name = 'workflow_ibfk_1' AND
            constraint_type = 'FOREIGN KEY')
    THEN
        SET @query = CONCAT('ALTER TABLE workflow DROP FOREIGN KEY workflow_ibfk_1;');
        PREPARE stmt FROM @query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

CALL PROC_DROP_FOREIGN_KEY();
DROP PROCEDURE IF EXISTS PROC_DROP_FOREIGN_KEY;

-- there's no DROP INDEX IF EXISTS, so...
DROP PROCEDURE IF EXISTS _drop_index;
DELIMITER //
CREATE PROCEDURE _drop_index()
DROP_INDEX:BEGIN
    -- unfortunately, MySQL and MariaDB don't agree on either SQLSTATE or error code for
    -- trying to drop an index that doesn't exist, so need to be a little less targeted
    -- in the handler than would be ideal
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        SELECT CONCAT('Index not found so not dropped: ', @index, ' on workflow');
    SET @index='id';
    DROP INDEX id ON workflow;
END //
CALL _drop_index() //
DROP PROCEDURE _drop_index //
DELIMITER ;

SET FOREIGN_KEY_CHECKS = 1;

-- Make id the primary key
ALTER TABLE workflow ADD PRIMARY KEY (id);

-- Recreate foreign key constraint
DROP PROCEDURE IF EXISTS PROC_CREATE_FOREIGN_KEY;
DELIMITER $$
CREATE PROCEDURE PROC_CREATE_FOREIGN_KEY()
BEGIN
    IF NOT EXISTS(
        SELECT * FROM information_schema.table_constraints
        WHERE
            table_schema    = database()     AND
            table_name      = 'workflow'      AND
            constraint_name = 'workflow_ibfk_1' AND
            constraint_type = 'FOREIGN KEY')
    THEN
        SET @query = CONCAT('ALTER TABLE workflow ADD CONSTRAINT workflow_ibfk_1 FOREIGN KEY (id) REFERENCES workflow_oid(id) ON DELETE CASCADE;');
        PREPARE stmt FROM @query;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

CALL PROC_CREATE_FOREIGN_KEY();
DROP PROCEDURE IF EXISTS PROC_CREATE_FOREIGN_KEY;