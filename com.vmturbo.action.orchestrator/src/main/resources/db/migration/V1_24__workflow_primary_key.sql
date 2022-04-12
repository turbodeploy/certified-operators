-- Drop the unique index on id
SET FOREIGN_KEY_CHECKS = 0;
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
