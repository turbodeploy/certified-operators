DELIMITER $$

DROP PROCEDURE IF EXISTS `CreateIndex` $$
CREATE PROCEDURE Createindex
(
    given_table    VARCHAR(64) CHARACTER SET utf8mb4,
    given_index    VARCHAR(64) CHARACTER SET utf8mb4,
    given_columns  VARCHAR(64) CHARACTER SET utf8mb4
)
BEGIN

    DECLARE IndexIsThere INTEGER;

    SELECT COUNT(1) INTO IndexIsThere
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE table_name   = given_table
    AND   index_name   = given_index;

    IF IndexIsThere = 0 THEN
        SET @sqlstmt = CONCAT('CREATE INDEX ',given_index,' ON ',given_table,' (',given_columns,')');
        PREPARE st FROM @sqlstmt;
        EXECUTE st;
DEALLOCATE PREPARE st;
ELSE
SELECT CONCAT('Index ',given_index,' already exists on Table ',given_table) CreateindexErrorMessage;
END IF;

END $$

DELIMITER ;

call createindex('assigned_identity','idx_expired','expired');
