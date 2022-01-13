DELIMITER $$

DROP PROCEDURE IF EXISTS `DropIndex` $$
CREATE PROCEDURE DropIndex
(
    given_table    VARCHAR(64) CHARACTER SET utf8mb4,
    given_index    VARCHAR(64) CHARACTER SET utf8mb4
)
BEGIN

    DECLARE IndexIsThere INTEGER;

    SELECT COUNT(1) INTO IndexIsThere
    FROM INFORMATION_SCHEMA.STATISTICS
    WHERE table_name   = given_table
    AND   index_name   = given_index;

    IF IndexIsThere != 0 THEN
        SET @sqlstmt = CONCAT('DROP INDEX ',given_index,' ON ',given_table);
        PREPARE st FROM @sqlstmt;
        EXECUTE st;
DEALLOCATE PREPARE st;
ELSE
SELECT CONCAT('Index ',given_index,' does not exists on Table ',given_table) CreateindexErrorMessage;
END IF;

END $$

DELIMITER ;
call dropindex('grouping', 'idx_grouping_display_name');
call dropindex('grouping', 'idx_grouping_disc_src_id');

-- Convert the character set from utf8 to utf8mb4
ALTER TABLE grouping CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

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

call createindex('grouping','idx_grouping_display_name','display_name(191)');
call createindex('grouping','idx_grouping_disc_src_id','origin_discovered_src_id(191)');

DROP PROCEDURE IF EXISTS `CreateIndex`;
DROP PROCEDURE IF EXISTS `DropIndex`;
