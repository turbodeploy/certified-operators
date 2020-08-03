--    Update "mkt_snapshots_stats" table to include an "entity_type" column

DROP PROCEDURE IF EXISTS _add_entity_type_column;

DELIMITER ;;
CREATE PROCEDURE _add_entity_type_column()
ADD_ENTITY_TYPE_COLUMN:BEGIN
    BEGIN
        -- Continue if the column already exists
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
            SELECT CONCAT('Duplicate column name ''entity_type''');

        -- Set default to 2047, because 2047 means UNKNOWN.
        -- For UNKNOWN rows, we will try to deduce the entity type based on the commodity type. This should
        --   mirror the legacy logic.
        ALTER TABLE mkt_snapshots_stats ADD entity_type SMALLINT NOT NULL DEFAULT 2047;
    END;
END ;;
DELIMITER ;

CALL _add_entity_type_column() ;
DROP PROCEDURE _add_entity_type_column ;