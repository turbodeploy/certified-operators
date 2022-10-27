-- Drop foreign key if exists
DROP PROCEDURE IF EXISTS PROC_DROP_FOREIGN_KEY;
DELIMITER $$
CREATE PROCEDURE PROC_DROP_FOREIGN_KEY()
BEGIN
    IF EXISTS(
        SELECT * FROM information_schema.table_constraints
        WHERE
            table_schema    = database()     AND
            table_name      = 'entity_savings_by_day'      AND
            constraint_name = 'fk_entity_savings_by_day_entity_oid' AND
            constraint_type = 'FOREIGN KEY')
    THEN
        ALTER TABLE entity_savings_by_day DROP FOREIGN KEY fk_entity_savings_by_day_entity_oid;
    END IF;
    IF EXISTS(
        SELECT * FROM information_schema.table_constraints
        WHERE
            table_schema    = database()     AND
            table_name      = 'entity_savings_by_month'      AND
            constraint_name = 'fk_entity_savings_by_month_entity_oid' AND
            constraint_type = 'FOREIGN KEY')
    THEN
        ALTER TABLE entity_savings_by_month DROP FOREIGN KEY fk_entity_savings_by_month_entity_oid;
    END IF;
END$$
DELIMITER ;

CALL PROC_DROP_FOREIGN_KEY();
DROP PROCEDURE IF EXISTS PROC_DROP_FOREIGN_KEY;
