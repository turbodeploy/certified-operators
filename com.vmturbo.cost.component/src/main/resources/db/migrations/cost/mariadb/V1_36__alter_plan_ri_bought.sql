-- This table stores all bought reserved instance information for the plan.
CREATE TABLE IF NOT EXISTS plan_reserved_instance_bought (
    -- The id of reserved instance.
    id                                 BIGINT          NOT NULL,

    -- The id of plan topology context.
    plan_id                            BIGINT          NOT NULL,

    -- The id of reserved instance spec which this reserved instance referring to.
    reserved_instance_spec_id          BIGINT          NOT NULL,

    -- The serialized protobuf object contains detailed information about the reserved instance.
    reserved_instance_bought_info      BLOB            NOT NULL,

    -- Add one count column to reserved instance bought table, it means the number of instances bought
    -- of the reserved instance
    count                              INT             NOT NULL,

-- The foreign key referring to reserved instance spec table.
    FOREIGN  KEY (reserved_instance_spec_id) REFERENCES reserved_instance_spec(id) ON DELETE CASCADE,

    PRIMARY KEY (id)
);

DROP PROCEDURE IF EXISTS `DROP_COLUMN_V__36`;
DROP PROCEDURE IF EXISTS `DROP_PRIMARY_KEY_V__36`;
DELIMITER //
    CREATE PROCEDURE DROP_COLUMN_V__36()
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT CONCAT('Failed to ', @operation, ' on table ', @table);
        END;
        SET @table='plan_reserved_instance_bought';
        SET @operation='add `per_instance_fixed_cost` column';
        ALTER TABLE plan_reserved_instance_bought ADD COLUMN per_instance_fixed_cost DOUBLE;
        SET @operation='add `per_instance_recurring_cost_hourly` column';
        ALTER TABLE plan_reserved_instance_bought ADD COLUMN per_instance_recurring_cost_hourly DOUBLE;
        SET @operation='add `per_instance_amortized_cost_hourly` column';
        ALTER TABLE plan_reserved_instance_bought ADD COLUMN per_instance_amortized_cost_hourly DOUBLE;
    END//

    CREATE PROCEDURE DROP_PRIMARY_KEY_V__36()
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            select "No Primary Key found in plan_reserved_instance_bought bought table." as '';
        END;

        SET @preparedStatement = (SELECT('ALTER TABLE plan_reserved_instance_bought DROP PRIMARY KEY'));
        PREPARE dropPrimaryKey FROM @preparedStatement;
        EXECUTE dropPrimaryKey;
        DEALLOCATE PREPARE dropPrimaryKey;
    END//
DELIMITER ;

call DROP_COLUMN_V__36();
call DROP_PRIMARY_KEY_V__36();

-- Add plan_id to primary key for plan_reserved_instance_bought table
ALTER TABLE plan_reserved_instance_bought ADD PRIMARY KEY (id, plan_id);

DROP PROCEDURE IF EXISTS `DROP_COLUMN_V__36`;
DROP PROCEDURE IF EXISTS `DROP_PRIMARY_KEY_V__36`;

