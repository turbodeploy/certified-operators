DROP PROCEDURE IF EXISTS DROP_PRIMARY_KEY_V__75;
DELIMITER //
CREATE PROCEDURE DROP_PRIMARY_KEY_V__75(
        IN tableName VARCHAR(255)
    )
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
            SET @msg = (SELECT CONCAT("No Primary Key found in ", tableName, " table."));
            SELECT @msg as '';
        END;

        SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " DROP PRIMARY KEY"));
        PREPARE dropPrimaryKey FROM @preparedStatement;
        EXECUTE dropPrimaryKey;
        DEALLOCATE PREPARE dropPrimaryKey;
    END//
DELIMITER ;

TRUNCATE TABLE entity_cost;
call DROP_PRIMARY_KEY_V__75('entity_cost');
ALTER TABLE entity_cost ADD PRIMARY KEY (`created_time`, `associated_entity_id`, `cost_type`, `cost_source`);
CREATE INDEX idx_entity_cost_associated_entity_id on entity_cost(associated_entity_id);
ALTER TABLE entity_cost DROP INDEX ec_ct;

TRUNCATE TABLE reserved_instance_coverage_latest;
call DROP_PRIMARY_KEY_V__75('reserved_instance_coverage_latest');
ALTER TABLE reserved_instance_coverage_latest ADD PRIMARY KEY (`snapshot_time`, `entity_id`);
CREATE INDEX idx_reserved_instance_coverage_latest_entity_id on reserved_instance_coverage_latest(entity_id);
ALTER TABLE reserved_instance_coverage_latest DROP INDEX ricl_st;

TRUNCATE TABLE reserved_instance_utilization_latest;
call DROP_PRIMARY_KEY_V__75('reserved_instance_utilization_latest');
ALTER TABLE reserved_instance_utilization_latest ADD PRIMARY KEY (`snapshot_time`, `id`);
CREATE INDEX idx_reserved_instance_utilization_latest_id on reserved_instance_utilization_latest(id);
ALTER TABLE reserved_instance_utilization_latest DROP INDEX riul_st;

DROP PROCEDURE IF EXISTS DROP_PRIMARY_KEY_V__75;
