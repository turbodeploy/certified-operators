/**
 * This script does a number of things:
 * 1. it adds account_id, region_id, availability_zone_id to entity_cost,
 *    entity_cost_by_hour, entity_cost_by_day, and entity_cost_by_month tables. This columes
 *    respectively keep the account, region, availability zone associated with the entity
 *    that we keep the cost for. This change also define an index on these columns.
 * Note: there has been some changes in this script to make it idempotent. Previously, this
 * migration script was not idempotent. Therefore, it could result in migration failure. The file
 * V1_26__Callback is responsible for fixing the flyway checksum as a the result of this change.
 */

DROP PROCEDURE IF EXISTS ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26;
DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_MIG_1_26;
DROP PROCEDURE IF EXISTS DROP_COLUMN_IF_EXISTS_MIG_1_26;

DELIMITER //
CREATE PROCEDURE ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26(
        IN tableName VARCHAR(255),   -- name of table to add column to
        IN columnName VARCHAR(255)  -- name of column to add
    )
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SET @msg = (SELECT CONCAT("Column ", columnName, " is in place in table ", tableName, "."));
          SELECT @msg as '';
      END;

      SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " ADD COLUMN ", columnName," bigint(20) DEFAULT NULL;"));
      PREPARE alterIfNotExists FROM @preparedStatement;
      EXECUTE alterIfNotExists;
      DEALLOCATE PREPARE alterIfNotExists;
  END//

CREATE PROCEDURE ADD_INDEX_IF_NOT_EXISTS_MIG_1_26(
        IN tableName VARCHAR(255),   -- name of table to add index to
        IN columnName VARCHAR(255),  -- name of column to add index on
        IN indexName VARCHAR(255)  -- name of index to add
    )
  BEGIN
      DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SET @msg = (SELECT CONCAT("Index ", indexName, " is already in place in table ", tableName, "."));
          SELECT @msg as '';
      END;

      SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " ADD INDEX ", indexName, "(", columnName ,");"));
      PREPARE alterIfNotExists FROM @preparedStatement;
      EXECUTE alterIfNotExists;
      DEALLOCATE PREPARE alterIfNotExists;
  END//

CREATE PROCEDURE DROP_COLUMN_IF_EXISTS_MIG_1_26(
        IN tableName VARCHAR(255),   -- name of table to add column to
        IN columnName VARCHAR(255)  -- name of column to add
    )
  BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION BEGIN
          SET @msg = (SELECT CONCAT("Column ", columnName, " has been already removed from table ", tableName, "."));
          SELECT @msg as '';
      END;

      SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " DROP COLUMN ", columnName, ";"));
      PREPARE alterIfNotExists FROM @preparedStatement;
      EXECUTE alterIfNotExists;
      DEALLOCATE PREPARE alterIfNotExists;
  END//

DELIMITER ;

-- add columns to entity_cost table
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost', 'account_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost', 'region_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost', 'availability_zone_id');

-- add columns to entity_cost_by_hour table
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_hour', 'account_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_hour', 'region_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_hour', 'availability_zone_id');

-- add columns to entity_cost_by_day table
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_day', 'account_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_day', 'region_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_day', 'availability_zone_id');

-- add columns to entity_cost_by_month table
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_month', 'account_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_month', 'region_id');
CALL ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_month', 'availability_zone_id');

-- add indices for newly added columns to entity_cost
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost', 'account_id', 'entity_cost_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost', 'region_id', 'entity_cost_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost', 'availability_zone_id', 'entity_cost_availability_zone_id_index');

-- add indices for newly added columns to entity_cost_by_hour
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_hour', 'account_id', 'entity_cost_by_hour_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_hour', 'region_id', 'entity_cost_by_hour_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_hour', 'availability_zone_id', 'entity_cost_by_hour_availability_zone_id_index');

-- add indices for newly added columns to entity_cost_by_day
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_day', 'account_id', 'entity_cost_by_day_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_day', 'region_id', 'entity_cost_by_day_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_day', 'availability_zone_id', 'entity_cost_by_day_availability_zone_id_index');

-- add indices for newly added columns to entity_cost_by_month
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_month', 'account_id', 'entity_cost_by_month_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_month', 'region_id', 'entity_cost_by_month_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_MIG_1_26('entity_cost_by_month', 'availability_zone_id', 'entity_cost_by_month_availability_zone_id_index');

/** We drop the trigger that sets the hour_key, day_key, month_key as the
 * rollup process can work without them. We also drop those columns
 */
DROP TRIGGER IF EXISTS entity_cost_keys;

-- Drop the columns that no longer are being used.
CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost', 'hour_key');
CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost', 'day_key');
CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost', 'month_key');

CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost_by_hour', 'hour_key');
CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost_by_hour', 'day_key');
CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost_by_hour', 'month_key');

CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost_by_day', 'day_key');
CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost_by_day', 'month_key');

CALL DROP_COLUMN_IF_EXISTS_MIG_1_26('entity_cost_by_month', 'month_key');

/**
 * When not default value sets for the first timestamp column
 * of a table the default value will be set as CURRENT_TIME ON UPDATE CURRENT TIME
 * which is undesirable for our case. Therefore, we set a default value for
 * create_time of entity_cost_by_hour, entity_cost_by_day, and entity_cost_by_month
 * table.
 */
ALTER TABLE entity_cost_by_hour ALTER created_time SET DEFAULT '0000-00-00 00:00:00';
ALTER TABLE entity_cost_by_day ALTER created_time SET DEFAULT '0000-00-00 00:00:00';
ALTER TABLE entity_cost_by_month ALTER created_time SET DEFAULT '0000-00-00 00:00:00';

-- Dropping temporary procedures
DROP PROCEDURE IF EXISTS ADD_COLUMN_IF_NOT_EXISTS_MIG_1_26;
DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_MIG_1_26;
DROP PROCEDURE IF EXISTS DROP_COLUMN_IF_EXISTS_MIG_1_26;