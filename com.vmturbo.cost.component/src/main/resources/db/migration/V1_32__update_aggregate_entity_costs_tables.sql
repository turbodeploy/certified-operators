select 'Updating aggregate entity cost tables. This might take couple of minutes depending on amount of data.' AS '';

DROP PROCEDURE IF EXISTS CREATE_AND_REPLACE_TABLE_V__32;
DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_V__32;

DELIMITER //
        CREATE PROCEDURE CREATE_AND_REPLACE_TABLE_V__32(
              IN original_table VARCHAR(255)   -- name of table.
          )
        BEGIN
         DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
         SELECT "Was unable to create and replace aggregate tables. Please check cost DB for inconsistencies." as '';
        END;
        SET @tableName = (SELECT CONCAT(original_table,"_temp"));
        # Dropping temporary table if exists.
        SET @preparedStatement = (SELECT CONCAT("DROP TABLE IF EXISTS ", @tableName));
                PREPARE dropTable FROM @preparedStatement;
                EXECUTE dropTable;
                DEALLOCATE PREPARE dropTable;
        SET @preparedStatement = (SELECT CONCAT('CREATE TABLE ', @tableName,' (select * from ', original_table ,'
                                  group by created_time, associated_entity_id,
                                  associated_entity_type, cost_type,
                                  account_id, currency, region_id, availability_zone_id );'));
                PREPARE createTable FROM @preparedStatement;
                EXECUTE createTable;
                DEALLOCATE PREPARE createTable;
        SET @preparedStatement = (SELECT CONCAT('DROP TABLE IF EXISTS ', original_table));
                PREPARE dropTable FROM @preparedStatement;
                EXECUTE dropTable;
                DEALLOCATE PREPARE dropTable;
        SET @preparedStatement = (SELECT CONCAT('RENAME TABLE ', @tableName, ' to ', original_table));
                PREPARE renameTable FROM @preparedStatement;
                EXECUTE renameTable;
                DEALLOCATE PREPARE renameTable;
        END//

        CREATE PROCEDURE ADD_INDEX_IF_NOT_EXISTS_V__32(
              IN tableName VARCHAR(255),   -- name of table to add index to
              IN columnName VARCHAR(255),  -- name of column to add index on
              IN indexName VARCHAR(255)  -- name of index to add
          )
        BEGIN
            DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
            BEGIN
                SET @msg = (SELECT CONCAT("Index ", indexName, " is already in place in table ", tableName, "."));
                SELECT @msg as '';
            END;

            SET @preparedStatement = (SELECT CONCAT("ALTER TABLE ", tableName, " ADD INDEX ", indexName, "(", columnName ,");"));
            PREPARE alterIfNotExists FROM @preparedStatement;
            EXECUTE alterIfNotExists;
            DEALLOCATE PREPARE alterIfNotExists;
        END//

DELIMITER ;

ALTER TABLE entity_cost_by_day MODIFY COLUMN created_time TIMESTAMP NOT NULL DEFAULT 0;
ALTER TABLE entity_cost_by_hour MODIFY COLUMN created_time TIMESTAMP NOT NULL DEFAULT 0;
ALTER TABLE entity_cost_by_month MODIFY COLUMN created_time TIMESTAMP NOT NULL DEFAULT 0;

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#----------------------------------------------------Update entity_cost_by_day-----------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select 'Updating entity_cost_by_day.' AS '';

CALL CREATE_AND_REPLACE_TABLE_V__32('entity_cost_by_day');

ALTER TABLE entity_cost_by_day  ALTER COLUMN  availability_zone_id  SET DEFAULT 0;
ALTER TABLE entity_cost_by_day  ALTER COLUMN  account_id  SET DEFAULT 0;
ALTER TABLE entity_cost_by_day  ALTER COLUMN  region_id  SET DEFAULT 0;

alter table entity_cost_by_day add unique `unique_index` (created_time, associated_entity_id, associated_entity_type, cost_type, currency, account_id,region_id,availability_zone_id);

CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_day', 'created_time', 'ech_ct');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_day', 'account_id', 'entity_cost_by_day_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_day', 'region_id', 'entity_cost_by_day_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_day', 'availability_zone_id', 'entity_cost_by_day_availability_zone_id_index');

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#----------------------------------------------------Update entity_cost_by_hour----------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select 'Updating entity_cost_by_hour.' AS '';

CALL CREATE_AND_REPLACE_TABLE_V__32('entity_cost_by_hour');
ALTER TABLE entity_cost_by_hour  ALTER COLUMN  availability_zone_id  SET DEFAULT 0;
ALTER TABLE entity_cost_by_hour  ALTER COLUMN  account_id  SET DEFAULT 0;
ALTER TABLE entity_cost_by_hour  ALTER COLUMN  region_id  SET DEFAULT 0;


alter table entity_cost_by_hour add unique `unique_index` (created_time, associated_entity_id, associated_entity_type, cost_type, currency, account_id,region_id,availability_zone_id);

-- add indices for newly added columns to entity_cost
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_hour', 'created_time', 'ech_ct');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_hour', 'account_id', 'entity_cost_by_hour_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_hour', 'region_id', 'entity_cost_by_hour_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_hour', 'availability_zone_id', 'entity_cost_by_hour_availability_zone_id_index');

#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
#----------------------------------------------------Update entity_cost_by_month----------------------------------------------------
#++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

select 'Updating entity_cost_by_month.' AS '';

CALL CREATE_AND_REPLACE_TABLE_V__32('entity_cost_by_month');

ALTER TABLE entity_cost_by_month  ALTER COLUMN  availability_zone_id  SET DEFAULT 0;
ALTER TABLE entity_cost_by_month  ALTER COLUMN  account_id  SET DEFAULT 0;
ALTER TABLE entity_cost_by_month  ALTER COLUMN  region_id  SET DEFAULT 0;

alter table entity_cost_by_month add unique `unique_index` (created_time, associated_entity_id, associated_entity_type, cost_type, currency, account_id,region_id,availability_zone_id);

-- add indices for newly added columns to entity_cost
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_month', 'created_time', 'ech_ct');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_month', 'account_id', 'entity_cost_by_month_account_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_month', 'region_id', 'entity_cost_by_month_region_id_index');
CALL ADD_INDEX_IF_NOT_EXISTS_V__32('entity_cost_by_month', 'availability_zone_id', 'entity_cost_by_month_availability_zone_id_index');

-- add primary key constraint to entity_cost table.
ALTER TABLE entity_cost ADD PRIMARY KEY (associated_entity_id, created_time, cost_type, cost_source);

-- Dropping temporary procedures
DROP PROCEDURE IF EXISTS CREATE_AND_REPLACE_TABLE_V__32;
DROP PROCEDURE IF EXISTS ADD_INDEX_IF_NOT_EXISTS_V__32;

select 'Update of aggregate entity cost tables was successful.' AS '';