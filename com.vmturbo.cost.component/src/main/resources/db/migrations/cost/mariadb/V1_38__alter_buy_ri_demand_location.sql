-- Alter the location column in the buy RI demand table from availability zone to region_or_zone to
-- reflect storage of Azure demand.
DROP PROCEDURE IF EXISTS `CHANGE_COLUMN_V__38`;
-- add new columns to tables
DELIMITER //
    CREATE PROCEDURE CHANGE_COLUMN_V__38()
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        BEGIN
            SELECT CONCAT('Failed to ', @operation, ' on table ', @table);
        END;
        SET @table='compute_tier_type_hourly_by_week';
        SET @operation='change `availability_zone to region_or_zone` column';
        ALTER TABLE compute_tier_type_hourly_by_week CHANGE COLUMN availability_zone region_or_zone_id BIGINT NOT NULL;
    END//
DELIMITER ;

call CHANGE_COLUMN_V__38();
DROP PROCEDURE IF EXISTS `CHANGE_COLUMN_V__38`;
