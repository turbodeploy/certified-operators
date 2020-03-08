--    Update "hist_utilization" table:
--        - PRIMARY Constraint was added to the hist_utilization table for fields: oid, producer_oid, property_type_id, commodity_key, value_type, property_slot
--
--    See OM-49200 for details.

DROP PROCEDURE IF EXISTS _change_hist_indexes;
DELIMITER ;;
CREATE PROCEDURE _change_hist_indexes()
CHANGE_HIST_INDEXES:BEGIN
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
            SELECT CONCAT('Index not found so not dropped: ', @index, ' on hist_utilization');
        SET @index='hist_util_index';
        DROP INDEX hist_util_index ON hist_utilization;
        SET @index='PRIMARY';
        DROP INDEX `PRIMARY` ON hist_utilization;
    END;

    ALTER TABLE hist_utilization ADD CONSTRAINT PK_Utilization PRIMARY KEY (`oid`,`producer_oid`, `property_type_id`, `property_subtype_id`, `value_type`, `property_slot`, `commodity_key`);
END ;;
CALL _change_hist_indexes() ;;
DROP PROCEDURE _change_hist_indexes ;;
