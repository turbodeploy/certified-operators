--    Update "hist_utilization" table:
--        - UNIQUE Constraint was added to the hist_utilization table for fields: oid, producer_oid, property_type_id, commodity_key, value_type, property_slot
--
--    See OM-49200 for details.

DELIMITER ;;
DROP INDEX IF EXISTS `hist_util_index` ON hist_utilization;
;;
ALTER TABLE hist_utilization ADD CONSTRAINT UC_Utilization PRIMARY KEY (`oid`,`producer_oid`, `property_type_id`, `property_subtype_id`, `value_type`, `property_slot`, `commodity_key`);
;;
DELIMITER ;
