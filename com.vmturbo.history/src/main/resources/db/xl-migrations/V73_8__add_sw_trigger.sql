/* add missing triggers to sw_stats_latest to create hour_key, day_key and month_key on insert */
/* Create sc trigger to create unique keys on insert.  The unique keys are based on a hash of all the columns concatenated */


DELIMITER //
 DROP TRIGGER IF EXISTS set_sw_primary_keys//
 CREATE TRIGGER set_sw_primary_keys BEFORE INSERT ON sw_stats_latest
 FOR EACH ROW BEGIN
      set NEW.hour_key=md5(concat(
        ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
        ifnull(NEW.uuid,'-'),
        ifnull(NEW.producer_uuid,'-'),
        ifnull(NEW.property_type,'-'),
        ifnull(NEW.property_subtype,'-'),
        ifnull(NEW.relation,'-'),
        ifnull(NEW.commodity_key,'-')
      ));  

      SET NEW.day_key=md5(concat(
        ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
        ifnull(NEW.uuid,'-'),
        ifnull(NEW.producer_uuid,'-'),
        ifnull(NEW.property_type,'-'),
        ifnull(NEW.property_subtype,'-'),
        ifnull(NEW.relation,'-'),
        ifnull(NEW.commodity_key,'-')
      ));  

      SET NEW.month_key=md5(concat(
        ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
        ifnull(NEW.uuid,'-'),
        ifnull(NEW.producer_uuid,'-'),
        ifnull(NEW.property_type,'-'),
        ifnull(NEW.property_subtype,'-'),
        ifnull(NEW.relation,'-'),
        ifnull(NEW.commodity_key,'-')
      ));  
END//
DELIMITER ;


/* Initialize hour_key, day_key, month_key for sw_ history tables where existing keys are null */

  update sw_stats_latest
  set hour_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )), day_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )), month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where hour_key is null;

  update sw_stats_by_hour
  set hour_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )), day_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )), month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where hour_key is null;

 update sw_stats_by_day
  set day_key=md5(concat(
    ifnull(date_format(snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )), month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where day_key is null;

  update sw_stats_by_month
  set month_key=md5(concat(
    ifnull(date_format(last_day(snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
    ifnull(uuid,'-'),
    ifnull(producer_uuid,'-'),
    ifnull(property_type,'-'),
    ifnull(property_subtype,'-'),
    ifnull(relation,'-'),
    ifnull(commodity_key,'-')
  )) where month_key is null;




-- modify existing checkaggr to include the creation of partitions in the check for aggregations occuring
--  add a function to compute the number of aggregation status tasks currently running
DELIMITER //
DROP FUNCTION IF EXISTS CHECKAGGR //
CREATE FUNCTION CHECKAGGR() RETURNS INTEGER
  BEGIN
    DECLARE AGGREGATING INTEGER DEFAULT 0;

    SELECT COUNT(*) INTO AGGREGATING FROM INFORMATION_SCHEMA.PROCESSLIST WHERE
      COMMAND != 'Sleep' AND INFO like '%aggreg%' and info not like '%INFORMATION_SCHEMA%' and info not like '%PARTITION%';
    RETURN AGGREGATING;
  END;//

DELIMITER ;



