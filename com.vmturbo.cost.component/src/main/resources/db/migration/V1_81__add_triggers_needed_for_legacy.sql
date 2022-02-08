DELIMITER //

DROP TRIGGER IF EXISTS reserved_instance_utilization_keys //
CREATE TRIGGER reserved_instance_utilization_keys BEFORE INSERT ON reserved_instance_utilization_latest
  FOR EACH ROW
  BEGIN
  /* Set hour, day, and month keys for existing databases */
  SET NEW.hour_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.day_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.month_key=md5(concat(
      ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));
  END//

DROP TRIGGER IF EXISTS reserved_instance_coverage_keys //
CREATE TRIGGER reserved_instance_coverage_keys BEFORE INSERT ON reserved_instance_coverage_latest
  FOR EACH ROW
  BEGIN
  /* Set hour, day, and month keys for existing databases */
  SET NEW.hour_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d %H:00:00"),'-'),
      ifnull(NEW.entity_id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.day_key=md5(concat(
      ifnull(date_format(NEW.snapshot_time,"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.entity_id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));

  SET NEW.month_key=md5(concat(
      ifnull(date_format(last_day(NEW.snapshot_time),"%Y-%m-%d 00:00:00"),'-'),
      ifnull(NEW.entity_id,'-'),
      ifnull(NEW.region_id,'-'),
      ifnull(NEW.availability_zone_id,'-'),
      ifnull(NEW.business_account_id,'-')
  ));
  END//

DELIMITER ;