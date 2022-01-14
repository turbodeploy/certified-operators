--
-- Table structure for table bu_stats_latest
--

DROP TABLE IF EXISTS bu_stats_latest;
CREATE TABLE bu_stats_latest (
  snapshot_time datetime DEFAULT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  aggregated tinyint(1) NOT NULL DEFAULT '0',
  hour_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  day_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  month_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);

DELIMITER ;;
 CREATE  DEFINER=CURRENT_USER  TRIGGER set_bu_primary_keys BEFORE INSERT ON bu_stats_latest
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
END ;;
DELIMITER ;

--
-- Table structure for table bu_stats_by_hour
--

DROP TABLE IF EXISTS bu_stats_by_hour;
CREATE TABLE bu_stats_by_hour (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  aggregated tinyint(1) NOT NULL DEFAULT '0',
  new_samples int(11) DEFAULT NULL,
  hour_key varchar(32) CHARACTER SET utf8 NOT NULL,
  day_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  month_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (hour_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);


DROP TABLE IF EXISTS bu_stats_by_day;
CREATE TABLE bu_stats_by_day (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  aggregated tinyint(1) NOT NULL DEFAULT '0',
  new_samples int(11) DEFAULT NULL,
  day_key varchar(32) CHARACTER SET utf8 NOT NULL,
  month_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (day_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);


DROP TABLE IF EXISTS bu_stats_by_month;
CREATE TABLE bu_stats_by_month (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  new_samples int(11) DEFAULT NULL,
  month_key varchar(32) CHARACTER SET utf8 NOT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (month_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);

--
-- Table structure for table view_pod_stats_latest
--

DROP TABLE IF EXISTS view_pod_stats_latest;
CREATE TABLE view_pod_stats_latest (
  snapshot_time datetime DEFAULT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  aggregated tinyint(1) NOT NULL DEFAULT '0',
  hour_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  day_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  month_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);

DELIMITER ;;
 CREATE  DEFINER=CURRENT_USER  TRIGGER set_view_pod_primary_keys BEFORE INSERT ON view_pod_stats_latest
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
END ;;
DELIMITER ;

--
-- Table structure for table view_pod_stats_by_hour
--

DROP TABLE IF EXISTS view_pod_stats_by_hour;
CREATE TABLE view_pod_stats_by_hour (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  aggregated tinyint(1) NOT NULL DEFAULT '0',
  new_samples int(11) DEFAULT NULL,
  hour_key varchar(32) CHARACTER SET utf8 NOT NULL,
  day_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  month_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (hour_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);

DROP TABLE IF EXISTS view_pod_stats_by_day;
CREATE TABLE view_pod_stats_by_day (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  aggregated tinyint(1) NOT NULL DEFAULT '0',
  new_samples int(11) DEFAULT NULL,
  day_key varchar(32) CHARACTER SET utf8 NOT NULL,
  month_key varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (day_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);

DROP TABLE IF EXISTS view_pod_stats_by_month;
CREATE TABLE view_pod_stats_by_month (
  snapshot_time datetime NOT NULL,
  uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  producer_uuid varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_type varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  property_subtype varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  capacity decimal(15,3) DEFAULT NULL,
  avg_value decimal(15,3) DEFAULT NULL,
  min_value decimal(15,3) DEFAULT NULL,
  max_value decimal(15,3) DEFAULT NULL,
  relation tinyint(3) DEFAULT NULL,
  commodity_key varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  samples int(11) DEFAULT NULL,
  new_samples int(11) DEFAULT NULL,
  month_key varchar(32) CHARACTER SET utf8 NOT NULL,
  effective_capacity decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (month_key,snapshot_time),
  KEY snapshot_time (snapshot_time),
  KEY uuid (uuid),
  KEY property_type (property_type),
  KEY property_subtype (property_subtype)
)
 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION start VALUES LESS THAN (0),
PARTITION future VALUES LESS THAN MAXVALUE);

 DROP PROCEDURE IF EXISTS trigger_rotate_partition ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE trigger_rotate_partition()
  rotation_block: BEGIN

    SET @aggregation_in_progress = CHECKAGGR();
    IF @aggregation_in_progress > 0 THEN
      SELECT 'Aggregation is already running: skip rotation.' as '';
      LEAVE rotation_block;
    END IF;


    # market
    CALL rotate_partition('market_stats_latest');
    CALL rotate_partition('market_stats_by_day');
    CALL rotate_partition('market_stats_by_hour');
    CALL rotate_partition('market_stats_by_month');

    # app
    CALL rotate_partition('app_stats_latest');
    CALL rotate_partition('app_stats_by_day');
    CALL rotate_partition('app_stats_by_hour');
    CALL rotate_partition('app_stats_by_month');

    # ch
    CALL rotate_partition('ch_stats_latest');
    CALL rotate_partition('ch_stats_by_day');
    CALL rotate_partition('ch_stats_by_hour');
    CALL rotate_partition('ch_stats_by_month');

    # cnt
    CALL rotate_partition('cnt_stats_latest');
    CALL rotate_partition('cnt_stats_by_day');
    CALL rotate_partition('cnt_stats_by_hour');
    CALL rotate_partition('cnt_stats_by_month');

    # cpod
    CALL rotate_partition('cpod_stats_latest');
    CALL rotate_partition('cpod_stats_by_day');
    CALL rotate_partition('cpod_stats_by_hour');
    CALL rotate_partition('cpod_stats_by_month');

    # dpod
    CALL rotate_partition('dpod_stats_latest');
    CALL rotate_partition('dpod_stats_by_day');
    CALL rotate_partition('dpod_stats_by_hour');
    CALL rotate_partition('dpod_stats_by_month');

    # da
    CALL rotate_partition('da_stats_latest');
    CALL rotate_partition('da_stats_by_day');
    CALL rotate_partition('da_stats_by_hour');
    CALL rotate_partition('da_stats_by_month');

    # ds
    CALL rotate_partition('ds_stats_latest');
    CALL rotate_partition('ds_stats_by_day');
    CALL rotate_partition('ds_stats_by_hour');
    CALL rotate_partition('ds_stats_by_month');

    # iom
    CALL rotate_partition('iom_stats_latest');
    CALL rotate_partition('iom_stats_by_day');
    CALL rotate_partition('iom_stats_by_hour');
    CALL rotate_partition('iom_stats_by_month');

    # lp
    CALL rotate_partition('lp_stats_latest');
    CALL rotate_partition('lp_stats_by_day');
    CALL rotate_partition('lp_stats_by_hour');
    CALL rotate_partition('lp_stats_by_month');

    # pm
    CALL rotate_partition('pm_stats_latest');
    CALL rotate_partition('pm_stats_by_day');
    CALL rotate_partition('pm_stats_by_hour');
    CALL rotate_partition('pm_stats_by_month');

    # sc
    CALL rotate_partition('sc_stats_latest');
    CALL rotate_partition('sc_stats_by_day');
    CALL rotate_partition('sc_stats_by_hour');
    CALL rotate_partition('sc_stats_by_month');

    # sw
    CALL rotate_partition('sw_stats_latest');
    CALL rotate_partition('sw_stats_by_day');
    CALL rotate_partition('sw_stats_by_hour');
    CALL rotate_partition('sw_stats_by_month');

    # vdc
    CALL rotate_partition('vdc_stats_latest');
    CALL rotate_partition('vdc_stats_by_day');
    CALL rotate_partition('vdc_stats_by_hour');
    CALL rotate_partition('vdc_stats_by_month');

    # vm
    CALL rotate_partition('vm_stats_latest');
    CALL rotate_partition('vm_stats_by_day');
    CALL rotate_partition('vm_stats_by_hour');
    CALL rotate_partition('vm_stats_by_month');

    # vpod
    CALL rotate_partition('vpod_stats_latest');
    CALL rotate_partition('vpod_stats_by_day');
    CALL rotate_partition('vpod_stats_by_hour');
    CALL rotate_partition('vpod_stats_by_month');

    # bu
    CALL rotate_partition('bu_stats_latest');
    CALL rotate_partition('bu_stats_by_day');
    CALL rotate_partition('bu_stats_by_hour');
    CALL rotate_partition('bu_stats_by_month');
    # view_pod
    CALL rotate_partition('view_pod_stats_latest');
    CALL rotate_partition('view_pod_stats_by_day');
    CALL rotate_partition('view_pod_stats_by_hour');
    CALL rotate_partition('view_pod_stats_by_month');
  END ;;

 DROP EVENT IF EXISTS aggregate_stats_event ;;
 CREATE  DEFINER=CURRENT_USER  EVENT aggregate_stats_event ON SCHEDULE EVERY 10 MINUTE STARTS '2018-03-09 17:32:36' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call market_aggregate('market');
  call aggregate('app');
  call aggregate('ch');
  call aggregate('cnt');
  call aggregate('cpod');
  call aggregate('dpod');
  call aggregate('da');
  call aggregate('ds');
  call aggregate('iom');
  call aggregate('lp');
  call aggregate('pm');
  call aggregate('sc');
  call aggregate('sw');
  call aggregate('vdc');
  call aggregate('vm');
  call aggregate('bu');
  call aggregate('view_pod');
  call aggregate('vpod');

  CALL trigger_rotate_partition();
END  ;;
