-- MySQL dump 10.16  Distrib 10.2.13-MariaDB, for osx10.13 (x86_64)
--
-- Host: localhost    Database: vmtdb
-- ------------------------------------------------------
-- Server version	10.1.30-MariaDB-0ubuntu0.17.10.1

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Table structure for table `action_modes`
--

DROP TABLE IF EXISTS `action_modes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_modes` (
  `id` tinyint(3) NOT NULL,
  `name` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `action_modes`
--

LOCK TABLES `action_modes` WRITE;
/*!40000 ALTER TABLE `action_modes` DISABLE KEYS */;
INSERT INTO `action_modes` VALUES (0,'DISABLED'),(1,'RECOMMEND'),(2,'MANUAL'),(3,'AUTOMATIC'),(4,'COLLECTION');
/*!40000 ALTER TABLE `action_modes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `action_savings_unit`
--

DROP TABLE IF EXISTS `action_savings_unit`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_savings_unit` (
  `id` tinyint(3) NOT NULL,
  `name` varchar(25) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `action_savings_unit`
--

LOCK TABLES `action_savings_unit` WRITE;
/*!40000 ALTER TABLE `action_savings_unit` DISABLE KEYS */;
INSERT INTO `action_savings_unit` VALUES (0,'DOLLARS'),(1,'DOLLARS_PER_HOUR'),(2,'DOLLARS_PER_DAY'),(3,'DOLLARS_PER_MONTH');
/*!40000 ALTER TABLE `action_savings_unit` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `action_states`
--

DROP TABLE IF EXISTS `action_states`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_states` (
  `id` tinyint(3) NOT NULL,
  `name` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `action_states`
--

LOCK TABLES `action_states` WRITE;
/*!40000 ALTER TABLE `action_states` DISABLE KEYS */;
INSERT INTO `action_states` VALUES (0,'PENDING_ACCEPT'),(1,'ACCEPTED'),(2,'REJECTED'),(3,'IN_PROGRESS'),(4,'SUCCEEDED'),(5,'FAILED'),(6,'RECOMMENDED'),(7,'DISABLED'),(8,'QUEUED'),(9,'CLEARED');
/*!40000 ALTER TABLE `action_states` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `action_types`
--

DROP TABLE IF EXISTS `action_types`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `action_types` (
  `id` tinyint(3) NOT NULL,
  `name` varchar(25) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `action_types`
--

LOCK TABLES `action_types` WRITE;
/*!40000 ALTER TABLE `action_types` DISABLE KEYS */;
INSERT INTO `action_types` VALUES (0,'NONE'),(1,'START'),(2,'MOVE'),(3,'SUSPEND'),(4,'TERMINATE'),(5,'SPAWN'),(6,'ADD_PROVIDER'),(7,'CHANGE'),(8,'REMOVE_PROVIDER'),(9,'PROVISION'),(10,'RECONFIGURE'),(11,'RESIZE'),(12,'RESIZE_CAPACITY'),(13,'WARN'),(14,'RECONFIGURE_THRESHOLD'),(15,'DELETE'),(16,'RIGHT_SIZE'),(17,'RESERVE_ON_PM'),(18,'RESERVE_ON_DS'),(19,'RESIZE_FOR_EFFICIENCY'),(20,'RESIZE_FOR_PERFORMANCE'),(21,'CROSS_TARGET_MOVE'),(22,'MOVE_TOGETHER'),(23,'RESERVE_ON_DA');
/*!40000 ALTER TABLE `action_types` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `actions`
--

DROP TABLE IF EXISTS `actions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `actions` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `clear_time` datetime DEFAULT NULL,
  `action_type` tinyint(3) DEFAULT NULL,
  `action_state` tinyint(3) DEFAULT NULL,
  `action_mode` tinyint(3) DEFAULT NULL,
  `user_name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target_object_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `current` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `new` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `details` mediumtext COLLATE utf8_unicode_ci,
  `notification_id` bigint(20) DEFAULT NULL,
  `action_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `savings` decimal(20,7) DEFAULT NULL,
  `savings_unit` tinyint(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `action_type` (`action_type`),
  KEY `action_state` (`action_state`),
  KEY `action_mode` (`action_mode`),
  KEY `actions_ibfk_1` (`notification_id`),
  KEY `savings_unit` (`savings_unit`),
  KEY `create_time_idx` (`create_time`) USING BTREE,
  KEY `target_object_uuid_idx` (`target_object_uuid`),
  KEY `current_idx` (`current`),
  KEY `new_idx` (`new`),
  KEY `action_uuid_idx` (`action_uuid`),
  CONSTRAINT `actions_ibfk_1` FOREIGN KEY (`notification_id`) REFERENCES `notifications` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_ibfk_2` FOREIGN KEY (`action_type`) REFERENCES `action_types` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_ibfk_3` FOREIGN KEY (`action_state`) REFERENCES `action_states` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_ibfk_4` FOREIGN KEY (`action_mode`) REFERENCES `action_modes` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_ibfk_5` FOREIGN KEY (`savings_unit`) REFERENCES `action_savings_unit` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `actions`
--

LOCK TABLES `actions` WRITE;
/*!40000 ALTER TABLE `actions` DISABLE KEYS */;
/*!40000 ALTER TABLE `actions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `actions_OLD`
--

DROP TABLE IF EXISTS `actions_OLD`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `actions_OLD` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `clear_time` datetime DEFAULT NULL,
  `action_type` tinyint(3) DEFAULT NULL,
  `action_state` tinyint(3) DEFAULT NULL,
  `action_mode` tinyint(3) DEFAULT NULL,
  `user_name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target_object_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `current` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `new` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `details` mediumtext COLLATE utf8_unicode_ci,
  `notification_id` bigint(20) DEFAULT NULL,
  `action_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `action_type` (`action_type`),
  KEY `action_state` (`action_state`),
  KEY `action_mode` (`action_mode`),
  KEY `actions_ibfk_1` (`notification_id`),
  CONSTRAINT `actions_OLD_ibfk_1` FOREIGN KEY (`notification_id`) REFERENCES `notifications` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_OLD_ibfk_2` FOREIGN KEY (`action_type`) REFERENCES `action_types` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_OLD_ibfk_3` FOREIGN KEY (`action_state`) REFERENCES `action_states` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE,
  CONSTRAINT `actions_OLD_ibfk_4` FOREIGN KEY (`action_mode`) REFERENCES `action_modes` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=937388746321 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `actions_OLD`
--

LOCK TABLES `actions_OLD` WRITE;
/*!40000 ALTER TABLE `actions_OLD` DISABLE KEYS */;
/*!40000 ALTER TABLE `actions_OLD` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `actions_stats_by_day`
--

DROP TABLE IF EXISTS `actions_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `actions_stats_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `scope_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `related_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `count` int(11) DEFAULT NULL,
  `savings` decimal(20,7) DEFAULT NULL,
  `n_entities` int(5) DEFAULT NULL,
  `investment` decimal(20,7) DEFAULT NULL,
  `savings_unit` tinyint(3) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`scope_type`,`uuid`,`provider_uuid`,`property_type`,`property_subtype`),
  KEY `savings_unit` (`savings_unit`),
  CONSTRAINT `actions_stats_by_day_ibfk_1` FOREIGN KEY (`savings_unit`) REFERENCES `action_savings_unit` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `actions_stats_by_day`
--

LOCK TABLES `actions_stats_by_day` WRITE;
/*!40000 ALTER TABLE `actions_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `actions_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `actions_stats_by_hour`
--

DROP TABLE IF EXISTS `actions_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `actions_stats_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `scope_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `related_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `count` int(11) DEFAULT NULL,
  `savings` decimal(20,7) DEFAULT NULL,
  `n_entities` int(5) DEFAULT NULL,
  `investment` decimal(20,7) DEFAULT NULL,
  `savings_unit` tinyint(3) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`scope_type`,`uuid`,`provider_uuid`,`property_type`,`property_subtype`),
  KEY `savings_unit` (`savings_unit`),
  CONSTRAINT `actions_stats_by_hour_ibfk_1` FOREIGN KEY (`savings_unit`) REFERENCES `action_savings_unit` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `actions_stats_by_hour`
--

LOCK TABLES `actions_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `actions_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `actions_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `actions_stats_by_month`
--

DROP TABLE IF EXISTS `actions_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `actions_stats_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `scope_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `related_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `count` int(11) DEFAULT NULL,
  `savings` decimal(20,7) DEFAULT NULL,
  `n_entities` int(5) DEFAULT NULL,
  `investment` decimal(20,7) DEFAULT NULL,
  `savings_unit` tinyint(3) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`scope_type`,`uuid`,`provider_uuid`,`property_type`,`property_subtype`),
  KEY `savings_unit` (`savings_unit`),
  CONSTRAINT `actions_stats_by_month_ibfk_1` FOREIGN KEY (`savings_unit`) REFERENCES `action_savings_unit` (`id`) ON DELETE NO ACTION ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `actions_stats_by_month`
--

LOCK TABLES `actions_stats_by_month` WRITE;
/*!40000 ALTER TABLE `actions_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `actions_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `active_vms`
--

DROP TABLE IF EXISTS `active_vms`;
/*!50001 DROP VIEW IF EXISTS `active_vms`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `active_vms` (
  `uuid` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `aggregation_status`
--

DROP TABLE IF EXISTS `aggregation_status`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `aggregation_status` (
  `status` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `aggregation_status`
--

LOCK TABLES `aggregation_status` WRITE;
/*!40000 ALTER TABLE `aggregation_status` DISABLE KEYS */;
/*!40000 ALTER TABLE `aggregation_status` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `app_daily_ins_vw`
--

DROP TABLE IF EXISTS `app_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `app_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `app_daily_upd_vw`
--

DROP TABLE IF EXISTS `app_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `app_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `app_groups`
--

DROP TABLE IF EXISTS `app_groups`;
/*!50001 DROP VIEW IF EXISTS `app_groups`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_groups` (
  `entity_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `app_hourly_ins_vw`
--

DROP TABLE IF EXISTS `app_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `app_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `app_hourly_upd_vw`
--

DROP TABLE IF EXISTS `app_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `app_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `app_monthly_ins_vw`
--

DROP TABLE IF EXISTS `app_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `app_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `app_monthly_upd_vw`
--

DROP TABLE IF EXISTS `app_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `app_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `app_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `app_spend_by_day`
--

DROP TABLE IF EXISTS `app_spend_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_spend_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hash` bigint(20) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_spend_by_day`
--

LOCK TABLES `app_spend_by_day` WRITE;
/*!40000 ALTER TABLE `app_spend_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_spend_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `app_spend_by_hour`
--

DROP TABLE IF EXISTS `app_spend_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_spend_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hash` bigint(20) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_spend_by_hour`
--

LOCK TABLES `app_spend_by_hour` WRITE;
/*!40000 ALTER TABLE `app_spend_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_spend_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `app_spend_by_month`
--

DROP TABLE IF EXISTS `app_spend_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_spend_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_spend_by_month`
--

LOCK TABLES `app_spend_by_month` WRITE;
/*!40000 ALTER TABLE `app_spend_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_spend_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `app_stats_by_day`
--

DROP TABLE IF EXISTS `app_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_stats_by_day`
--

LOCK TABLES `app_stats_by_day` WRITE;
/*!40000 ALTER TABLE `app_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `app_stats_by_hour`
--

DROP TABLE IF EXISTS `app_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_stats_by_hour`
--

LOCK TABLES `app_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `app_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `app_stats_by_month`
--

DROP TABLE IF EXISTS `app_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_stats_by_month`
--

LOCK TABLES `app_stats_by_month` WRITE;
/*!40000 ALTER TABLE `app_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `app_stats_latest`
--

DROP TABLE IF EXISTS `app_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `app_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`),
  KEY `market_aggr_idx` (`aggregated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `app_stats_latest`
--

LOCK TABLES `app_stats_latest` WRITE;
/*!40000 ALTER TABLE `app_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `app_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_app_primary_keys BEFORE INSERT ON app_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `appl_performance`
--

DROP TABLE IF EXISTS `appl_performance`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `appl_performance` (
  `log_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `id` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `performance_type` varchar(64) COLLATE utf8_unicode_ci DEFAULT NULL,
  `entity_type` varchar(64) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rows_aggregated` int(11) DEFAULT NULL,
  `start_time` datetime DEFAULT NULL,
  `end_time` datetime DEFAULT NULL,
  `runtime_seconds` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;


--
-- Table structure for table `audit_log_entries`
--

DROP TABLE IF EXISTS `audit_log_entries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `audit_log_entries` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `snapshot_time` datetime DEFAULT NULL,
  `action_name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `category` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target_object_class` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target_object_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target_object_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `source_class` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `source_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `source_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `destination_class` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `destination_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `destination_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `details` mediumtext COLLATE utf8_unicode_ci,
  PRIMARY KEY (`id`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `target_object_uuid` (`target_object_uuid`),
  KEY `source_uuid` (`source_uuid`),
  KEY `destination_uuid` (`destination_uuid`),
  KEY `category` (`category`),
  KEY `action_name` (`action_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `audit_log_entries`
--

LOCK TABLES `audit_log_entries` WRITE;
/*!40000 ALTER TABLE `audit_log_entries` DISABLE KEYS */;
/*!40000 ALTER TABLE `audit_log_entries` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `audit_log_retention_policies`
--

DROP TABLE IF EXISTS `audit_log_retention_policies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `audit_log_retention_policies` (
  `policy_name` varchar(50) NOT NULL,
  `retention_period` int(11) DEFAULT NULL,
  PRIMARY KEY (`policy_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `audit_log_retention_policies`
--

LOCK TABLES `audit_log_retention_policies` WRITE;
/*!40000 ALTER TABLE `audit_log_retention_policies` DISABLE KEYS */;
INSERT INTO `audit_log_retention_policies` VALUES ('retention_days',365);
/*!40000 ALTER TABLE `audit_log_retention_policies` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `bkgd_version_655`
--

DROP TABLE IF EXISTS `bkgd_version_655`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `bkgd_version_655` (
  `migration_needed` tinyint(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `bkgd_version_655`
--

LOCK TABLES `bkgd_version_655` WRITE;
/*!40000 ALTER TABLE `bkgd_version_655` DISABLE KEYS */;
/*!40000 ALTER TABLE `bkgd_version_655` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `ch_daily_ins_vw`
--

DROP TABLE IF EXISTS `ch_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `ch_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ch_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ch_daily_upd_vw`
--

DROP TABLE IF EXISTS `ch_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `ch_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ch_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ch_hourly_ins_vw`
--

DROP TABLE IF EXISTS `ch_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `ch_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ch_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ch_hourly_upd_vw`
--

DROP TABLE IF EXISTS `ch_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `ch_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ch_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ch_monthly_ins_vw`
--

DROP TABLE IF EXISTS `ch_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `ch_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ch_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ch_monthly_upd_vw`
--

DROP TABLE IF EXISTS `ch_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `ch_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ch_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `ch_stats_by_day`
--

DROP TABLE IF EXISTS `ch_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ch_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ch_stats_by_day`
--

LOCK TABLES `ch_stats_by_day` WRITE;
/*!40000 ALTER TABLE `ch_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `ch_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ch_stats_by_hour`
--

DROP TABLE IF EXISTS `ch_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ch_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ch_stats_by_hour`
--

LOCK TABLES `ch_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `ch_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `ch_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ch_stats_by_month`
--

DROP TABLE IF EXISTS `ch_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ch_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ch_stats_by_month`
--

LOCK TABLES `ch_stats_by_month` WRITE;
/*!40000 ALTER TABLE `ch_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `ch_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ch_stats_latest`
--

DROP TABLE IF EXISTS `ch_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ch_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ch_stats_latest`
--

LOCK TABLES `ch_stats_latest` WRITE;
/*!40000 ALTER TABLE `ch_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `ch_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_ch_primary_keys BEFORE INSERT ON ch_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `classifications`
--

DROP TABLE IF EXISTS `classifications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `classifications` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `classifications`
--

LOCK TABLES `classifications` WRITE;
/*!40000 ALTER TABLE `classifications` DISABLE KEYS */;
/*!40000 ALTER TABLE `classifications` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `classifications_entities_entities`
--

DROP TABLE IF EXISTS `classifications_entities_entities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `classifications_entities_entities` (
  `classification_src_id` int(11) NOT NULL DEFAULT '0',
  `entity_dest_id` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`classification_src_id`,`entity_dest_id`),
  KEY `entity_dest_id` (`entity_dest_id`),
  CONSTRAINT `classifications_entities_entities_ibfk_1` FOREIGN KEY (`classification_src_id`) REFERENCES `classifications` (`id`) ON DELETE CASCADE,
  CONSTRAINT `classifications_entities_entities_ibfk_2` FOREIGN KEY (`entity_dest_id`) REFERENCES `entities` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `classifications_entities_entities`
--

LOCK TABLES `classifications_entities_entities` WRITE;
/*!40000 ALTER TABLE `classifications_entities_entities` DISABLE KEYS */;
/*!40000 ALTER TABLE `classifications_entities_entities` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloud_service_categories`
--

DROP TABLE IF EXISTS `cloud_service_categories`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloud_service_categories` (
  `id` smallint(6) NOT NULL,
  `name` varchar(25) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloud_service_categories`
--

LOCK TABLES `cloud_service_categories` WRITE;
/*!40000 ALTER TABLE `cloud_service_categories` DISABLE KEYS */;
INSERT INTO `cloud_service_categories` VALUES (0,'Unknown'),(1,'Analytics'),(2,'Application Services'),(3,'Artificial Intelligence'),(4,'Business Productivity'),(5,'Compute'),(6,'Desktop'),(7,'Database'),(8,'Developer Tools'),(9,'Internet of Things'),(10,'Management Tools'),(11,'Messaging'),(12,'Migration'),(13,'Mobile Services'),(14,'Networking'),(15,'Security'),(16,'Storage');
/*!40000 ALTER TABLE `cloud_service_categories` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloud_service_providers`
--

DROP TABLE IF EXISTS `cloud_service_providers`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloud_service_providers` (
  `id` smallint(6) NOT NULL,
  `name` varchar(25) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloud_service_providers`
--

LOCK TABLES `cloud_service_providers` WRITE;
/*!40000 ALTER TABLE `cloud_service_providers` DISABLE KEYS */;
INSERT INTO `cloud_service_providers` VALUES (0,'unknown'),(1,'AWS'),(2,'AZURE');
/*!40000 ALTER TABLE `cloud_service_providers` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cloud_services`
--

DROP TABLE IF EXISTS `cloud_services`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cloud_services` (
  `uuid` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `target_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_id` smallint(6) DEFAULT NULL,
  `category_id` smallint(6) DEFAULT NULL,
  PRIMARY KEY (`uuid`),
  KEY `category_id` (`category_id`),
  KEY `provider_id` (`provider_id`),
  CONSTRAINT `services_ibfk_1` FOREIGN KEY (`category_id`) REFERENCES `cloud_service_categories` (`id`),
  CONSTRAINT `services_ibfk_2` FOREIGN KEY (`provider_id`) REFERENCES `cloud_service_providers` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cloud_services`
--

LOCK TABLES `cloud_services` WRITE;
/*!40000 ALTER TABLE `cloud_services` DISABLE KEYS */;
/*!40000 ALTER TABLE `cloud_services` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cluster_members`
--

DROP TABLE IF EXISTS `cluster_members`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_members` (
  `recorded_on` date DEFAULT NULL,
  `group_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `internal_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `group_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `group_type` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `member_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `display_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `recorded_on` (`recorded_on`),
  KEY `internal_name` (`internal_name`),
  KEY `group_uuid` (`group_uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cluster_members`
--

LOCK TABLES `cluster_members` WRITE;
/*!40000 ALTER TABLE `cluster_members` DISABLE KEYS */;
/*!40000 ALTER TABLE `cluster_members` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `cluster_members_end_of_month`
--

DROP TABLE IF EXISTS `cluster_members_end_of_month`;
/*!50001 DROP VIEW IF EXISTS `cluster_members_end_of_month`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cluster_members_end_of_month` (
  `recorded_on` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cluster_members_yesterday`
--

DROP TABLE IF EXISTS `cluster_members_yesterday`;
/*!50001 DROP VIEW IF EXISTS `cluster_members_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cluster_members_yesterday` (
  `recorded_on` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cluster_membership`
--

DROP TABLE IF EXISTS `cluster_membership`;
/*!50001 DROP VIEW IF EXISTS `cluster_membership`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cluster_membership` (
  `recorded_on` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cluster_membership_helper`
--

DROP TABLE IF EXISTS `cluster_membership_helper`;
/*!50001 DROP VIEW IF EXISTS `cluster_membership_helper`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cluster_membership_helper` (
  `uuid` tinyint NOT NULL,
  `val` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `cluster_stats_by_day`
--

DROP TABLE IF EXISTS `cluster_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_stats_by_day` (
  `recorded_on` date NOT NULL,
  `internal_name` varchar(250) COLLATE utf8_unicode_ci NOT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `value` decimal(15,3) DEFAULT NULL,
  `aggregated` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`recorded_on`,`internal_name`,`property_type`,`property_subtype`),
  KEY `recorded_on` (`recorded_on`),
  KEY `internal_name` (`internal_name`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`),
  KEY `cluster_aggr_idx` (`aggregated`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cluster_stats_by_day`
--

LOCK TABLES `cluster_stats_by_day` WRITE;
/*!40000 ALTER TABLE `cluster_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `cluster_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cluster_stats_by_month`
--

DROP TABLE IF EXISTS `cluster_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cluster_stats_by_month` (
  `recorded_on` date NOT NULL,
  `internal_name` varchar(250) COLLATE utf8_unicode_ci NOT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `value` decimal(15,3) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  PRIMARY KEY (`recorded_on`,`internal_name`,`property_type`,`property_subtype`),
  KEY `recorded_on` (`recorded_on`),
  KEY `internal_name` (`internal_name`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cluster_stats_by_month`
--

LOCK TABLES `cluster_stats_by_month` WRITE;
/*!40000 ALTER TABLE `cluster_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `cluster_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `cluster_stats_monthly_ins_vw`
--

DROP TABLE IF EXISTS `cluster_stats_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `cluster_stats_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cluster_stats_monthly_ins_vw` (
  `recorded_on` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `value` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cluster_stats_monthly_upd_vw`
--

DROP TABLE IF EXISTS `cluster_stats_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `cluster_stats_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cluster_stats_monthly_upd_vw` (
  `recorded_on` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `value` tinyint NOT NULL,
  `samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cnt_daily_ins_vw`
--

DROP TABLE IF EXISTS `cnt_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `cnt_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cnt_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cnt_daily_upd_vw`
--

DROP TABLE IF EXISTS `cnt_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `cnt_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cnt_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cnt_hourly_ins_vw`
--

DROP TABLE IF EXISTS `cnt_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `cnt_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cnt_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cnt_hourly_upd_vw`
--

DROP TABLE IF EXISTS `cnt_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `cnt_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cnt_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cnt_monthly_ins_vw`
--

DROP TABLE IF EXISTS `cnt_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `cnt_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cnt_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `cnt_monthly_upd_vw`
--

DROP TABLE IF EXISTS `cnt_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `cnt_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `cnt_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `cnt_stats_by_day`
--

DROP TABLE IF EXISTS `cnt_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cnt_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cnt_stats_by_day`
--

LOCK TABLES `cnt_stats_by_day` WRITE;
/*!40000 ALTER TABLE `cnt_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `cnt_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cnt_stats_by_hour`
--

DROP TABLE IF EXISTS `cnt_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cnt_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cnt_stats_by_hour`
--

LOCK TABLES `cnt_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `cnt_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `cnt_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cnt_stats_by_month`
--

DROP TABLE IF EXISTS `cnt_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cnt_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cnt_stats_by_month`
--

LOCK TABLES `cnt_stats_by_month` WRITE;
/*!40000 ALTER TABLE `cnt_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `cnt_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cnt_stats_latest`
--

DROP TABLE IF EXISTS `cnt_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cnt_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cnt_stats_latest`
--

LOCK TABLES `cnt_stats_latest` WRITE;
/*!40000 ALTER TABLE `cnt_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `cnt_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_cnt_primary_keys BEFORE INSERT ON cnt_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `cpod_stats_by_day`
--

DROP TABLE IF EXISTS `cpod_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpod_stats_by_day` (
  `snapshot_time` date NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cpod_stats_by_day`
--

LOCK TABLES `cpod_stats_by_day` WRITE;
/*!40000 ALTER TABLE `cpod_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `cpod_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cpod_stats_by_hour`
--

DROP TABLE IF EXISTS `cpod_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpod_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cpod_stats_by_hour`
--

LOCK TABLES `cpod_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `cpod_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `cpod_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cpod_stats_by_month`
--

DROP TABLE IF EXISTS `cpod_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpod_stats_by_month` (
  `snapshot_time` date NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cpod_stats_by_month`
--

LOCK TABLES `cpod_stats_by_month` WRITE;
/*!40000 ALTER TABLE `cpod_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `cpod_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `cpod_stats_latest`
--

DROP TABLE IF EXISTS `cpod_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `cpod_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `cpod_stats_latest`
--

LOCK TABLES `cpod_stats_latest` WRITE;
/*!40000 ALTER TABLE `cpod_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `cpod_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_cpod_primary_keys BEFORE INSERT ON cpod_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `customer_info`
--

DROP TABLE IF EXISTS `customer_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `customer_info` (
  `customer_name` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
  `image_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `copyright` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`customer_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `customer_info`
--

LOCK TABLES `customer_info` WRITE;
/*!40000 ALTER TABLE `customer_info` DISABLE KEYS */;
INSERT INTO `customer_info` VALUES ('Turbonomic','/srv/reports/images/logo.png','/srv/reports/images/copyright.jpg');
/*!40000 ALTER TABLE `customer_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `da_stats_by_day`
--

DROP TABLE IF EXISTS `da_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `da_stats_by_day` (
  `snapshot_time` date NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `da_stats_by_day`
--

LOCK TABLES `da_stats_by_day` WRITE;
/*!40000 ALTER TABLE `da_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `da_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `da_stats_by_hour`
--

DROP TABLE IF EXISTS `da_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `da_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `da_stats_by_hour`
--

LOCK TABLES `da_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `da_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `da_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `da_stats_by_month`
--

DROP TABLE IF EXISTS `da_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `da_stats_by_month` (
  `snapshot_time` date NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `da_stats_by_month`
--

LOCK TABLES `da_stats_by_month` WRITE;
/*!40000 ALTER TABLE `da_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `da_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `da_stats_latest`
--

DROP TABLE IF EXISTS `da_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `da_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `da_stats_latest`
--

LOCK TABLES `da_stats_latest` WRITE;
/*!40000 ALTER TABLE `da_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `da_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_da_primary_keys BEFORE INSERT ON da_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Temporary table structure for view `dpod_daily_ins_vw`
--

DROP TABLE IF EXISTS `dpod_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `dpod_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `dpod_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `dpod_daily_upd_vw`
--

DROP TABLE IF EXISTS `dpod_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `dpod_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `dpod_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `dpod_hourly_ins_vw`
--

DROP TABLE IF EXISTS `dpod_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `dpod_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `dpod_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `dpod_hourly_upd_vw`
--

DROP TABLE IF EXISTS `dpod_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `dpod_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `dpod_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `dpod_monthly_ins_vw`
--

DROP TABLE IF EXISTS `dpod_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `dpod_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `dpod_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `dpod_monthly_upd_vw`
--

DROP TABLE IF EXISTS `dpod_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `dpod_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `dpod_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `dpod_stats_by_day`
--

DROP TABLE IF EXISTS `dpod_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpod_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dpod_stats_by_day`
--

LOCK TABLES `dpod_stats_by_day` WRITE;
/*!40000 ALTER TABLE `dpod_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `dpod_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dpod_stats_by_hour`
--

DROP TABLE IF EXISTS `dpod_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpod_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dpod_stats_by_hour`
--

LOCK TABLES `dpod_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `dpod_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `dpod_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dpod_stats_by_month`
--

DROP TABLE IF EXISTS `dpod_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpod_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dpod_stats_by_month`
--

LOCK TABLES `dpod_stats_by_month` WRITE;
/*!40000 ALTER TABLE `dpod_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `dpod_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `dpod_stats_latest`
--

DROP TABLE IF EXISTS `dpod_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `dpod_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `dpod_stats_latest`
--

LOCK TABLES `dpod_stats_latest` WRITE;
/*!40000 ALTER TABLE `dpod_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `dpod_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_dpod_primary_keys BEFORE INSERT ON dpod_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Temporary table structure for view `ds_capacity_by_day`
--

DROP TABLE IF EXISTS `ds_capacity_by_day`;
/*!50001 DROP VIEW IF EXISTS `ds_capacity_by_day`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_capacity_by_day` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_capacity_by_day_per_ds_group`
--

DROP TABLE IF EXISTS `ds_capacity_by_day_per_ds_group`;
/*!50001 DROP VIEW IF EXISTS `ds_capacity_by_day_per_ds_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_capacity_by_day_per_ds_group` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_capacity_by_hour`
--

DROP TABLE IF EXISTS `ds_capacity_by_hour`;
/*!50001 DROP VIEW IF EXISTS `ds_capacity_by_hour`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_capacity_by_hour` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_daily_ins_vw`
--

DROP TABLE IF EXISTS `ds_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `ds_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_daily_upd_vw`
--

DROP TABLE IF EXISTS `ds_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `ds_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_group_assns`
--

DROP TABLE IF EXISTS `ds_group_assns`;
/*!50001 DROP VIEW IF EXISTS `ds_group_assns`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_group_assns` (
  `assn_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_group_members`
--

DROP TABLE IF EXISTS `ds_group_members`;
/*!50001 DROP VIEW IF EXISTS `ds_group_members`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_group_members` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_group_members_helper`
--

DROP TABLE IF EXISTS `ds_group_members_helper`;
/*!50001 DROP VIEW IF EXISTS `ds_group_members_helper`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_group_members_helper` (
  `entity_dest_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_groups`
--

DROP TABLE IF EXISTS `ds_groups`;
/*!50001 DROP VIEW IF EXISTS `ds_groups`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_groups` (
  `entity_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_hourly_ins_vw`
--

DROP TABLE IF EXISTS `ds_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `ds_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_hourly_upd_vw`
--

DROP TABLE IF EXISTS `ds_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `ds_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_instances`
--

DROP TABLE IF EXISTS `ds_instances`;
/*!50001 DROP VIEW IF EXISTS `ds_instances`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_instances` (
  `name` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_monthly_ins_vw`
--

DROP TABLE IF EXISTS `ds_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `ds_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_monthly_upd_vw`
--

DROP TABLE IF EXISTS `ds_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `ds_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `ds_stats_by_day`
--

DROP TABLE IF EXISTS `ds_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ds_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ds_stats_by_day`
--

LOCK TABLES `ds_stats_by_day` WRITE;
/*!40000 ALTER TABLE `ds_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `ds_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `ds_stats_by_day_per_ds_group`
--

DROP TABLE IF EXISTS `ds_stats_by_day_per_ds_group`;
/*!50001 DROP VIEW IF EXISTS `ds_stats_by_day_per_ds_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_stats_by_day_per_ds_group` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `ds_stats_by_hour`
--

DROP TABLE IF EXISTS `ds_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ds_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ds_stats_by_hour`
--

LOCK TABLES `ds_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `ds_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `ds_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `ds_stats_by_hour_per_ds_group`
--

DROP TABLE IF EXISTS `ds_stats_by_hour_per_ds_group`;
/*!50001 DROP VIEW IF EXISTS `ds_stats_by_hour_per_ds_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_stats_by_hour_per_ds_group` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `ds_stats_by_month`
--

DROP TABLE IF EXISTS `ds_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ds_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ds_stats_by_month`
--

LOCK TABLES `ds_stats_by_month` WRITE;
/*!40000 ALTER TABLE `ds_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `ds_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ds_stats_latest`
--

DROP TABLE IF EXISTS `ds_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ds_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ds_stats_latest`
--

LOCK TABLES `ds_stats_latest` WRITE;
/*!40000 ALTER TABLE `ds_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `ds_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_ds_primary_keys BEFORE INSERT ON ds_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Temporary table structure for view `ds_util_info_yesterday`
--

DROP TABLE IF EXISTS `ds_util_info_yesterday`;
/*!50001 DROP VIEW IF EXISTS `ds_util_info_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_util_info_yesterday` (
  `snapshot_time` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `ds_util_stats_yesterday`
--

DROP TABLE IF EXISTS `ds_util_stats_yesterday`;
/*!50001 DROP VIEW IF EXISTS `ds_util_stats_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `ds_util_stats_yesterday` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `entities`
--

DROP TABLE IF EXISTS `entities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `entities` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `display_name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `creation_class` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `uuid` (`uuid`),
  KEY `creation_class` (`creation_class`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `entities`
--

LOCK TABLES `entities` WRITE;
/*!40000 ALTER TABLE `entities` DISABLE KEYS */;
/*!40000 ALTER TABLE `entities` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `entity_assns`
--

DROP TABLE IF EXISTS `entity_assns`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `entity_assns` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `entity_entity_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `entity_entity_id` (`entity_entity_id`),
  KEY `name` (`name`,`entity_entity_id`),
  CONSTRAINT `entity_assns_ibfk_1` FOREIGN KEY (`entity_entity_id`) REFERENCES `entities` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `entity_assns`
--

LOCK TABLES `entity_assns` WRITE;
/*!40000 ALTER TABLE `entity_assns` DISABLE KEYS */;
/*!40000 ALTER TABLE `entity_assns` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `entity_assns_members_entities`
--

DROP TABLE IF EXISTS `entity_assns_members_entities`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `entity_assns_members_entities` (
  `entity_assn_src_id` bigint(20) NOT NULL DEFAULT '0',
  `entity_dest_id` bigint(20) NOT NULL DEFAULT '0',
  PRIMARY KEY (`entity_assn_src_id`,`entity_dest_id`),
  KEY `entity_dest_id` (`entity_dest_id`),
  KEY `entity_assn_src_id` (`entity_assn_src_id`,`entity_dest_id`),
  CONSTRAINT `entity_assns_members_entities_ibfk_1` FOREIGN KEY (`entity_assn_src_id`) REFERENCES `entity_assns` (`id`) ON DELETE CASCADE,
  CONSTRAINT `entity_assns_members_entities_ibfk_2` FOREIGN KEY (`entity_dest_id`) REFERENCES `entities` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `entity_assns_members_entities`
--

LOCK TABLES `entity_assns_members_entities` WRITE;
/*!40000 ALTER TABLE `entity_assns_members_entities` DISABLE KEYS */;
/*!40000 ALTER TABLE `entity_assns_members_entities` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `entity_attrs`
--

DROP TABLE IF EXISTS `entity_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `entity_attrs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `value` varchar(1000) COLLATE utf8_unicode_ci DEFAULT NULL,
  `entity_entity_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `entity_entity_id` (`entity_entity_id`),
  KEY `name` (`name`,`entity_entity_id`),
  CONSTRAINT `entity_attrs_ibfk_1` FOREIGN KEY (`entity_entity_id`) REFERENCES `entities` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `entity_attrs`
--

LOCK TABLES `entity_attrs` WRITE;
/*!40000 ALTER TABLE `entity_attrs` DISABLE KEYS */;
/*!40000 ALTER TABLE `entity_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `entity_group_assns`
--

DROP TABLE IF EXISTS `entity_group_assns`;
/*!50001 DROP VIEW IF EXISTS `entity_group_assns`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `entity_group_assns` (
  `assn_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `entity_group_members`
--

DROP TABLE IF EXISTS `entity_group_members`;
/*!50001 DROP VIEW IF EXISTS `entity_group_members`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `entity_group_members` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `entity_group_members_helper`
--

DROP TABLE IF EXISTS `entity_group_members_helper`;
/*!50001 DROP VIEW IF EXISTS `entity_group_members_helper`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `entity_group_members_helper` (
  `entity_dest_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `entity_groups`
--

DROP TABLE IF EXISTS `entity_groups`;
/*!50001 DROP VIEW IF EXISTS `entity_groups`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `entity_groups` (
  `entity_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `instance_type_hourly_by_week`
--

DROP TABLE IF EXISTS `instance_type_hourly_by_week`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `instance_type_hourly_by_week` (
  `hour` int(3) NOT NULL,
  `day` tinyint(3) NOT NULL,
  `target_uuid` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
  `instance_type` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
  `availability_zone` varchar(80) COLLATE utf8_unicode_ci NOT NULL,
  `platform` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `tenancy` varchar(36) COLLATE utf8_unicode_ci NOT NULL,
  `weighted_value` decimal(15,3) DEFAULT NULL,
  PRIMARY KEY (`hour`,`day`,`target_uuid`,`instance_type`,`availability_zone`,`platform`,`tenancy`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `instance_type_hourly_by_week`
--

LOCK TABLES `instance_type_hourly_by_week` WRITE;
/*!40000 ALTER TABLE `instance_type_hourly_by_week` DISABLE KEYS */;
/*!40000 ALTER TABLE `instance_type_hourly_by_week` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `iom_daily_ins_vw`
--

DROP TABLE IF EXISTS `iom_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `iom_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `iom_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `iom_daily_upd_vw`
--

DROP TABLE IF EXISTS `iom_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `iom_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `iom_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `iom_hourly_ins_vw`
--

DROP TABLE IF EXISTS `iom_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `iom_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `iom_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `iom_hourly_upd_vw`
--

DROP TABLE IF EXISTS `iom_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `iom_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `iom_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `iom_monthly_ins_vw`
--

DROP TABLE IF EXISTS `iom_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `iom_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `iom_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `iom_monthly_upd_vw`
--

DROP TABLE IF EXISTS `iom_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `iom_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `iom_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `iom_stats_by_day`
--

DROP TABLE IF EXISTS `iom_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `iom_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `iom_stats_by_day`
--

LOCK TABLES `iom_stats_by_day` WRITE;
/*!40000 ALTER TABLE `iom_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `iom_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `iom_stats_by_hour`
--

DROP TABLE IF EXISTS `iom_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `iom_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `iom_stats_by_hour`
--

LOCK TABLES `iom_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `iom_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `iom_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `iom_stats_by_month`
--

DROP TABLE IF EXISTS `iom_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `iom_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `iom_stats_by_month`
--

LOCK TABLES `iom_stats_by_month` WRITE;
/*!40000 ALTER TABLE `iom_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `iom_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `iom_stats_latest`
--

DROP TABLE IF EXISTS `iom_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `iom_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `iom_stats_latest`
--

LOCK TABLES `iom_stats_latest` WRITE;
/*!40000 ALTER TABLE `iom_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `iom_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_iom_primary_keys BEFORE INSERT ON iom_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `is_persistence`
--

DROP TABLE IF EXISTS `is_persistence`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `is_persistence` (
  `oid` bigint(8) unsigned NOT NULL,
  `field_1` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_2` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_3` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_4` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_5` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_6` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_7` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_8` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_9` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_10` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_11` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_12` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_13` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_14` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_15` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_16` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `is_persistence`
--

LOCK TABLES `is_persistence` WRITE;
/*!40000 ALTER TABLE `is_persistence` DISABLE KEYS */;
/*!40000 ALTER TABLE `is_persistence` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lp_stats_by_day`
--

DROP TABLE IF EXISTS `lp_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lp_stats_by_day` (
  `snapshot_time` date NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lp_stats_by_day`
--

LOCK TABLES `lp_stats_by_day` WRITE;
/*!40000 ALTER TABLE `lp_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `lp_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lp_stats_by_hour`
--

DROP TABLE IF EXISTS `lp_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lp_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lp_stats_by_hour`
--

LOCK TABLES `lp_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `lp_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `lp_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lp_stats_by_month`
--

DROP TABLE IF EXISTS `lp_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lp_stats_by_month` (
  `snapshot_time` date NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lp_stats_by_month`
--

LOCK TABLES `lp_stats_by_month` WRITE;
/*!40000 ALTER TABLE `lp_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `lp_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `lp_stats_latest`
--

DROP TABLE IF EXISTS `lp_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `lp_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `day_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  `month_key` varchar(32) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `lp_stats_latest`
--

LOCK TABLES `lp_stats_latest` WRITE;
/*!40000 ALTER TABLE `lp_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `lp_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_lp_primary_keys BEFORE INSERT ON lp_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `market_stats_by_day`
--

DROP TABLE IF EXISTS `market_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `market_stats_by_day` (
  `snapshot_time` datetime DEFAULT NULL,
  `topology_context_id` bigint(20) DEFAULT NULL,
  `entity_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `topology_context_id` (`topology_context_id`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`),
  KEY `market_stats_by_hour_idx` (`snapshot_time`,`topology_context_id`,`entity_type`,`property_type`,`property_subtype`,`relation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `market_stats_by_day`
--

LOCK TABLES `market_stats_by_day` WRITE;
/*!40000 ALTER TABLE `market_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `market_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `market_stats_by_hour`
--

DROP TABLE IF EXISTS `market_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `market_stats_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `topology_context_id` bigint(20) DEFAULT NULL,
  `entity_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `topology_context_id` (`topology_context_id`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`),
  KEY `market_stats_by_hour_idx` (`snapshot_time`,`topology_context_id`,`entity_type`,`property_type`,`property_subtype`,`relation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `market_stats_by_hour`
--

LOCK TABLES `market_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `market_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `market_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `market_stats_by_month`
--

DROP TABLE IF EXISTS `market_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `market_stats_by_month` (
  `snapshot_time` datetime DEFAULT NULL,
  `topology_context_id` bigint(20) DEFAULT NULL,
  `entity_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `topology_context_id` (`topology_context_id`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`),
  KEY `market_stats_by_hour_idx` (`snapshot_time`,`topology_context_id`,`entity_type`,`property_type`,`property_subtype`,`relation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `market_stats_by_month`
--

LOCK TABLES `market_stats_by_month` WRITE;
/*!40000 ALTER TABLE `market_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `market_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `market_stats_latest`
--

DROP TABLE IF EXISTS `market_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `market_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `topology_context_id` bigint(20) DEFAULT NULL,
  `topology_id` bigint(20) DEFAULT NULL,
  `entity_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  KEY `snapshot_time` (`snapshot_time`),
  KEY `topology_context_id` (`topology_context_id`),
  KEY `topology_id` (`topology_id`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`),
  KEY `market_latest_idx` (`snapshot_time`,`topology_context_id`,`entity_type`,`property_type`,`property_subtype`,`relation`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `market_stats_latest`
--

LOCK TABLES `market_stats_latest` WRITE;
/*!40000 ALTER TABLE `market_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `market_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mkt_snapshots`
--

DROP TABLE IF EXISTS `mkt_snapshots`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mkt_snapshots` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `mkt_snapshot_source_id` bigint(20) DEFAULT NULL,
  `scenario_id` bigint(20) NOT NULL,
  `display_name` varchar(250) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `state` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `committed` tinyint(1) DEFAULT NULL,
  `run_time` datetime NOT NULL,
  `run_complete_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `scenario_id` (`scenario_id`),
  KEY `committed` (`committed`),
  CONSTRAINT `mkt_snapshots_ibfk_1` FOREIGN KEY (`scenario_id`) REFERENCES `scenarios` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mkt_snapshots`
--

LOCK TABLES `mkt_snapshots` WRITE;
/*!40000 ALTER TABLE `mkt_snapshots` DISABLE KEYS */;
/*!40000 ALTER TABLE `mkt_snapshots` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mkt_snapshots_stats`
--

DROP TABLE IF EXISTS `mkt_snapshots_stats`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mkt_snapshots_stats` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `recorded_on` datetime NOT NULL,
  `mkt_snapshot_id` bigint(20) NOT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(30,3) DEFAULT NULL,
  `avg_value` decimal(30,3) DEFAULT NULL,
  `min_value` decimal(30,3) DEFAULT NULL,
  `max_value` decimal(30,3) DEFAULT NULL,
  `projection_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `mkt_snapshot_id` (`mkt_snapshot_id`),
  CONSTRAINT `mkt_snapshots_stats_ibfk_1` FOREIGN KEY (`mkt_snapshot_id`) REFERENCES `mkt_snapshots` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mkt_snapshots_stats`
--

LOCK TABLES `mkt_snapshots_stats` WRITE;
/*!40000 ALTER TABLE `mkt_snapshots_stats` DISABLE KEYS */;
/*!40000 ALTER TABLE `mkt_snapshots_stats` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mkt_snapshots_unplaced`
--

DROP TABLE IF EXISTS `mkt_snapshots_unplaced`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mkt_snapshots_unplaced` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `mkt_snapshot_id` bigint(20) DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `display_name` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,
  `classname` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `not_placed_on` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `is_placed_on` varchar(200) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `mkt_snapshot_id` (`mkt_snapshot_id`),
  CONSTRAINT `mkt_snapshots_unplaced_ibfk_1` FOREIGN KEY (`mkt_snapshot_id`) REFERENCES `mkt_snapshots` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mkt_snapshots_unplaced`
--

LOCK TABLES `mkt_snapshots_unplaced` WRITE;
/*!40000 ALTER TABLE `mkt_snapshots_unplaced` DISABLE KEYS */;
/*!40000 ALTER TABLE `mkt_snapshots_unplaced` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `mkt_snapshots_users`
--

DROP TABLE IF EXISTS `mkt_snapshots_users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `mkt_snapshots_users` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `mkt_snapshot_id` bigint(20) NOT NULL,
  `username` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_group` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `mkt_snapshot_id` (`mkt_snapshot_id`),
  KEY `username` (`username`),
  KEY `user_group` (`user_group`),
  CONSTRAINT `mkt_snapshots_users_ibfk_1` FOREIGN KEY (`mkt_snapshot_id`) REFERENCES `mkt_snapshots` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `mkt_snapshots_users`
--

LOCK TABLES `mkt_snapshots_users` WRITE;
/*!40000 ALTER TABLE `mkt_snapshots_users` DISABLE KEYS */;
/*!40000 ALTER TABLE `mkt_snapshots_users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `notifications`
--

DROP TABLE IF EXISTS `notifications`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `notifications` (
  `id` bigint(20) NOT NULL,
  `clear_time` datetime DEFAULT NULL,
  `last_notify_time` datetime DEFAULT NULL,
  `severity` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `category` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `name` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `importance` double DEFAULT NULL,
  `description` mediumtext COLLATE utf8_unicode_ci,
  `notification_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `notifications`
--

LOCK TABLES `notifications` WRITE;
/*!40000 ALTER TABLE `notifications` DISABLE KEYS */;
/*!40000 ALTER TABLE `notifications` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `notifications_risk`
--

DROP TABLE IF EXISTS `notifications_risk`;
/*!50001 DROP VIEW IF EXISTS `notifications_risk`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `notifications_risk` (
  `id` tinyint NOT NULL,
  `clear_time` tinyint NOT NULL,
  `last_notify_time` tinyint NOT NULL,
  `severity` tinyint NOT NULL,
  `category` tinyint NOT NULL,
  `name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `importance` tinyint NOT NULL,
  `description` tinyint NOT NULL,
  `notification_uuid` tinyint NOT NULL,
  `risk` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `on_demand_reports`
--

DROP TABLE IF EXISTS `on_demand_reports`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `on_demand_reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `obj_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `scope_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `short_desc` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `filename` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` mediumtext COLLATE utf8_unicode_ci,
  `category` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `title` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `on_demand_reports`
--

LOCK TABLES `on_demand_reports` WRITE;
/*!40000 ALTER TABLE `on_demand_reports` DISABLE KEYS */;
INSERT INTO `on_demand_reports` VALUES (1,'PhysicalMachine','Group','Host Group Summary','pm_group_profile','Host Group Summary','Performance Management/Hosts','Host Group Summary'),(2,'PhysicalMachine','Instance','Host Summary','pm_profile','Host Summary','Performance Management/Hosts','Host Summary'),(3,'Storage','Group','Datastore Group Summary','sd_group_profile','Datastore Group Summary','Performance Management/Storage','Datastore Group Summary'),(4,'Storage','Instance','Data Store Summary','sd_profile','Data Store Summary','Performance Management/Storage','Data Store Summary'),(5,'VirtualMachine','Group','VM Group Summary','vm_group_profile','VM Group Summary','Performance Management/VMs','VM Group Summary'),(6,'VirtualMachine','Instance','Virtual Machine Summary','vm_profile','Virtual Machine Summary','Performance Management/VMs','Virtual Machine Summary'),(8,'PhysicalMachine','Group','Hosting Summary by Host Group','pm_group_hosting','Hosting Summary by Host Group','Capacity Management/Hosts','Hosting Summary by Host Group'),(9,'VirtualMachine','Group','Physical Resources Chargeback by VM Group','vm_group_profile_physical_resources','Monthly Virtual Machine summary of Physical Resources','Capacity Management/VMs','VM Group Summary (Physical Resources)'),(10,'VirtualMachine','Group','Individual VM Monthly Summary by VM Group','vm_group_individual_monthly_summary','Monthly Virtual Machine Group Summary Breakdown by Individual VM','Capacity Management/VMs','Individual VM Monthly Summary by VM Group'),(11,'VirtualMachine','Group','Month Based VM Group Over Under Provisioning','vm_group_daily_over_under_prov_grid_30_days','Month Based VM Group Over Under Provisioning','Capacity Management/VMs','Month Based VM Group Over Under Provisioning'),(12,'VirtualMachine','Group','Shows a capacity-centered view of CPU and Memory utilization, providing actual q','vm_group_30_days_vm_top_bottom_capacity_grid','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','Capacity Management/VMs','VM 15 Top Bottom Capacity'),(13,'VirtualMachine','Group','This report shows Virtual Machines that are over and under provisioned as indica','vm_group_daily_over_under_prov_grid_given_days','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory  ','VM Over/Under Provisioning 90 Days','VM Group Over/Under Provisioning by Number of Days Ago'),(14,'PhysicalMachine','Group','Monthly Individual Cluster Summary ','pm_group_monthly_individual_cluster_summary','Monthly Individual Cluster Summary','Performance Management/Hosts','Host Monthly Individual Cluster Summary'),(15,'Cluster','Group','Host Top Bottom Capacity Grid Per Cluster','pm_group_pm_top_bottom_capacity_grid_per_cluster','Host Top Bottom Capacity Grid Per Cluster','Performance Management/Hosts','Host Top Bottom Capacity Grid Per Cluster'),(16,'Cluster','Group','Host/VM Group - Resource Utilization Heatmap','pm_group_daily_pm_vm_utilization_heatmap_grid','Heatmap report per cluster','Capacity Management/Hosts','Host/VM Group - Resource Utilization Heatmap'),(17,'VirtualMachine','Group','VM Group RightSizingInfo','vm_group_rightsizing_advice_grid','VM Group RightSizing Info','Performance Management/VMs','VM Group RightSizing Info');
/*!40000 ALTER TABLE `on_demand_reports` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `pm_capacity_by_day`
--

DROP TABLE IF EXISTS `pm_capacity_by_day`;
/*!50001 DROP VIEW IF EXISTS `pm_capacity_by_day`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_capacity_by_day` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_capacity_by_day_per_pm_group`
--

DROP TABLE IF EXISTS `pm_capacity_by_day_per_pm_group`;
/*!50001 DROP VIEW IF EXISTS `pm_capacity_by_day_per_pm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_capacity_by_day_per_pm_group` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_capacity_by_hour`
--

DROP TABLE IF EXISTS `pm_capacity_by_hour`;
/*!50001 DROP VIEW IF EXISTS `pm_capacity_by_hour`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_capacity_by_hour` (
  `class_name` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_daily_ins_vw`
--

DROP TABLE IF EXISTS `pm_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `pm_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_daily_upd_vw`
--

DROP TABLE IF EXISTS `pm_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `pm_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_group_assns`
--

DROP TABLE IF EXISTS `pm_group_assns`;
/*!50001 DROP VIEW IF EXISTS `pm_group_assns`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_group_assns` (
  `assn_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_group_members`
--

DROP TABLE IF EXISTS `pm_group_members`;
/*!50001 DROP VIEW IF EXISTS `pm_group_members`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_group_members` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_group_members_helper`
--

DROP TABLE IF EXISTS `pm_group_members_helper`;
/*!50001 DROP VIEW IF EXISTS `pm_group_members_helper`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_group_members_helper` (
  `entity_dest_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_groups`
--

DROP TABLE IF EXISTS `pm_groups`;
/*!50001 DROP VIEW IF EXISTS `pm_groups`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_groups` (
  `entity_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_hourly_ins_vw`
--

DROP TABLE IF EXISTS `pm_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `pm_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_hourly_upd_vw`
--

DROP TABLE IF EXISTS `pm_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `pm_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_instances`
--

DROP TABLE IF EXISTS `pm_instances`;
/*!50001 DROP VIEW IF EXISTS `pm_instances`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_instances` (
  `name` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_monthly_ins_vw`
--

DROP TABLE IF EXISTS `pm_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `pm_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_monthly_upd_vw`
--

DROP TABLE IF EXISTS `pm_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `pm_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `pm_stats_by_day`
--

DROP TABLE IF EXISTS `pm_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pm_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pm_stats_by_day`
--

LOCK TABLES `pm_stats_by_day` WRITE;
/*!40000 ALTER TABLE `pm_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `pm_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `pm_stats_by_day_per_pm_group`
--

DROP TABLE IF EXISTS `pm_stats_by_day_per_pm_group`;
/*!50001 DROP VIEW IF EXISTS `pm_stats_by_day_per_pm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_stats_by_day_per_pm_group` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `pm_stats_by_hour`
--

DROP TABLE IF EXISTS `pm_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pm_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pm_stats_by_hour`
--

LOCK TABLES `pm_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `pm_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `pm_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `pm_stats_by_hour_per_pm_group`
--

DROP TABLE IF EXISTS `pm_stats_by_hour_per_pm_group`;
/*!50001 DROP VIEW IF EXISTS `pm_stats_by_hour_per_pm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_stats_by_hour_per_pm_group` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `pm_stats_by_month`
--

DROP TABLE IF EXISTS `pm_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pm_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pm_stats_by_month`
--

LOCK TABLES `pm_stats_by_month` WRITE;
/*!40000 ALTER TABLE `pm_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `pm_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `pm_stats_latest`
--

DROP TABLE IF EXISTS `pm_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `pm_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `pm_stats_latest`
--

LOCK TABLES `pm_stats_latest` WRITE;
/*!40000 ALTER TABLE `pm_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `pm_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_pm_primary_keys BEFORE INSERT ON pm_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Temporary table structure for view `pm_util_info_yesterday`
--

DROP TABLE IF EXISTS `pm_util_info_yesterday`;
/*!50001 DROP VIEW IF EXISTS `pm_util_info_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_util_info_yesterday` (
  `snapshot_time` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_util_stats_yesterday`
--

DROP TABLE IF EXISTS `pm_util_stats_yesterday`;
/*!50001 DROP VIEW IF EXISTS `pm_util_stats_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_util_stats_yesterday` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `pm_vm_count_by_day_per_pm_group`
--

DROP TABLE IF EXISTS `pm_vm_count_by_day_per_pm_group`;
/*!50001 DROP VIEW IF EXISTS `pm_vm_count_by_day_per_pm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `pm_vm_count_by_day_per_pm_group` (
  `group_uuid` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `vm_count` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `report_attrs`
--

DROP TABLE IF EXISTS `report_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `report_attrs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `report_id` int(11) NOT NULL,
  `report_type` tinyint(3) NOT NULL,
  `name` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `default_value` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `att_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`report_id`,`report_type`,`name`),
  KEY `report_id` (`report_id`),
  KEY `report_type` (`report_type`),
  KEY `name` (`name`),
  KEY `id` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `report_attrs`
--

LOCK TABLES `report_attrs` WRITE;
/*!40000 ALTER TABLE `report_attrs` DISABLE KEYS */;
INSERT INTO `report_attrs` VALUES (4,2,2,'num_days_ago','30','int'),(2,6,2,'num_days_ago','30','int'),(3,6,2,'show_charts','True','boolean'),(1,13,2,'num_days_ago','30','int');
/*!40000 ALTER TABLE `report_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `report_subscriptions`
--

DROP TABLE IF EXISTS `report_subscriptions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `report_subscriptions` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `email` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `period` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `day_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `obj_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `report_id` int(11) DEFAULT NULL,
  `report_type` tinyint(3) DEFAULT NULL,
  `format` tinyint(3) DEFAULT NULL,
  `username` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `report_type` (`report_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `report_subscriptions`
--

LOCK TABLES `report_subscriptions` WRITE;
/*!40000 ALTER TABLE `report_subscriptions` DISABLE KEYS */;
/*!40000 ALTER TABLE `report_subscriptions` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `retention_policies`
--

DROP TABLE IF EXISTS `retention_policies`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `retention_policies` (
  `policy_name` varchar(50) DEFAULT NULL,
  `retention_period` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `retention_policies`
--

LOCK TABLES `retention_policies` WRITE;
/*!40000 ALTER TABLE `retention_policies` DISABLE KEYS */;
INSERT INTO `retention_policies` VALUES ('retention_latest_hours',2),('retention_hours',72),('retention_days',60),('retention_months',24);
/*!40000 ALTER TABLE `retention_policies` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ri_stats_by_day`
--

DROP TABLE IF EXISTS `ri_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ri_stats_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ri_stats_by_day`
--

LOCK TABLES `ri_stats_by_day` WRITE;
/*!40000 ALTER TABLE `ri_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `ri_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ri_stats_by_hour`
--

DROP TABLE IF EXISTS `ri_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ri_stats_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ri_stats_by_hour`
--

LOCK TABLES `ri_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `ri_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `ri_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ri_stats_by_month`
--

DROP TABLE IF EXISTS `ri_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ri_stats_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ri_stats_by_month`
--

LOCK TABLES `ri_stats_by_month` WRITE;
/*!40000 ALTER TABLE `ri_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `ri_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ri_stats_latest`
--

DROP TABLE IF EXISTS `ri_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ri_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ri_stats_latest`
--

LOCK TABLES `ri_stats_latest` WRITE;
/*!40000 ALTER TABLE `ri_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `ri_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `sc_daily_ins_vw`
--

DROP TABLE IF EXISTS `sc_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `sc_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sc_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sc_daily_upd_vw`
--

DROP TABLE IF EXISTS `sc_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `sc_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sc_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sc_hourly_ins_vw`
--

DROP TABLE IF EXISTS `sc_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `sc_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sc_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sc_hourly_upd_vw`
--

DROP TABLE IF EXISTS `sc_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `sc_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sc_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sc_monthly_ins_vw`
--

DROP TABLE IF EXISTS `sc_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `sc_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sc_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sc_monthly_upd_vw`
--

DROP TABLE IF EXISTS `sc_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `sc_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sc_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `sc_stats_by_day`
--

DROP TABLE IF EXISTS `sc_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sc_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sc_stats_by_day`
--

LOCK TABLES `sc_stats_by_day` WRITE;
/*!40000 ALTER TABLE `sc_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `sc_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sc_stats_by_hour`
--

DROP TABLE IF EXISTS `sc_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sc_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sc_stats_by_hour`
--

LOCK TABLES `sc_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `sc_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `sc_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sc_stats_by_month`
--

DROP TABLE IF EXISTS `sc_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sc_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sc_stats_by_month`
--

LOCK TABLES `sc_stats_by_month` WRITE;
/*!40000 ALTER TABLE `sc_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `sc_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sc_stats_latest`
--

DROP TABLE IF EXISTS `sc_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sc_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sc_stats_latest`
--

LOCK TABLES `sc_stats_latest` WRITE;
/*!40000 ALTER TABLE `sc_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `sc_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_sc_primary_keys BEFORE INSERT ON sc_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `scenarios`
--

DROP TABLE IF EXISTS `scenarios`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `display_name` varchar(250) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `create_time` datetime NOT NULL,
  `update_time` datetime DEFAULT NULL,
  `source_scenario_id` bigint(20) DEFAULT NULL,
  `type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `display_name` (`display_name`),
  KEY `source_scenario_id` (`source_scenario_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios`
--

LOCK TABLES `scenarios` WRITE;
/*!40000 ALTER TABLE `scenarios` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarios_changes`
--

DROP TABLE IF EXISTS `scenarios_changes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios_changes` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scenario_id` bigint(20) NOT NULL,
  `type` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `count` int(3) DEFAULT NULL,
  `number` int(3) DEFAULT NULL,
  `se_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT '',
  `change_obj` varchar(80) COLLATE utf8_unicode_ci DEFAULT '',
  `description` varchar(250) COLLATE utf8_unicode_ci DEFAULT '',
  `merge_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `operation_type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `segment_id` varchar(40) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `scenario_id` (`scenario_id`),
  CONSTRAINT `scenarios_changes_ibfk_1` FOREIGN KEY (`scenario_id`) REFERENCES `scenarios` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios_changes`
--

LOCK TABLES `scenarios_changes` WRITE;
/*!40000 ALTER TABLE `scenarios_changes` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios_changes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarios_changes_projection`
--

DROP TABLE IF EXISTS `scenarios_changes_projection`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios_changes_projection` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scenario_change_id` bigint(20) NOT NULL,
  `days` int(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `scenario_change_id` (`scenario_change_id`),
  CONSTRAINT `scenarios_changes_projection_ibfk_1` FOREIGN KEY (`scenario_change_id`) REFERENCES `scenarios_changes` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=416 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios_changes_projection`
--

LOCK TABLES `scenarios_changes_projection` WRITE;
/*!40000 ALTER TABLE `scenarios_changes_projection` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios_changes_projection` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarios_changes_targets`
--

DROP TABLE IF EXISTS `scenarios_changes_targets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios_changes_targets` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scenario_change_id` bigint(20) NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `display_name` varchar(200) COLLATE utf8_unicode_ci DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `scenario_change_id` (`scenario_change_id`),
  CONSTRAINT `scenarios_changes_targets_ibfk_1` FOREIGN KEY (`scenario_change_id`) REFERENCES `scenarios_changes` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios_changes_targets`
--

LOCK TABLES `scenarios_changes_targets` WRITE;
/*!40000 ALTER TABLE `scenarios_changes_targets` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios_changes_targets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarios_projection`
--

DROP TABLE IF EXISTS `scenarios_projection`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios_projection` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scenario_id` bigint(20) DEFAULT NULL,
  `days` int(3) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `scenario_id` (`scenario_id`),
  CONSTRAINT `scenarios_projection_ibfk_1` FOREIGN KEY (`scenario_id`) REFERENCES `scenarios` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios_projection`
--

LOCK TABLES `scenarios_projection` WRITE;
/*!40000 ALTER TABLE `scenarios_projection` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios_projection` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarios_scopes`
--

DROP TABLE IF EXISTS `scenarios_scopes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios_scopes` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scenario_id` bigint(20) NOT NULL,
  `scope_uuid` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `scope_name` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,
  `scope_type` varchar(100) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `scope_uuid` (`scope_uuid`),
  KEY `scenario_id` (`scenario_id`),
  CONSTRAINT `scenarios_scopes_ibfk_1` FOREIGN KEY (`scenario_id`) REFERENCES `scenarios` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios_scopes`
--

LOCK TABLES `scenarios_scopes` WRITE;
/*!40000 ALTER TABLE `scenarios_scopes` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios_scopes` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `scenarios_users`
--

DROP TABLE IF EXISTS `scenarios_users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `scenarios_users` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `scenario_id` bigint(20) NOT NULL,
  `username` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_group` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `scenario_id` (`scenario_id`),
  KEY `username` (`username`),
  KEY `user_group` (`user_group`),
  CONSTRAINT `scenarios_users_ibfk_1` FOREIGN KEY (`scenario_id`) REFERENCES `scenarios` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `scenarios_users`
--

LOCK TABLES `scenarios_users` WRITE;
/*!40000 ALTER TABLE `scenarios_users` DISABLE KEYS */;
/*!40000 ALTER TABLE `scenarios_users` ENABLE KEYS */;
UNLOCK TABLES;



--
-- Temporary table structure for view `service_daily_ins_vw`
--

DROP TABLE IF EXISTS `service_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `service_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `service_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `service_daily_upd_vw`
--

DROP TABLE IF EXISTS `service_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `service_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `service_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `service_monthly_ins_vw`
--

DROP TABLE IF EXISTS `service_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `service_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `service_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `service_monthly_upd_vw`
--

DROP TABLE IF EXISTS `service_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `service_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `service_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `service_spend_by_day`
--

DROP TABLE IF EXISTS `service_spend_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `service_spend_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hash` bigint(20) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  CONSTRAINT `service_spend_by_day_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `cloud_services` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `service_spend_by_day`
--

LOCK TABLES `service_spend_by_day` WRITE;
/*!40000 ALTER TABLE `service_spend_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `service_spend_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `service_spend_by_hour`
--

DROP TABLE IF EXISTS `service_spend_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `service_spend_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hash` bigint(20) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  CONSTRAINT `service_spend_by_hour_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `cloud_services` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `service_spend_by_hour`
--

LOCK TABLES `service_spend_by_hour` WRITE;
/*!40000 ALTER TABLE `service_spend_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `service_spend_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `service_spend_by_month`
--

DROP TABLE IF EXISTS `service_spend_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `service_spend_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  CONSTRAINT `service_spend_by_month_ibfk_1` FOREIGN KEY (`uuid`) REFERENCES `cloud_services` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `service_spend_by_month`
--

LOCK TABLES `service_spend_by_month` WRITE;
/*!40000 ALTER TABLE `service_spend_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `service_spend_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `spend_stats_by_day`
--

DROP TABLE IF EXISTS `spend_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `spend_stats_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `scope_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `related_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subvalue` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `n_entities` int(5) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`scope_type`,`uuid`,`provider_uuid`,`property_type`,`property_subtype`,`property_subvalue`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `spend_stats_by_day`
--

LOCK TABLES `spend_stats_by_day` WRITE;
/*!40000 ALTER TABLE `spend_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `spend_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `spend_stats_by_hour`
--

DROP TABLE IF EXISTS `spend_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `spend_stats_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `scope_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `related_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subvalue` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `n_entities` int(5) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`scope_type`,`uuid`,`provider_uuid`,`property_type`,`property_subtype`,`property_subvalue`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `spend_stats_by_hour`
--

LOCK TABLES `spend_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `spend_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `spend_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `spend_stats_by_month`
--

DROP TABLE IF EXISTS `spend_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `spend_stats_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `scope_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `related_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subvalue` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `n_entities` int(5) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`scope_type`,`uuid`,`provider_uuid`,`property_type`,`property_subtype`,`property_subvalue`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `spend_stats_by_month`
--

LOCK TABLES `spend_stats_by_month` WRITE;
/*!40000 ALTER TABLE `spend_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `spend_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `standard_reports`
--

DROP TABLE IF EXISTS `standard_reports`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `standard_reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `filename` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `title` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `category` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `short_desc` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` mediumtext COLLATE utf8_unicode_ci,
  `period` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `day_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=191 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `standard_reports`
--

LOCK TABLES `standard_reports` WRITE;
/*!40000 ALTER TABLE `standard_reports` DISABLE KEYS */;
INSERT INTO `standard_reports` VALUES (140,'daily_infra_30_days_avg_stats_vs_thresholds_grid','Host/VM - 30 Days Stats vs Thresholds','Capacity Management/Hosts','30 days average utilization stats vs thresholds','This report shows daily average utilization measurements for the past 30 days. Averages are grouped into two sections. One rolled up for physical machines and another rolled up for virtual machines.','Daily',NULL),(145,'daily_infra_60_days_hosting_bar','Hosting Summary by Physical Machine','Workload Balancing','Infra 60 Day Hosting Ratios','This report shows the ratio of VMs per Host over the prior 60 days, and shows the most recent measured hosting levels for each host.','Daily',NULL),(146,'monthly_cluster_summary','Monthly Overview with Cluster Summary','Capacity Management/Hosts','Monthly Overview with Cluster Summary','This report shows monthly statistics for the enterprise as a whole, and a monthly summary for each cluster.','Monthly',NULL),(147,'monthly_summary','Monthly Overview','Capacity Management/Hosts','Monthly Overview','This report shows monthly statistics for the enterprise.','Monthly',NULL),(148,'daily_vm_over_under_prov_grid','VM Over/Under Provisioning 90 Days','Capacity Management/VMs','VM Over/Under Provisioning 90 Days','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory','Daily',NULL),(150,'daily_vm_rightsizing_advice_grid','VM Rightsizing Recommendations','Capacity Management/VMs','VM Rightsizing Recommendations','This report shows rightsizing recommendations for Virtual Machines based on time range and threshold settings','Daily',NULL),(153,'daily_infra_utilization_day_over_day_by_hour_line','Utilization Day Over Day by Hour','Performance Management/Enterprise','Utilization Day Over Day by Hour','This report shows average hourly utilization measurements compared day-over-day for the two days indicated. All machines in the infrastructure are included in the averages.','Daily',NULL),(154,'daily_infra_utilization_levels_by_hour_line','Utilization Levels by Hour','Performance Management/Enterprise','Infra Utilization Levels by Hour','This report shows by-hour averages for CPU and Memory utlization measurements for Physical and Virtual machines. Averages are rolled up for all machines.','Daily',NULL),(155,'daily_pm_vm_top_bottom_utilized_grid','Host/VM - Top Bottom Utilized','Performance Management/Hosts','Host/VM Top Bottom Utilized','This report shows the top and bottom physical and virtual machines by their Price Index measurements. Price Index is a derived value that reflects the consumption levels of key compute resources taken in combination. Larger values indicate machines on which resources are most utilized.','Daily',NULL),(156,'daily_pm_vm_utilization_heatmap_grid','Host/VM - Resource Utilization Heatmap','Capacity Management/Hosts','Host/VM Utilization Heatmap','This report shows the hottest physical and virtual machines according to their Price Index measurements.  Price Index is a derived value that reflects the consumption levels of key compute resources taken in combination. Larger values indicate machines on which resources are most utilized.','Daily',NULL),(160,'weekly_infra_utilization_week_over_week_by_day_line','Utilization Week Over Week by Day','Performance Management/Enterprise','Infra Utilization Week Over Week by Day','This report shows average daily utilization measurements compared week-over-week for the two weeks indicated. All machines in the infrastructure are included in the averages.','Weekly',NULL),(165,'daily_pm_top_bottom_capacity_grid','Host Top Bottom Capacity','Capacity Management/Hosts','Host Top Bottom Capacity','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','Daily',NULL),(166,'daily_vm_top_bottom_capacity_grid','VM Top Bottom Capacity','Capacity Management/VMs','VM Top Bottom Capacity','Shows a capacity-centered view of VCPU and VMemory utilization, providing actual quantities both used and in reserve.  ','Daily',NULL),(167,'daily_storage_top_bottom_capacity_grid','Storage Top Bottom Capacity','Capacity Management/Storage','Storage Top Bottom Capacity','This report shows the top and bottom data stores based on their level of consumption as of the date indicated.','Daily',NULL),(168,'daily_pm_top_cpu_ready_queue_grid','Host Top CPU Ready Queue','Performance Management/Hosts','Host Top CPU Ready Queue','Shows top machines by CPU ready queue utilization ','Daily',NULL),(169,'daily_pm_top_resource_utilization_bar','Top Host Utilization by Resource','Performance Management/Hosts','Top Host Utilization by Resource','Shows top machines by utilization of each resource type','Daily',NULL),(170,'daily_cluster_30_days_avg_stats_vs_thresholds_grid','Host/VM Groups - 30 Day Stats vs Thresholds','Capacity Management/Hosts','Machine Groups 30 Day Avg Stats vs Thresholds','Shows a roll-up of average utilization values per day for 30 days for each grouping of machines.  Values are highlighted where they cross certain threshold values that may indicate a condition of over utilization of resources. ','Daily',NULL),(172,'daily_potential_storage_waste_grid','Storage Associated with Dormant VMs','Capacity Management/Storage','Storage Associated with Dormant VMs','This report shows the amounts of storage used by virtual machines that have been dormant for several days.  This storage has the potential to be reclaimed, depending upon the final disposition of such VMs. ','Daily',NULL),(173,'daily_digest_storage_top_disk_grid','Storage Top Disk Consumption','Capacity Management/Storage','Storage Top Disk','This report presents two independent views of information related to VDisk storage. The first highlights data stores that possess the largest amounts of VDisk storage.  The second lists Virtual Machines organized most to least in terms of their consumption of VDisk storage. ','Daily',NULL),(175,'daily_digest_storage_top_log_grid','Storage Top Log Consumption','Capacity Management/Storage','Storage Top Log','This report presents two independent views of information related to Log storage. The first highlights data stores that possess the largest amounts of Log storage.  The second lists Virtual Machines organized most to least in terms of their consumption of Log storage. ','Daily',NULL),(176,'daily_digest_storage_top_snapshot_grid','Storage Top Snapshot Consumption','Capacity Management/Storage','Storage Top Snapshot','This report presents two independent views of information related to Snapshot storage. The first highlights data stores that possess the largest amounts of Snapshot storage.  The second lists Virtual Machines organized most to least in terms of their consumption of Snapshot storage. ','Daily',NULL),(177,'daily_digest_storage_top_swap_grid','Storage Top Swap Consumption','Capacity Management/Storage','Storage Top Swap','This report presents two independent views of information related to Swap storage. The first highlights data stores that possess the largest amounts of Swap storage.  The second lists Virtual Machines organized most to least in terms of their consumption of Swap storage. ','Daily',NULL),(178,'daily_digest_storage_top_unused_grid','Storage Wasted Allocations','Capacity Management/Storage','Storage Waste','This report presents data stores that possess the largest amounts of wasted storage.  Wasted storage is taken as storage used for purposes not directly associated with supporting the operation of a VM.','Daily',NULL),(179,'daily_digest_storage_top_latency_grid','Storage Access IO Latency','Performance Management/Storage','Digest Storage Top Latency','This report presents two independent views of information related to Latency in accessing storage. The first highlights data stores having the largest amounts of storage access latency.  The second lists Virtual Machines organized most to least in terms of latency in accessing storage. ','Daily',NULL),(180,'daily_digest_storage_top_iops_grid','Storage Access IOPS','Performance Management/Storage','Digest Storage Top Latency','This report presents two independent views of information related to IOPS for storage. The first highlights data stores that possess the largest usage of IOPS.  The second lists Virtual Machines organized most to least in terms of their use of IOPS related to storage. ','Daily',NULL),(181,'monthly_individual_vm_summary','Individual VM Monthly Summary','Capacity Management/VMs','End of the month CPU, Memory, Storage summary','This report shows a monthly summary of Virtual Machines CPU, Memory and Storage measurements','Monthly',NULL),(182,'monthly_30_days_pm_top_bottom_capacity_grid','Monthly Top Bottom 15 Hosts Capacity','Capacity Management/Hosts','Host 15 Top Bottom Capacity','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','Monthly',NULL),(183,'monthly_30_days_vm_top_bottom_capacity_grid','Monthly Top Bottom 15 VMs Capacity','Capacity Management/VMs','VM 15 Top Bottom Capacity','Shows a capacity-centered view of CPU and Memory utilization, providing actual quantities both used and in reserve.  ','Monthly',NULL),(184,'daily_vm_over_under_prov_grid_30_days','VM Over/Under Provisioning 30 Days','Capacity Management/VMs','VM Over/Under Provisioning 30 Days','This report shows Virtual Machines that are over and under provisioned as indicated by their utilization of CPU and Memory','Daily',NULL),(185,'daily_digest_storage_top_latency_grid_30_days','Storage Access IO Latency - 30 Days','Performance Management/Storage','Digest Storage Top Latency','This report presents two independent views of information related to Latency in accessing storage. The first highlights data stores having the largest amounts of storage access latency.  The second lists Virtual Machines organized most to least in terms of latency in accessing storage. ','Daily',NULL),(186,'daily_digest_storage_top_iops_grid_30_days','Storage Access IOPS - 30 Days','Performance Management/Storage','Digest Storage Top Latency','This report presents two independent views of information related to IOPS for storage. The first highlights data stores that possess the largest usage of IOPS.  The second lists Virtual Machines organized most to least in terms of their use of IOPS related to storage. ','Daily',NULL),(187,'daily_digest_storage_top_latency_grid_7_days','Storage Access IO Latency - 7 Days','Performance Management/Storage','Digest Storage Top Latency','This report presents two independent views of information related to Latency in accessing storage. The first highlights data stores having the largest amounts of storage access latency.  The second lists Virtual Machines organized most to least in terms of latency in accessing storage. ','Daily',NULL),(188,'daily_digest_storage_top_iops_grid_7_days','Storage Access IOPS - 7 Days','Performance Management/Storage','Digest Storage Top Latency','This report presents two independent views of information related to IOPS for storage. The first highlights data stores that possess the largest usage of IOPS.  The second lists Virtual Machines organized most to least in terms of their use of IOPS related to storage. ','Daily',NULL),(189,'weekly_socket_audit_report','Weekly Socket Audit Report','Capacity Management/Hosts','Socket Audit Report Weekly','This report gives a socket count per PM for licensing purposes ','Weekly',NULL),(190,'monthly_VMem_capacity_cost','Monthly VMem Capacity','Capacity Management/VMs','VMem Capacity','This report shows the VMem capacity over the last month, that was assigned to the VMs in the environment. Where the capacity for a VM was increased or decreased during the month, the higher value is used for the entire month. This report also lists all the clusters that were active in the month.','Monthly',NULL);
/*!40000 ALTER TABLE `standard_reports` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `subscription_attrs`
--

DROP TABLE IF EXISTS `subscription_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `subscription_attrs` (
  `subscription_id` int(11) NOT NULL,
  `report_attr_id` int(11) NOT NULL,
  `value` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`subscription_id`,`report_attr_id`),
  KEY `report_attr_id` (`report_attr_id`),
  KEY `subscription_id` (`subscription_id`),
  CONSTRAINT `subscription_attrs_ibfk_1` FOREIGN KEY (`subscription_id`) REFERENCES `report_subscriptions` (`id`) ON DELETE CASCADE,
  CONSTRAINT `subscription_attrs_ibfk_2` FOREIGN KEY (`report_attr_id`) REFERENCES `report_attrs` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `subscription_attrs`
--

LOCK TABLES `subscription_attrs` WRITE;
/*!40000 ALTER TABLE `subscription_attrs` DISABLE KEYS */;
/*!40000 ALTER TABLE `subscription_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `supply_chain_stats_by_day`
--

DROP TABLE IF EXISTS `supply_chain_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `supply_chain_stats_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `scope_type` varchar(8) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `value` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `supply_chain_stats_by_day`
--

LOCK TABLES `supply_chain_stats_by_day` WRITE;
/*!40000 ALTER TABLE `supply_chain_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `supply_chain_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `supply_chain_stats_by_month`
--

DROP TABLE IF EXISTS `supply_chain_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `supply_chain_stats_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `scope_type` varchar(8) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `value` int(11) DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `supply_chain_stats_by_month`
--

LOCK TABLES `supply_chain_stats_by_month` WRITE;
/*!40000 ALTER TABLE `supply_chain_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `supply_chain_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `sw_daily_ins_vw`
--

DROP TABLE IF EXISTS `sw_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `sw_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sw_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sw_daily_upd_vw`
--

DROP TABLE IF EXISTS `sw_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `sw_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sw_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sw_hourly_ins_vw`
--

DROP TABLE IF EXISTS `sw_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `sw_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sw_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sw_hourly_upd_vw`
--

DROP TABLE IF EXISTS `sw_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `sw_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sw_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sw_monthly_ins_vw`
--

DROP TABLE IF EXISTS `sw_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `sw_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sw_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `sw_monthly_upd_vw`
--

DROP TABLE IF EXISTS `sw_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `sw_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `sw_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `sw_stats_by_day`
--

DROP TABLE IF EXISTS `sw_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sw_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sw_stats_by_day`
--

LOCK TABLES `sw_stats_by_day` WRITE;
/*!40000 ALTER TABLE `sw_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `sw_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sw_stats_by_hour`
--

DROP TABLE IF EXISTS `sw_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sw_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sw_stats_by_hour`
--

LOCK TABLES `sw_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `sw_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `sw_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sw_stats_by_month`
--

DROP TABLE IF EXISTS `sw_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sw_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sw_stats_by_month`
--

LOCK TABLES `sw_stats_by_month` WRITE;
/*!40000 ALTER TABLE `sw_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `sw_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `sw_stats_latest`
--

DROP TABLE IF EXISTS `sw_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `sw_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `sw_stats_latest`
--

LOCK TABLES `sw_stats_latest` WRITE;
/*!40000 ALTER TABLE `sw_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `sw_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;

/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_sw_primary_keys BEFORE INSERT ON sw_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;


--
-- Table structure for table `system_load`
--

DROP TABLE IF EXISTS `system_load`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `system_load` (
  `slice` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`,`uuid`,`property_type`,`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `system_load`
--

LOCK TABLES `system_load` WRITE;
/*!40000 ALTER TABLE `system_load` DISABLE KEYS */;
/*!40000 ALTER TABLE `system_load` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `ui_reports`
--

DROP TABLE IF EXISTS `ui_reports`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `ui_reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `filename` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `username` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` mediumtext COLLATE utf8_unicode_ci,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `ui_reports`
--

LOCK TABLES `ui_reports` WRITE;
/*!40000 ALTER TABLE `ui_reports` DISABLE KEYS */;
/*!40000 ALTER TABLE `ui_reports` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `user_attrs`
--

DROP TABLE IF EXISTS `user_attrs`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_attrs` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `value` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_user_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_attrs_ibfk_1` (`user_user_id`),
  CONSTRAINT `user_attrs_ibfk_1` FOREIGN KEY (`user_user_id`) REFERENCES `users` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user_attrs`
--

LOCK TABLES `user_attrs` WRITE;
/*!40000 ALTER TABLE `user_attrs` DISABLE KEYS */;
/*!40000 ALTER TABLE `user_attrs` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `user_ds_stats_by_day`
--

DROP TABLE IF EXISTS `user_ds_stats_by_day`;
/*!50001 DROP VIEW IF EXISTS `user_ds_stats_by_day`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_ds_stats_by_day` (
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `user_ds_stats_by_hour`
--

DROP TABLE IF EXISTS `user_ds_stats_by_hour`;
/*!50001 DROP VIEW IF EXISTS `user_ds_stats_by_hour`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_ds_stats_by_hour` (
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `user_pm_stats_by_day`
--

DROP TABLE IF EXISTS `user_pm_stats_by_day`;
/*!50001 DROP VIEW IF EXISTS `user_pm_stats_by_day`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_pm_stats_by_day` (
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `user_pm_stats_by_hour`
--

DROP TABLE IF EXISTS `user_pm_stats_by_hour`;
/*!50001 DROP VIEW IF EXISTS `user_pm_stats_by_hour`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_pm_stats_by_hour` (
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `user_reports`
--

DROP TABLE IF EXISTS `user_reports`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_reports` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `title` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `category` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `short_desc` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `description` mediumtext COLLATE utf8_unicode_ci,
  `xml_descriptor` mediumtext COLLATE utf8_unicode_ci,
  `period` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `day_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `username` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user_reports`
--

LOCK TABLES `user_reports` WRITE;
/*!40000 ALTER TABLE `user_reports` DISABLE KEYS */;
/*!40000 ALTER TABLE `user_reports` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `user_roles`
--

DROP TABLE IF EXISTS `user_roles`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `user_roles` (
  `id` tinyint(3) NOT NULL,
  `role` varchar(30) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `user_roles`
--

LOCK TABLES `user_roles` WRITE;
/*!40000 ALTER TABLE `user_roles` DISABLE KEYS */;
INSERT INTO `user_roles` VALUES (0,'ADMINISTRATOR'),(1,'AUTOMATOR'),(2,'ADVISOR'),(3,'OBSERVER'),(7,'DEPLOYER');
/*!40000 ALTER TABLE `user_roles` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `user_vm_stats_by_day`
--

DROP TABLE IF EXISTS `user_vm_stats_by_day`;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_day`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_vm_stats_by_day` (
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `user_vm_stats_by_day_per_group`
--

DROP TABLE IF EXISTS `user_vm_stats_by_day_per_group`;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_day_per_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_vm_stats_by_day_per_group` (
  `group_name` tinyint NOT NULL,
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `user_vm_stats_by_hour`
--

DROP TABLE IF EXISTS `user_vm_stats_by_hour`;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_hour`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_vm_stats_by_hour` (
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `user_vm_stats_by_hour_per_group`
--

DROP TABLE IF EXISTS `user_vm_stats_by_hour_per_group`;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_hour_per_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `user_vm_stats_by_hour_per_group` (
  `group_name` tinyint NOT NULL,
  `instance_name` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `users` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `username` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `password` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `displayName` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `userType` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `loginProvider` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `uuid` (`uuid`),
  KEY `username` (`username`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vc_licenses`
--

DROP TABLE IF EXISTS `vc_licenses`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vc_licenses` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `target` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `product` varchar(250) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` int(11) DEFAULT NULL,
  `used` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vc_licenses`
--

LOCK TABLES `vc_licenses` WRITE;
/*!40000 ALTER TABLE `vc_licenses` DISABLE KEYS */;
/*!40000 ALTER TABLE `vc_licenses` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `vdc_daily_ins_vw`
--

DROP TABLE IF EXISTS `vdc_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vdc_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vdc_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vdc_daily_upd_vw`
--

DROP TABLE IF EXISTS `vdc_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vdc_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vdc_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vdc_hourly_ins_vw`
--

DROP TABLE IF EXISTS `vdc_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vdc_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vdc_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vdc_hourly_upd_vw`
--

DROP TABLE IF EXISTS `vdc_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vdc_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vdc_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vdc_monthly_ins_vw`
--

DROP TABLE IF EXISTS `vdc_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vdc_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vdc_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vdc_monthly_upd_vw`
--

DROP TABLE IF EXISTS `vdc_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vdc_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vdc_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `vdc_stats_by_day`
--

DROP TABLE IF EXISTS `vdc_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vdc_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vdc_stats_by_day`
--

LOCK TABLES `vdc_stats_by_day` WRITE;
/*!40000 ALTER TABLE `vdc_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `vdc_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vdc_stats_by_hour`
--

DROP TABLE IF EXISTS `vdc_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vdc_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vdc_stats_by_hour`
--

LOCK TABLES `vdc_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `vdc_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `vdc_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vdc_stats_by_month`
--

DROP TABLE IF EXISTS `vdc_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vdc_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vdc_stats_by_month`
--

LOCK TABLES `vdc_stats_by_month` WRITE;
/*!40000 ALTER TABLE `vdc_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `vdc_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vdc_stats_latest`
--

DROP TABLE IF EXISTS `vdc_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vdc_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vdc_stats_latest`
--

LOCK TABLES `vdc_stats_latest` WRITE;
/*!40000 ALTER TABLE `vdc_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `vdc_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_vdc_primary_keys BEFORE INSERT ON vdc_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `version_info`
--

DROP TABLE IF EXISTS `version_info`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `version_info` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `version` double(5,1) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `version_info`
--

LOCK TABLES `version_info` WRITE;
/*!40000 ALTER TABLE `version_info` DISABLE KEYS */;
INSERT INTO `version_info` VALUES (1,73.7);
/*!40000 ALTER TABLE `version_info` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `vm_capacity_by_day`
--

DROP TABLE IF EXISTS `vm_capacity_by_day`;
/*!50001 DROP VIEW IF EXISTS `vm_capacity_by_day`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_capacity_by_day` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_capacity_by_day_per_vm_group`
--

DROP TABLE IF EXISTS `vm_capacity_by_day_per_vm_group`;
/*!50001 DROP VIEW IF EXISTS `vm_capacity_by_day_per_vm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_capacity_by_day_per_vm_group` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_capacity_by_hour`
--

DROP TABLE IF EXISTS `vm_capacity_by_hour`;
/*!50001 DROP VIEW IF EXISTS `vm_capacity_by_hour`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_capacity_by_hour` (
  `class_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `utilization` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `used_capacity` tinyint NOT NULL,
  `available_capacity` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `hour_number` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_daily_ins_vw`
--

DROP TABLE IF EXISTS `vm_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vm_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_daily_upd_vw`
--

DROP TABLE IF EXISTS `vm_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vm_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_group_assns`
--

DROP TABLE IF EXISTS `vm_group_assns`;
/*!50001 DROP VIEW IF EXISTS `vm_group_assns`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_group_assns` (
  `assn_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_group_members`
--

DROP TABLE IF EXISTS `vm_group_members`;
/*!50001 DROP VIEW IF EXISTS `vm_group_members`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_group_members` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_group_members_agg`
--

DROP TABLE IF EXISTS `vm_group_members_agg`;
/*!50001 DROP VIEW IF EXISTS `vm_group_members_agg`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_group_members_agg` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_group_members_helper`
--

DROP TABLE IF EXISTS `vm_group_members_helper`;
/*!50001 DROP VIEW IF EXISTS `vm_group_members_helper`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_group_members_helper` (
  `entity_dest_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_groups`
--

DROP TABLE IF EXISTS `vm_groups`;
/*!50001 DROP VIEW IF EXISTS `vm_groups`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_groups` (
  `entity_id` tinyint NOT NULL,
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_hourly_ins_vw`
--

DROP TABLE IF EXISTS `vm_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vm_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_hourly_upd_vw`
--

DROP TABLE IF EXISTS `vm_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vm_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_instances`
--

DROP TABLE IF EXISTS `vm_instances`;
/*!50001 DROP VIEW IF EXISTS `vm_instances`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_instances` (
  `name` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_monthly_ins_vw`
--

DROP TABLE IF EXISTS `vm_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vm_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_monthly_upd_vw`
--

DROP TABLE IF EXISTS `vm_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vm_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `provider_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `rate` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `vm_spend_by_day`
--

DROP TABLE IF EXISTS `vm_spend_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_spend_by_day` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hash` bigint(20) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_spend_by_day`
--

LOCK TABLES `vm_spend_by_day` WRITE;
/*!40000 ALTER TABLE `vm_spend_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_spend_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vm_spend_by_hour`
--

DROP TABLE IF EXISTS `vm_spend_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_spend_by_hour` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hash` bigint(20) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_spend_by_hour`
--

LOCK TABLES `vm_spend_by_hour` WRITE;
/*!40000 ALTER TABLE `vm_spend_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_spend_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vm_spend_by_month`
--

DROP TABLE IF EXISTS `vm_spend_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_spend_by_month` (
  `snapshot_time` date DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `provider_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `rate` decimal(20,7) DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `artifact_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_spend_by_month`
--

LOCK TABLES `vm_spend_by_month` WRITE;
/*!40000 ALTER TABLE `vm_spend_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_spend_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vm_stats_by_day`
--

DROP TABLE IF EXISTS `vm_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_stats_by_day`
--

LOCK TABLES `vm_stats_by_day` WRITE;
/*!40000 ALTER TABLE `vm_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `vm_stats_by_day_per_vm_group`
--

DROP TABLE IF EXISTS `vm_stats_by_day_per_vm_group`;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_day_per_vm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_stats_by_day_per_vm_group` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_stats_by_day_per_vm_group_agg`
--

DROP TABLE IF EXISTS `vm_stats_by_day_per_vm_group_agg`;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_day_per_vm_group_agg`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_stats_by_day_per_vm_group_agg` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `vm_stats_by_hour`
--

DROP TABLE IF EXISTS `vm_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_stats_by_hour`
--

LOCK TABLES `vm_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `vm_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary table structure for view `vm_stats_by_hour_per_vm_group`
--

DROP TABLE IF EXISTS `vm_stats_by_hour_per_vm_group`;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_hour_per_vm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_stats_by_hour_per_vm_group` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_stats_by_hour_per_vm_group_agg`
--

DROP TABLE IF EXISTS `vm_stats_by_hour_per_vm_group_agg`;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_hour_per_vm_group_agg`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_stats_by_hour_per_vm_group_agg` (
  `group_uuid` tinyint NOT NULL,
  `internal_name` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `member_uuid` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `vm_stats_by_month`
--

DROP TABLE IF EXISTS `vm_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_stats_by_month`
--

LOCK TABLES `vm_stats_by_month` WRITE;
/*!40000 ALTER TABLE `vm_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vm_stats_latest`
--

DROP TABLE IF EXISTS `vm_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vm_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vm_stats_latest`
--

LOCK TABLES `vm_stats_latest` WRITE;
/*!40000 ALTER TABLE `vm_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `vm_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_vm_primary_keys BEFORE INSERT ON vm_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Temporary table structure for view `vm_storage_used_by_day_per_vm_group`
--

DROP TABLE IF EXISTS `vm_storage_used_by_day_per_vm_group`;
/*!50001 DROP VIEW IF EXISTS `vm_storage_used_by_day_per_vm_group`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_storage_used_by_day_per_vm_group` (
  `group_uuid` tinyint NOT NULL,
  `group_name` tinyint NOT NULL,
  `group_type` tinyint NOT NULL,
  `recorded_on` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `storage_used` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_util_info_yesterday`
--

DROP TABLE IF EXISTS `vm_util_info_yesterday`;
/*!50001 DROP VIEW IF EXISTS `vm_util_info_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_util_info_yesterday` (
  `snapshot_time` tinyint NOT NULL,
  `display_name` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vm_util_stats_yesterday`
--

DROP TABLE IF EXISTS `vm_util_stats_yesterday`;
/*!50001 DROP VIEW IF EXISTS `vm_util_stats_yesterday`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vm_util_stats_yesterday` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vpod_daily_ins_vw`
--

DROP TABLE IF EXISTS `vpod_daily_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vpod_daily_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vpod_daily_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vpod_daily_upd_vw`
--

DROP TABLE IF EXISTS `vpod_daily_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vpod_daily_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vpod_daily_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vpod_hourly_ins_vw`
--

DROP TABLE IF EXISTS `vpod_hourly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vpod_hourly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vpod_hourly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vpod_hourly_upd_vw`
--

DROP TABLE IF EXISTS `vpod_hourly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vpod_hourly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vpod_hourly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vpod_monthly_ins_vw`
--

DROP TABLE IF EXISTS `vpod_monthly_ins_vw`;
/*!50001 DROP VIEW IF EXISTS `vpod_monthly_ins_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vpod_monthly_ins_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Temporary table structure for view `vpod_monthly_upd_vw`
--

DROP TABLE IF EXISTS `vpod_monthly_upd_vw`;
/*!50001 DROP VIEW IF EXISTS `vpod_monthly_upd_vw`*/;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8;
/*!50001 CREATE TABLE `vpod_monthly_upd_vw` (
  `snapshot_time` tinyint NOT NULL,
  `uuid` tinyint NOT NULL,
  `producer_uuid` tinyint NOT NULL,
  `property_type` tinyint NOT NULL,
  `property_subtype` tinyint NOT NULL,
  `relation` tinyint NOT NULL,
  `commodity_key` tinyint NOT NULL,
  `capacity` tinyint NOT NULL,
  `min_value` tinyint NOT NULL,
  `max_value` tinyint NOT NULL,
  `avg_value` tinyint NOT NULL,
  `samples` tinyint NOT NULL,
  `new_samples` tinyint NOT NULL
) ENGINE=MyISAM */;
SET character_set_client = @saved_cs_client;

--
-- Table structure for table `vpod_stats_by_day`
--

DROP TABLE IF EXISTS `vpod_stats_by_day`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vpod_stats_by_day` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`day_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vpod_stats_by_day`
--

LOCK TABLES `vpod_stats_by_day` WRITE;
/*!40000 ALTER TABLE `vpod_stats_by_day` DISABLE KEYS */;
/*!40000 ALTER TABLE `vpod_stats_by_day` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vpod_stats_by_hour`
--

DROP TABLE IF EXISTS `vpod_stats_by_hour`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vpod_stats_by_hour` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `new_samples` int(11) DEFAULT NULL,
  `hour_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  PRIMARY KEY (`hour_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vpod_stats_by_hour`
--

LOCK TABLES `vpod_stats_by_hour` WRITE;
/*!40000 ALTER TABLE `vpod_stats_by_hour` DISABLE KEYS */;
/*!40000 ALTER TABLE `vpod_stats_by_hour` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vpod_stats_by_month`
--

DROP TABLE IF EXISTS `vpod_stats_by_month`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vpod_stats_by_month` (
  `snapshot_time` datetime NOT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `samples` int(11) DEFAULT NULL,
  `new_samples` int(11) DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 NOT NULL,
  PRIMARY KEY (`month_key`,`snapshot_time`),
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vpod_stats_by_month`
--

LOCK TABLES `vpod_stats_by_month` WRITE;
/*!40000 ALTER TABLE `vpod_stats_by_month` DISABLE KEYS */;
/*!40000 ALTER TABLE `vpod_stats_by_month` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `vpod_stats_latest`
--

DROP TABLE IF EXISTS `vpod_stats_latest`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `vpod_stats_latest` (
  `snapshot_time` datetime DEFAULT NULL,
  `uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `producer_uuid` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_type` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `property_subtype` varchar(36) COLLATE utf8_unicode_ci DEFAULT NULL,
  `capacity` decimal(15,3) DEFAULT NULL,
  `avg_value` decimal(15,3) DEFAULT NULL,
  `min_value` decimal(15,3) DEFAULT NULL,
  `max_value` decimal(15,3) DEFAULT NULL,
  `relation` tinyint(3) DEFAULT NULL,
  `commodity_key` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `aggregated` tinyint(1) NOT NULL DEFAULT '0',
  `hour_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `day_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  `month_key` varchar(32) CHARACTER SET utf8 DEFAULT NULL,
  KEY `snapshot_time` (`snapshot_time`),
  KEY `uuid` (`uuid`),
  KEY `property_type` (`property_type`),
  KEY `property_subtype` (`property_subtype`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
  /*!50500 PARTITION BY RANGE (to_seconds(snapshot_time))
(PARTITION `start` VALUES LESS THAN (0) ENGINE = InnoDB,
PARTITION future VALUES LESS THAN MAXVALUE ENGINE = InnoDB) */;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `vpod_stats_latest`
--

LOCK TABLES `vpod_stats_latest` WRITE;
/*!40000 ALTER TABLE `vpod_stats_latest` DISABLE KEYS */;
/*!40000 ALTER TABLE `vpod_stats_latest` ENABLE KEYS */;
UNLOCK TABLES;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=CURRENT_USER*/ /*!50003 TRIGGER set_vpod_primary_keys BEFORE INSERT ON vpod_stats_latest
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
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Table structure for table `widget_element_datasets`
--

DROP TABLE IF EXISTS `widget_element_datasets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `widget_element_datasets` (
  `query_url` varchar(200) COLLATE utf8_unicode_ci DEFAULT NULL,
  `widget_element_id` bigint(20) DEFAULT NULL,
  KEY `widget_element_id` (`widget_element_id`),
  CONSTRAINT `widget_element_datasets_ibfk_1` FOREIGN KEY (`widget_element_id`) REFERENCES `widget_elements` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `widget_element_datasets`
--

LOCK TABLES `widget_element_datasets` WRITE;
/*!40000 ALTER TABLE `widget_element_datasets` DISABLE KEYS */;
/*!40000 ALTER TABLE `widget_element_datasets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `widget_element_properties`
--

DROP TABLE IF EXISTS `widget_element_properties`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `widget_element_properties` (
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `value` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `widget_element_id` bigint(20) DEFAULT NULL,
  KEY `widget_element_id` (`widget_element_id`),
  CONSTRAINT `widget_element_properties_ibfk_1` FOREIGN KEY (`widget_element_id`) REFERENCES `widget_elements` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `widget_element_properties`
--

LOCK TABLES `widget_element_properties` WRITE;
/*!40000 ALTER TABLE `widget_element_properties` DISABLE KEYS */;
/*!40000 ALTER TABLE `widget_element_properties` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `widget_elements`
--

DROP TABLE IF EXISTS `widget_elements`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `widget_elements` (
  `id` bigint(20) NOT NULL,
  `row` tinyint(3) DEFAULT NULL,
  `column` tinyint(3) DEFAULT NULL,
  `type` varchar(20) COLLATE utf8_unicode_ci DEFAULT NULL,
  `widget_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `widget_id` (`widget_id`),
  CONSTRAINT `widget_elements_ibfk_1` FOREIGN KEY (`widget_id`) REFERENCES `widgets` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `widget_elements`
--

LOCK TABLES `widget_elements` WRITE;
/*!40000 ALTER TABLE `widget_elements` DISABLE KEYS */;
/*!40000 ALTER TABLE `widget_elements` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `widgets`
--

DROP TABLE IF EXISTS `widgets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `widgets` (
  `id` bigint(20) NOT NULL,
  `title` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `scope` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `start_period` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  `end_period` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  `row` tinyint(3) DEFAULT NULL,
  `column` tinyint(3) DEFAULT NULL,
  `size_rows` tinyint(3) DEFAULT NULL,
  `size_columns` tinyint(3) DEFAULT NULL,
  `type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `widgetset_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `widgetset_id` (`widgetset_id`),
  CONSTRAINT `widgets_ibfk_2` FOREIGN KEY (`widgetset_id`) REFERENCES `widgetsets` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `widgets`
--

LOCK TABLES `widgets` WRITE;
/*!40000 ALTER TABLE `widgets` DISABLE KEYS */;
/*!40000 ALTER TABLE `widgets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `widgetsets`
--

DROP TABLE IF EXISTS `widgetsets`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `widgetsets` (
  `id` bigint(20) NOT NULL,
  `name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `scope` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `start_period` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  `end_period` varchar(10) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_name` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `category` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `scope_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `is_shared_with_all_users` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `widgetsets`
--

LOCK TABLES `widgetsets` WRITE;
/*!40000 ALTER TABLE `widgetsets` DISABLE KEYS */;
/*!40000 ALTER TABLE `widgetsets` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `widgetsets_default`
--

DROP TABLE IF EXISTS `widgetsets_default`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `widgetsets_default` (
  `id` bigint(20) NOT NULL,
  `scope_type` varchar(80) COLLATE utf8_unicode_ci DEFAULT NULL,
  `user_role_id` tinyint(3) DEFAULT NULL,
  `widgetset_id` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `widgetset_id` (`widgetset_id`),
  KEY `user_role_id` (`user_role_id`),
  CONSTRAINT `widgetsets_default_ibfk_1` FOREIGN KEY (`widgetset_id`) REFERENCES `widgetsets` (`id`) ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT `widgetsets_default_ibfk_2` FOREIGN KEY (`user_role_id`) REFERENCES `user_roles` (`id`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;

--
-- Dumping data for table `widgetsets_default`
--

LOCK TABLES `widgetsets_default` WRITE;
/*!40000 ALTER TABLE `widgetsets_default` DISABLE KEYS */;
/*!40000 ALTER TABLE `widgetsets_default` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Dumping events for database 'vmtdb'
--
/*!50106 SET @save_time_zone= @@TIME_ZONE */ ;
/*!50106 DROP EVENT IF EXISTS `aggregate_cluster_event` */;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `aggregate_cluster_event` ON SCHEDULE EVERY 10 MINUTE STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call aggregateClusterStats();
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `aggregate_spend_event` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `aggregate_spend_event` ON SCHEDULE EVERY 1 HOUR STARTS '2018-03-09 17:25:55' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call aggregateSpend('service');
  call aggregateSpend('vm');
  call aggregateSpend('app');
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `aggregate_stats_event` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `aggregate_stats_event` ON SCHEDULE EVERY 10 MINUTE STARTS '2018-03-09 17:32:36' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
  call aggregate('vpod');

  CALL trigger_rotate_partition();
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `purge_audit_log_expired_days` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `purge_audit_log_expired_days` ON SCHEDULE EVERY 1 DAY STARTS '2018-03-09 17:26:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  DELETE FROM audit_log_entries where snapshot_time<(select date_sub(current_timestamp, interval retention_period day) from audit_log_retention_policies where policy_name='retention_days');
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `purge_expired_days_cluster` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `purge_expired_days_cluster` ON SCHEDULE EVERY 1 DAY STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call purge_expired_days_cluster();
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `purge_expired_days_spend` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `purge_expired_days_spend` ON SCHEDULE EVERY 1 DAY STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call purge_expired_days_spend('service');
  call purge_expired_days_spend('vm');
  call purge_expired_days_spend('app');
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `purge_expired_hours_spend` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `purge_expired_hours_spend` ON SCHEDULE EVERY 1 HOUR STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call purge_expired_hours_spend('service');
  call purge_expired_hours_spend('vm');
  call purge_expired_hours_spend('app');
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `purge_expired_months_cluster` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `purge_expired_months_cluster` ON SCHEDULE EVERY 1 MONTH STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call purge_expired_months_cluster();
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
/*!50106 DROP EVENT IF EXISTS `purge_expired_months_spend` */;;
DELIMITER ;;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;;
/*!50003 SET character_set_client  = utf8 */ ;;
/*!50003 SET character_set_results = utf8 */ ;;
/*!50003 SET collation_connection  = utf8_general_ci */ ;;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;;
/*!50003 SET @saved_time_zone      = @@time_zone */ ;;
/*!50003 SET time_zone             = 'SYSTEM' */ ;;
/*!50106 CREATE*/ /*!50117 DEFINER=CURRENT_USER*/ /*!50106 EVENT `purge_expired_months_spend` ON SCHEDULE EVERY 1 MONTH STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
  call purge_expired_months_spend('service');
  call purge_expired_months_spend('vm');
  call purge_expired_months_spend('app');
END */ ;;
/*!50003 SET time_zone             = @saved_time_zone */ ;;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;;
/*!50003 SET character_set_client  = @saved_cs_client */ ;;
/*!50003 SET character_set_results = @saved_cs_results */ ;;
/*!50003 SET collation_connection  = @saved_col_connection */ ;;
DELIMITER ;
/*!50106 SET TIME_ZONE= @save_time_zone */ ;

--
-- Dumping routines for database 'vmtdb'
--
/*!50003 DROP FUNCTION IF EXISTS `CHECKAGGR` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `CHECKAGGR`() RETURNS int(11)
  BEGIN
    DECLARE AGGREGATING INTEGER DEFAULT 0;

    SELECT COUNT(*) INTO AGGREGATING FROM INFORMATION_SCHEMA.PROCESSLIST WHERE
      COMMAND != 'Sleep' AND INFO like '%aggreg%' and info not like '%INFORMATION_SCHEMA%' and info not like '%PARTITION%';
    RETURN AGGREGATING;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `cluster_nm1_factor` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `cluster_nm1_factor`(arg_group_name varchar(255)) RETURNS float
DETERMINISTIC
  BEGIN
    set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;

    IF @n_hosts > 0 THEN
      set @factor := (@n_hosts-1.0) / @n_hosts ;
    ELSE
      set @factor := 0 ;
    END IF ;

    return @factor ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `cluster_nm2_factor` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `cluster_nm2_factor`(arg_group_name varchar(255)) RETURNS float
DETERMINISTIC
  BEGIN
    set @n_hosts := (select count(*) from pm_group_members where internal_name = arg_group_name COLLATE utf8_unicode_ci) ;

    IF @n_hosts > 1 THEN
      set @factor := (@n_hosts-2.0) / @n_hosts ;
    ELSE
      set @factor := 0 ;
    END IF ;

    return @factor ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `datetime_from_ms` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `datetime_from_ms`(ms_time bigint) RETURNS datetime
DETERMINISTIC
  BEGIN
    return from_unixtime(ms_time/1000) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `date_from_ms` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `date_from_ms`(ms_time bigint) RETURNS date
DETERMINISTIC
  BEGIN
    return date(from_unixtime(ms_time/1000)) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `days_ago` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `days_ago`(ndays int) RETURNS date
DETERMINISTIC
  BEGIN
    return date_sub(date(now()), interval ndays day) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `days_from_now` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `days_from_now`(ndays int) RETURNS date
DETERMINISTIC
  BEGIN
    return date_add(date(now()), interval ndays day) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `end_of_day` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `end_of_day`(ref_date date) RETURNS date
DETERMINISTIC
  BEGIN
    return date(date_add(date_sub(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8)), interval 1 second), interval 1 day)) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `end_of_hour` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `end_of_hour`(ref_date timestamp) RETURNS timestamp
DETERMINISTIC
  BEGIN
    return timestamp(date_add(date_sub(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8)), interval 1 second), interval 1 hour)) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `end_of_month` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `end_of_month`(ref_date date) RETURNS date
DETERMINISTIC
  BEGIN
    return date_sub(date_add(start_of_month(ref_date), interval 1 month), interval 1 day) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `end_of_month_ms` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `end_of_month_ms`(ref_date date) RETURNS bigint(20)
DETERMINISTIC
  BEGIN
    return ms_from_date(date_add(start_of_month(ref_date), interval 1 month))-1 ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `ftn_pm_count_for_month` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `ftn_pm_count_for_month`(month_day_1 date) RETURNS int(11)
DETERMINISTIC
  BEGIN

    set @ms_day_1 := start_of_month(month_day_1) ;
    set @ms_day_n := end_of_month(month_day_1) ;

    set @count := (select count(uuid) as n_pms
                   from (select distinct uuid from pm_stats_by_day
                   where
                     property_type = 'priceIndex'
                     and snapshot_time between @ms_day_1 and @ms_day_n
                        ) as uuids
    ) ;

    return @count ;

  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `ftn_vm_count_for_month` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `ftn_vm_count_for_month`(month_day_1 date) RETURNS int(11)
DETERMINISTIC
  BEGIN

    set @ms_day_1 := start_of_month(month_day_1) ;
    set @ms_day_n := end_of_month(month_day_1) ;

    set @count := (select count(uuid) as n_vms
                   from (select distinct uuid from vm_stats_by_day
                   where
                     property_type = 'priceIndex'
                     and snapshot_time between @ms_day_1 and @ms_day_n
                        ) as uuids
    ) ;

    return @count ;

  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `ms_from_date` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `ms_from_date`(the_date date) RETURNS bigint(20)
DETERMINISTIC
  BEGIN
    return unix_timestamp(the_date)*1000 ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `ms_from_datetime` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `ms_from_datetime`(the_datetime datetime) RETURNS bigint(20)
DETERMINISTIC
  BEGIN
    return unix_timestamp(the_datetime)*1000 ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `start_of_day` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/* changed for XL */
CREATE DEFINER=CURRENT_USER FUNCTION `start_of_day`(ref_date datetime) RETURNS datetime
DETERMINISTIC
  BEGIN
    return date(date_format(ref_date, convert('%Y-%m-%d 00:00:00' using utf8))) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `start_of_hour` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `start_of_hour`(ref_date timestamp) RETURNS timestamp
DETERMINISTIC
  BEGIN
    return timestamp(date_format(ref_date, convert('%Y-%m-%d %H:00:00' using utf8))) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `start_of_month` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/* changes for xl */
CREATE DEFINER=CURRENT_USER FUNCTION `start_of_month`(ref_date datetime) RETURNS datetime
DETERMINISTIC
  BEGIN
    return date_sub(ref_date, interval day(ref_date)-1 day) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `start_of_month_ms` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER FUNCTION `start_of_month_ms`(ref_date date) RETURNS bigint(20)
DETERMINISTIC
  BEGIN
    return ms_from_date(start_of_month(ref_date)) ;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `aggregate` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `aggregate`(IN statspref CHAR(10))
  aggregate_proc:BEGIN
    DECLARE running_aggregations INT;
    DECLARE number_of_unaggregated_rows INT;
    DECLARE number_of_unaggregated_rows_hour INT;
    DECLARE number_of_unaggregated_rows_day INT;

    set @aggregation_id = md5(now());

    set sql_mode='';

    set running_aggregations = CHECKAGGR();
    if running_aggregations > 0 then
      select 'Aggregations already running... exiting rollup function.' as '';
      leave aggregate_proc;
    end if;

    insert into aggregation_status values ('Running', null);

    set @start_of_aggregation=now();

    /* HOURLY AGGREAGATION BEGIN */
    select concat(now(),' INFO:  Starting hourly aggregation ',statspref) as '';
    set @start_of_aggregation_hourly=now();

    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    set @sql=concat('update ',statspref,'_stats_latest a set a.aggregated=2 where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows = ROW_COUNT();
    DEALLOCATE PREPARE stmt;

    /* select number_of_unaggregated_rows; */


    if number_of_unaggregated_rows = 0 then
      select 'Nothing to aggregate...' as '';
      delete from aggregation_status;
      leave aggregate_proc;
    end if;


    /* create view returning only unaggregated rows */
    set @sql=concat('create or replace view ',statspref,'_hourly_ins_vw as
      select date_format(a.snapshot_time,"%Y-%m-%d %H:00:00") as snapshot_time,
      a.uuid,
      a.producer_uuid,
      a.property_type,
      a.property_subtype,
      a.relation,
      a.commodity_key,
      a.hour_key,
      a.day_key,
      a.month_key,
      max(a.capacity) as capacity,
      min(a.min_value) as min_value,
      max(a.max_value) as max_value,
      avg(a.avg_value) as avg_value,
      count(*) as samples,
      count(*) as new_samples
      from ',statspref,'_stats_latest a where a.aggregated=2 group by hour_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    /* Aggregate hourly data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */

    set @sql=concat('insert into ',statspref,'_stats_by_hour
         (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,
         max_value,min_value,avg_value,samples,aggregated,new_samples,hour_key,day_key,month_key)
           select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,
           max_value,min_value,avg_value,samples,0,new_samples,hour_key,day_key,month_key from ',statspref,'_hourly_ins_vw b
      on duplicate key update ',
                    statspref,'_stats_by_hour.snapshot_time=b.snapshot_time,',
                    statspref,'_stats_by_hour.uuid=b.uuid,',
                    statspref,'_stats_by_hour.producer_uuid=b.producer_uuid,',
                    statspref,'_stats_by_hour.property_type=b.property_type,',
                    statspref,'_stats_by_hour.property_subtype=b.property_subtype,',
                    statspref,'_stats_by_hour.relation=b.relation,',
                    statspref,'_stats_by_hour.commodity_key=b.commodity_key,',
                    statspref,'_stats_by_hour.min_value=if(b.min_value<',statspref,'_stats_by_hour.min_value, b.min_value, ',statspref,'_stats_by_hour.min_value),',
                    statspref,'_stats_by_hour.max_value=if(b.max_value>',statspref,'_stats_by_hour.max_value,b.max_value,',statspref,'_stats_by_hour.max_value),',
                    statspref,'_stats_by_hour.avg_value=((',statspref,'_stats_by_hour.avg_value*',statspref,'_stats_by_hour.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_hour.samples+b.new_samples),',
                    statspref,'_stats_by_hour.samples=',statspref,'_stats_by_hour.samples+b.new_samples,',
                    statspref,'_stats_by_hour.new_samples=b.new_samples,',
                    statspref,'_stats_by_hour.hour_key=b.hour_key,',
                    statspref,'_stats_by_hour.day_key=b.day_key,',
                    statspref,'_stats_by_hour.month_key=b.month_key,',
                    statspref,'_stats_by_hour.aggregated=0');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('update ',statspref,'_stats_latest set aggregated=1 where aggregated=2');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation_hourly=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Hourly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows,', Start time: ',@start_of_aggregation_hourly,', End time: ',@end_of_aggregation_hourly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION HOURLY',statspref,number_of_unaggregated_rows,@start_of_aggregation_hourly,@end_of_aggregation_hourly,time_to_sec(timediff(@end_of_aggregation_hourly,@start_of_aggregation_hourly)));
    /* END  HOURLY */


    /* DAILY AGGREGATION BEGIN */

    set @start_of_aggregation_daily=now();

    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    select concat(now(),' INFO:  Starting daily aggregation ',statspref) as '';

    set @sql=concat('update ',statspref,'_stats_by_hour a
set a.aggregated=2
where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows_hour = ROW_COUNT();

    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @sql=concat('create or replace view ',statspref,'_daily_ins_vw as
select date_format(a.snapshot_time,"%Y-%m-%d 00:00:00") as snapshot_time,
a.uuid,
a.producer_uuid,
a.property_type,
a.property_subtype,
a.relation,
a.commodity_key,
a.day_key,
a.month_key,
max(a.capacity) as capacity,
min(a.min_value) as min_value,
max(a.max_value) as max_value,
avg(a.avg_value) as avg_value,
sum(samples) as samples,
sum(new_samples) as new_samples
from ',statspref,'_stats_by_hour a where a.aggregated=2 group by day_key');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    /* Aggregate daily data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */

    set @sql=concat('insert into ',statspref,'_stats_by_day
 (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,
 max_value,min_value,avg_value,samples,aggregated,new_samples,day_key,month_key)
   select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,
   max_value,min_value,avg_value,samples,0,new_samples,day_key,month_key from ',statspref,'_daily_ins_vw b
on duplicate key update
',statspref,'_stats_by_day.snapshot_time=b.snapshot_time,
',statspref,'_stats_by_day.uuid=b.uuid,
',statspref,'_stats_by_day.producer_uuid=b.producer_uuid,
',statspref,'_stats_by_day.property_type=b.property_type,
',statspref,'_stats_by_day.property_subtype=b.property_subtype,
',statspref,'_stats_by_day.relation=b.relation,
',statspref,'_stats_by_day.commodity_key=b.commodity_key,
',statspref,'_stats_by_day.min_value=if(b.min_value<',statspref,'_stats_by_day.min_value, b.min_value, ',statspref,'_stats_by_day.min_value),
',statspref,'_stats_by_day.max_value=if(b.max_value>',statspref,'_stats_by_day.max_value,b.max_value,',statspref,'_stats_by_day.max_value),
',statspref,'_stats_by_day.avg_value=((',statspref,'_stats_by_day.avg_value*',statspref,'_stats_by_day.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_day.samples+b.new_samples),
',statspref,'_stats_by_day.samples=',statspref,'_stats_by_day.samples+b.new_samples,
',statspref,'_stats_by_day.new_samples=b.new_samples,
',statspref,'_stats_by_day.day_key=b.day_key,
',statspref,'_stats_by_day.month_key=b.month_key,
',statspref,'_stats_by_day.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @sql=concat('update ',statspref,'_stats_by_hour set aggregated=1 where aggregated=2');


    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /* select @sql; */
    set @end_of_aggregation_daily=now();

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Daily: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_hour,', Start time: ',@start_of_aggregation_daily,', End time: ',@end_of_aggregation_daily,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION DAILY',statspref,number_of_unaggregated_rows_hour,@start_of_aggregation_daily,@end_of_aggregation_daily,time_to_sec(timediff(@end_of_aggregation_daily,@start_of_aggregation_daily)));


    /* END DAILY AGGREGATION */


    /* MONTHLY AGGREGATION BEGIN */
    set @start_of_aggregation_monthly=now();
    /* Temporarily mark unnagregated frows for processing */
    /* aggregated=0 - not aggregated */
    /* aggregated=2 - processing aggregation */
    /* aggregated=1 - aggregated */
    select concat(now(),' INFO:  Starting monthly aggregation ',statspref) as '';

    set @sql=concat('update ',statspref,'_stats_by_day a
set a.aggregated=2
where a.aggregated=0');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    set number_of_unaggregated_rows_day = ROW_COUNT();

    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('create or replace view ',statspref,'_monthly_ins_vw as
select date_format(last_day(a.snapshot_time),"%Y-%m-%d 00:00:00") as snapshot_time,
a.uuid,
a.producer_uuid,
a.property_type,
a.property_subtype,
a.relation,
a.commodity_key,
a.month_key,
max(a.capacity) as capacity,
min(a.min_value) as min_value,
max(a.max_value) as max_value,
avg(a.avg_value) as avg_value,
sum(samples) as samples,
sum(new_samples) as new_samples
from ',statspref,'_stats_by_day a where a.aggregated=2 group by month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    /* Aggregate monthly data using MySQL on duplicate key update insert statement */
    /* Insert is performed if target row does not exist, otherwise an update is performed */


    set @sql=concat('insert into ',statspref,'_stats_by_month
 (snapshot_time,uuid, producer_uuid,property_type,property_subtype,relation,commodity_key,capacity,
 max_value,min_value,avg_value,samples,new_samples,month_key)
   select snapshot_time, uuid, producer_uuid,property_type,property_subtype,relation,commodity_key, capacity,
   max_value,min_value,avg_value,samples,new_samples,month_key from ',statspref,'_monthly_ins_vw b
on duplicate key update
',statspref,'_stats_by_month.snapshot_time=b.snapshot_time,
',statspref,'_stats_by_month.uuid=b.uuid,
',statspref,'_stats_by_month.producer_uuid=b.producer_uuid,
',statspref,'_stats_by_month.property_type=b.property_type,
',statspref,'_stats_by_month.property_subtype=b.property_subtype,
',statspref,'_stats_by_month.relation=b.relation,
',statspref,'_stats_by_month.commodity_key=b.commodity_key,
',statspref,'_stats_by_month.min_value=if(b.min_value<',statspref,'_stats_by_month.min_value, b.min_value, ',statspref,'_stats_by_month.min_value),
',statspref,'_stats_by_month.max_value=if(b.max_value>',statspref,'_stats_by_month.max_value,b.max_value,',statspref,'_stats_by_month.max_value),
',statspref,'_stats_by_month.avg_value=((',statspref,'_stats_by_month.avg_value*',statspref,'_stats_by_month.samples)+(b.avg_value*b.new_samples))/(',statspref,'_stats_by_month.samples+b.new_samples),
',statspref,'_stats_by_month.samples=',statspref,'_stats_by_month.samples+b.new_samples,
',statspref,'_stats_by_month.new_samples=b.new_samples,
',statspref,'_stats_by_month.month_key=b.month_key');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */


    set @sql=concat('update ',statspref,'_stats_by_day set aggregated=1 where aggregated=2');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */
    set @end_of_aggregation_monthly=now();

    /* END MONTHLY AGGREGATION */


    set @sql=concat('delete from aggregation_status');

    PREPARE stmt from @sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
    /* select @sql; */

    set @end_of_aggregation=now();
    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Monthly: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation_monthly,', End time: ',@end_of_aggregation_monthly,', Total Time: ', time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION MONTHLY',statspref,number_of_unaggregated_rows_day,@start_of_aggregation_monthly,@end_of_aggregation_monthly,time_to_sec(timediff(@end_of_aggregation_monthly,@start_of_aggregation_monthly)));

    select concat(now(),'   INFO: PERFORMANCE: Aggregation ID: ',@aggregation_id, ', Aggregation Total: ,',statspref,', Rows Aggregated: ',number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,', Start time: ',@start_of_aggregation,', End time: ',@end_of_aggregation,', Total Time: ', time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@aggregation_id, 'AGGREGATION TOTAL',statspref,number_of_unaggregated_rows+number_of_unaggregated_rows_hour+number_of_unaggregated_rows_day,@start_of_aggregation,@end_of_aggregation,time_to_sec(timediff(@end_of_aggregation,@start_of_aggregation)));

  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `aggregateClusterStats` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `aggregateClusterStats`()
  BEGIN

    /* MONTHLY CLUSTER STATS AGGREGATE */
    SET @monthly_insert_sql=concat('update cluster_stats_by_day a left join cluster_stats_by_month b
  on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
  and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  set a.aggregated=2
  where b.recorded_on is null
  and b.internal_name is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=0');

    PREPARE stmt from @monthly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists cluster_stats_monthly_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_insert_view=concat('create view cluster_stats_monthly_ins_vw as
  select date_format(last_day(a.recorded_on),"%Y-%m-01") as recorded_on,
  a.internal_name,
  a.property_type,
  a.property_subtype,
  avg(a.value) as value
  from cluster_stats_by_day a left join cluster_stats_by_month b
  on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
  and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.recorded_on is null
  and b.internal_name is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=2 group by 1,2,3,4');


    PREPARE stmt from @monthly_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_inserts_sql=concat('insert into cluster_stats_by_month (recorded_on, internal_name, property_type,property_subtype,value,samples)
     select recorded_on, internal_name, property_type,property_subtype,value,1 from cluster_stats_monthly_ins_vw');

    PREPARE stmt from @perform_monthly_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update cluster_stats_by_day set aggregated=1 where aggregated=2');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



    /* UPDATE EXISTING STATS */

    set @monthly_update_sql=concat('update cluster_stats_by_day a left join cluster_stats_by_month b
on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
and a.internal_name<=>b.internal_name
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=3
where a.aggregated=0');

    PREPARE stmt from @monthly_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists cluster_stats_monthly_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_update_view=concat('create view cluster_stats_monthly_upd_vw as
  select date_format(last_day(a.recorded_on),"%Y-%m-01") as recorded_on,
  a.internal_name,
  a.property_type,
  a.property_subtype,
  avg(a.value) as value,
  count(a.value) as samples
  from cluster_stats_by_day a left join cluster_stats_by_month b
  on date_format(last_day(a.recorded_on),"%Y-%m-01")<=>b.recorded_on
  and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.recorded_on is not null
  and b.internal_name is not null
  and b.property_type is not null
  and b.property_subtype is not null
  and a.aggregated=3 group by 1,2,3,4');

    PREPARE stmt from @monthly_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_updates_sql=concat('update cluster_stats_by_month a, cluster_stats_monthly_upd_vw b
  set a.recorded_on=b.recorded_on,
  a.internal_name=b.internal_name,
  a.property_type=b.property_type,
  a.property_subtype=b.property_subtype,
  a.value=((a.value*a.samples)+(b.value*b.samples))/(a.samples+b.samples),
  a.samples=a.samples+b.samples
  where a.recorded_on<=>b.recorded_on and a.internal_name<=>b.internal_name
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype');

    PREPARE stmt from @perform_monthly_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update cluster_stats_by_day set aggregated=1 where aggregated=3');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `aggregateSpend` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `aggregateSpend`(IN spendpref CHAR(10))
  BEGIN

    /* DAILY AGGREGATE */
    set @daily_insert_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=2
where b.snapshot_time is null
and b.uuid is null
and b.provider_uuid is null
and b.property_type is null
and b.property_subtype is null
and b.rate is null
and a.aggregated=0');

    PREPARE stmt from @daily_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists ',spendpref,'_daily_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @daily_insert_view=concat('create view ',spendpref,'_daily_ins_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  count(*) as samples,
  count(*) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
  on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is null
  and b.uuid is null
  and b.provider_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=2 group by 1,2,3,4,5');

    PREPARE stmt from @daily_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_daily_inserts_sql=concat('insert into ',spendpref,'_spend_by_day (snapshot_time, uuid, provider_uuid,property_type, property_subtype, rate,samples,new_samples,aggregated)
     select snapshot_time, uuid, provider_uuid, property_type, property_subtype, rate,samples,new_samples,0 from ',spendpref,'_daily_ins_vw');

    PREPARE stmt from @perform_daily_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=4 where aggregated=2');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    set @daily_update_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=3,
a.new_samples=1
where a.aggregated=0');

    PREPARE stmt from @daily_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists ',spendpref,'_daily_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @daily_update_view=concat('create view ',spendpref,'_daily_upd_vw as
  select date_format(a.snapshot_time,"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  count(*) as samples,
  count(*) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_day b
  on date_format(a.snapshot_time,"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is not null
  and b.uuid is not null
  and b.provider_uuid is not null
  and b.property_type is not null
  and b.property_subtype is not null
  and a.aggregated=3 group by 1,2,3,4,5');

    PREPARE stmt from @daily_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_daily_updates_sql=concat('update ',spendpref,'_spend_by_day a, ',spendpref,'_daily_upd_vw b
  set a.snapshot_time=b.snapshot_time,
  a.uuid=b.uuid,
  a.provider_uuid=b.provider_uuid,
  a.property_type=b.property_type,
  a.property_subtype=b.property_subtype,
  a.rate=((a.rate*a.samples)+(b.rate*b.samples))/(a.samples+b.samples),
  a.samples=a.samples+b.samples,
  a.new_samples=a.new_samples+b.new_samples,
  a.aggregated=0
  where a.snapshot_time<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype');


    PREPARE stmt from @perform_daily_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=4 where aggregated=3');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /*
	 * These can be useful for debugging intermediate state of the rollup
	 */
    /*
    drop view if exists ',spendpref,'_ins_vw;
    drop view if exists ',spendpref,'_upd_vw;
    */


    /* MONTHLY AGGREGATE */
    set @monthly_insert_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=5
where b.snapshot_time is null
and b.uuid is null
and b.provider_uuid is null
and b.property_type is null
and b.property_subtype is null
and a.aggregated=4');

    PREPARE stmt from @monthly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists ',spendpref,'_monthly_ins_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_insert_view=concat('create view ',spendpref,'_monthly_ins_vw as
  select date_format(last_day(a.snapshot_time),"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  sum(a.samples) as samples,
  sum(a.new_samples) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
  on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is null
  and b.uuid is null
  and b.provider_uuid is null
  and b.property_type is null
  and b.property_subtype is null
  and a.aggregated=5 group by 1,2,3,4,5');


    PREPARE stmt from @monthly_insert_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_inserts_sql=concat('insert into ',spendpref,'_spend_by_month (snapshot_time, uuid, provider_uuid,property_type,property_subtype,rate,samples)
     select snapshot_time, uuid, provider_uuid,property_type,property_subtype,rate,samples from ',spendpref,'_monthly_ins_vw');

    PREPARE stmt from @perform_monthly_inserts_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=1 where aggregated=5');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;



    set @monthly_update_sql=concat('update ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
and a.uuid<=>b.uuid
and a.provider_uuid<=>b.provider_uuid
and a.property_type<=>b.property_type
and a.property_subtype<=>b.property_subtype
set a.aggregated=6
where a.aggregated=4');

    PREPARE stmt from @monthly_update_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    SET @view_delete=concat('drop view if exists ',spendpref,'_monthly_upd_vw');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @monthly_update_view=concat('create view ',spendpref,'_monthly_upd_vw as
  select date_format(last_day(a.snapshot_time),"%Y-%m-%d") as snapshot_time,
  a.uuid,
  a.provider_uuid,
  a.property_type,
  a.property_subtype,
  avg(a.rate) as rate,
  sum(a.samples) as samples,
  sum(a.new_samples) as new_samples
  from ',spendpref,'_spend_by_hour a left join ',spendpref,'_spend_by_month b
  on date_format(last_day(a.snapshot_time),"%Y-%m-%d")<=>b.snapshot_time
  and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype
  where b.snapshot_time is not null
  and b.uuid is not null
  and b.provider_uuid is not null
  and b.property_type is not null
  and b.property_subtype is not null
  and a.aggregated=6 group by 1,2,3,4,5');

    PREPARE stmt from @monthly_update_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_monthly_updates_sql=concat('update ',spendpref,'_spend_by_month a, ',spendpref,'_monthly_upd_vw b
  set a.snapshot_time=b.snapshot_time,
  a.uuid=b.uuid,
  a.provider_uuid=b.provider_uuid,
  a.property_type=b.property_type,
  a.property_subtype=b.property_subtype,
  a.rate=((a.rate*a.samples)+(b.rate*b.samples))/(a.samples+b.samples),
  a.samples=a.samples+b.samples
  where a.snapshot_time<=>b.snapshot_time and a.uuid<=>b.uuid
  and a.provider_uuid<=>b.provider_uuid
  and a.property_type<=>b.property_type
  and a.property_subtype<=>b.property_subtype');

    PREPARE stmt from @perform_monthly_updates_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @update_stage_sql=concat('update ',spendpref,'_spend_by_hour set aggregated=1 where aggregated=6');

    PREPARE stmt from @update_stage_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    /*
    drop view if exists ',spendpref,'_ins_vw;
    drop view if exists ',spendpref,'_upd_vw;
    */


  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `clusterAggPreviousDay` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `clusterAggPreviousDay`(IN cluster_internal_name varchar(250))
  begin

    insert into cluster_stats_by_day
      select
        date_sub(date(concat(year(now()),date_format(now(),'%m'),date_format(now(),'%d'))), interval 1 day) as recorded_on,
        convert(cluster_internal_name using utf8) as internal_name,
        if(property_type='numCPUs', 'numCores',property_type) as property_type,
        property_subtype,
        if(property_subtype='utilization', val, sum(val)) as value
      from
        (

          select 'Host' as property_type, 'numHosts' as property_subtype, count(uuid) as val
          from
            (select distinct member_uuid as uuid from cluster_members_yesterday
            where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
            ) as yesterday_pms

          union

          select 'VM' as property_type, 'numVMs' as property_subtype, count(uuid) as val
          from
            (select distinct member_uuid as uuid from cluster_members_yesterday
            where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
            ) as yesterday_vms

          union

          select 'Mem' as property_type, 'capacity' as property_subtype, sum(mem) / 1024 / 1024 as val
          from
            ( select
                uuid,
                max(capacity) as mem
              from
                pm_stats_by_day
              where
                snapshot_time = date(date_sub(now(), interval 1 day))
                and property_type = 'Mem'
                and property_subtype = 'utilization'
              group by
                uuid
            ) as pm_stats_by_day
            natural join
            (select distinct member_uuid as uuid from cluster_members_yesterday
            where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
            ) as yesterday_pms

          union


          select property_type, property_subtype, avg(mem) as val
          from
            ( select property_type, property_subtype, uuid, avg(avg_value) as mem
              from
                pm_stats_by_day
              where snapshot_time = date(date_sub(now(), interval 1 day))
                    and property_type in ('Mem', 'CPU')
                    and property_subtype = 'utilization'
              group by uuid, property_type
            ) as pm_stats_by_day
            natural join
            (select distinct member_uuid as uuid
             from cluster_members_yesterday where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
            ) as yesterday_pms
          group by property_type

          union

          select 'CPU' as property_type, property_type as property_subtype, sum(cpu_prop) as val
          from
            ( select property_type, property_subtype, uuid, max(avg_value) as cpu_prop
              from
                pm_stats_by_day
              where
                snapshot_time = date(date_sub(now(), interval 1 day))
                and property_type in ('numSockets', 'numCPUs')
              group by
                uuid, property_type
            ) as pm_stats_by_day
            natural join
            (select distinct member_uuid as uuid from cluster_members_yesterday
            where convert(internal_name using utf8) = convert(cluster_internal_name using utf8)
            ) as yesterday_pms
          group by property_type

          union

          select 'Storage' as property_type, 'capacity' as property_subtype, sum(capacity) as val
          from
            (select ds_stats_by_month.ds_uuid as ds_uuid, capacity
             from
               (select uuid as ds_uuid,
                       max(capacity) as capacity
                from
                  ds_stats_by_day
                where
                  snapshot_time = date(date_sub(now(), interval 1 day))
                  and property_type = 'StorageAmount'
                  and property_subtype = 'utilization'
                group by
                  uuid
               ) as ds_stats_by_month
               natural join
               (select distinct producer_uuid as ds_uuid from
                 (select uuid as vm_uuid, producer_uuid
                  from vm_stats_by_day
                  where
                    property_subtype = 'used' and property_type = 'StorageAmount'
                    and snapshot_time = date(date_sub(now(), interval 1 day))
                  group by
                    uuid
                 ) as vm_to_ds
                 natural join
                 (select distinct member_uuid as vm_uuid from cluster_members_yesterday
                 where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
                 ) as yesterday_vms
               ) as included_ds_uuids
            ) as values_by_month

          union

          select 'Storage' as property_type, 'allocated' as property_subtype, sum(used_capacity) as val
          from
            (select uuid, avg_value as used_capacity
             from
               vm_stats_by_day
             where
               property_type = 'StorageAmount'
               and property_subtype = 'used'
               and snapshot_time = date(date_sub(now(), interval 1 day))
            ) as vm_stats_by_month
            natural join
            (select distinct member_uuid as uuid from cluster_members_yesterday
            where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
            ) as members

          union

          select 'Storage' as property_type, 'available' as property_subtype, sum((1.0-max_value)*capacity) as val
          from
            (select
               ds_uuid as uuid, capacity, max_value
             from
               (select uuid as ds_uuid, capacity, max_value
                from ds_stats_by_day
                where
                  snapshot_time = date(date_sub(now(), interval 1 day))
                  and property_type = 'StorageAmount'
                  and property_subtype = 'utilization'
               ) as ds_stats_by_day
               natural join
               (select distinct producer_uuid as ds_uuid from
                 (select uuid as vm_uuid, producer_uuid
                  from vm_stats_by_day
                  where
                    property_subtype = 'used' and property_type = 'StorageAmount'
                    and snapshot_time = date(date_sub(now(), interval 1 day))
                  group by uuid
                 ) as vm_to_ds
                 natural join
                 (select distinct member_uuid as vm_uuid from cluster_members_yesterday
                 where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
                 ) as yesterday_vms
               ) as included_ds_uuids
             group by uuid
            ) as values_by_month

          union

          select 'VMem' as property_type, 'capacity' as property_subtype, sum(vmem) / 1024 / 1024 as val
          from
            ( select uuid, max(capacity) as vmem
              from vm_stats_by_day
              where
                snapshot_time = date(date_sub(now(), interval 1 day))
                and property_type = 'VMem'
                and property_subtype = 'utilization'
              group by uuid
            ) as vm_stats_by_day
            natural join
            (select distinct member_uuid as uuid from cluster_members_yesterday
            where convert(internal_name using utf8) = replace(concat('VMs_',convert(cluster_internal_name using utf8)),'VMs_GROUP-PMs','GROUP-VMs')
            ) as yesterday_vms

        ) as side_by_side_data

      group by
        property_type, property_subtype
    ;

  end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `market_aggregate` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `market_aggregate`(IN statspref CHAR(10))
  BEGIN
    DECLARE v_stats_table varchar(32);
    DECLARE v_snapshot_time datetime;
    DECLARE v_topology_context_id bigint(20);
    DECLARE v_topology_id bigint(20);
    DECLARE v_entity_type varchar(80);
    DECLARE v_property_type varchar(36);
    DECLARE v_property_subtype varchar(36);
    DECLARE v_capacity decimal(15,3);
    DECLARE v_avg_value decimal(15,3);
    DECLARE v_min_value decimal(15,3);
    DECLARE v_max_value decimal(15,3);
    DECLARE v_relation integer;
    DECLARE v_aggregated boolean;
    DECLARE done int default false;
    DECLARE cur1 CURSOR for select * from mkt_stats_vw;

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=TRUE;
    -- dynamic query to prepare view of rows not aggregated
    set v_stats_table=concat(statspref,'_stats_latest');
    DROP VIEW IF EXISTS mkt_stats_vw;
    SET @query = CONCAT('CREATE VIEW mkt_stats_vw as select * from ',statspref,'_stats_latest where aggregated=false');

    PREPARE stmt from @query;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;


    -- open cursor
    open cur1;

    read_loop: LOOP
      fetch cur1 into v_snapshot_time,v_topology_context_id,v_topology_id,v_entity_type,v_property_type,v_property_subtype,v_capacity,v_avg_value,v_min_value,v_max_value,v_relation,v_aggregated;
      if done THEN
        LEAVE read_loop;
      end if;


      -- HOURLY MARKET AGGREGATE
      -- Set stats table to process ie.  market_stats_by_hour

      SET @stats_table = CONCAT(statspref,'_stats_by_hour');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_hour table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_topology_context_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;


      -- Build update sql statement
      set @update_hourly_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and topology_context_id<=>?
                                  and entity_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_hourly_sql=CONCAT('insert into ',@stats_table,
                                    ' (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples)
                                    values (?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the market_stats_hourly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_hourly_sql;
        SET @p1=v_avg_value;
        SET @p2=v_capacity;
        SET @p3=v_min_value;
        SET @p4=v_max_value;
        SET @p5=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p6=v_topology_context_id;
        SET @p8=v_entity_type;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p3,@p4,@p4,@p5,@p6,@p8,@p9,@p10,@p11;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_hourly_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d %H:00:00"),'%Y-%m-%d %H:00:00');
        SET @p2=v_topology_context_id;
        SET @p4=v_entity_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);

        EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;


      END IF;



      -- DAILY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_day

      SET @stats_table = CONCAT(statspref,'_stats_by_day');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_day table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
      SET @p2=v_topology_context_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;



      set @update_daily_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and topology_context_id<=>?
                                  and entity_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_daily_sql=CONCAT('insert into ',@stats_table,
                                   ' (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples)
                                   values (?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the market_stats_hourly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_daily_sql;
        SET @p1=v_avg_value;
        SET @p2=v_capacity;
        SET @p3=v_min_value;
        SET @p4=v_max_value;
        SET @p5=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p6=v_topology_context_id;
        SET @p8=v_entity_type;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p3,@p4,@p4,@p5,@p6,@p8,@p9,@p10,@p11;
        DEALLOCATE PREPARE stmt;

      ELSE
        PREPARE stmt from @insert_daily_sql;
        SET @p1=str_to_date(date_format(v_snapshot_time,"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_topology_context_id;
        SET @p4=v_entity_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);

        EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;


      END IF;




      -- MONTHLY AGGREGATE
      -- Set stats table to process ie.  vm_stats_by_month

      SET @stats_table = CONCAT(statspref,'_stats_by_month');

      SET @checkexists_sql = CONCAT('select @checkexists := 1 from ',@stats_table,' where snapshot_time<=>?
                               and topology_context_id<=>?
                               and entity_type<=>?
                               and property_type<=>?
                               and property_subtype<=>?
                               and relation<=>? limit 1 ');


      SET @checkexists=0;
      --  Build prepared statement to check if there is an existing row for the entity in for market_stats_by_month table, and set statement parameters.

      PREPARE stmt from @checkexists_sql;
      -- dataformat converts to string.  Converting back to datetime
      SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d %H:00:00');
      SET @p2=v_topology_context_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;



      set @update_monthly_sql=CONCAT('update ',@stats_table,'  set avg_value=if(? is null,avg_value,((avg_value*samples)+?)/(samples+1)),
                                                           samples=if(? is null, samples, samples+1),
                                                           capacity=?,
                                                           min_value=if(min_value < ?,min_value,?),
                                                           max_value=if(max_value > ?,max_value,?)
                                  where snapshot_time<=>?
                                  and topology_context_id<=>?
                                  and entity_type<=>?
                                  and property_type<=>?
                                  and property_subtype<=>?
                                  and relation<=>?');


      -- Build insert sql statement
      set @insert_monthly_sql=CONCAT('insert into ',@stats_table,
                                     ' (snapshot_time,topology_context_id,entity_type,property_type,property_subtype,relation,capacity,min_value,max_value,avg_value,samples)
                                     values (?,?,?,?,?,?,?,?,?,?,?)');



      -- Check if there is an existing entry in the market_stats_monthly_table.   If exists then update and recalculate min,max,avg, otherwise insert new row.
      IF @checkexists=1  THEN

        PREPARE stmt FROM @update_monthly_sql;
        SET @p1=v_avg_value;
        SET @p2=v_capacity;
        SET @p3=v_min_value;
        SET @p4=v_max_value;
        SET @p5=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p6=v_topology_context_id;
        SET @p8=v_entity_type;
        SET @p9=v_property_type;
        SET @p10=v_property_subtype;
        SET @p11=v_relation;

        EXECUTE stmt USING @p1,@p1,@p1,@p2,@p3,@p3,@p4,@p4,@p5,@p6,@p8,@p9,@p10,@p11;
        DEALLOCATE PREPARE stmt;



      ELSE

        PREPARE stmt from @insert_monthly_sql;
        SET @p1=str_to_date(date_format(last_day(v_snapshot_time),"%Y-%m-%d 00:00:00"),'%Y-%m-%d 00:00:00');
        SET @p2=v_topology_context_id;
        SET @p4=v_entity_type;
        SET @p5=v_property_type;
        SET @p6=v_property_subtype;
        SET @p7=v_relation;
        SET @p8=v_capacity;
        SET @p9=v_min_value;
        SET @p10=v_max_value;
        SET @p11=v_avg_value;
        SET @p12=if(v_avg_value is null,0,1);

        EXECUTE stmt USING @p1,@p2,@p4,@p5,@p6,@p7,@p8,@p9,@p10,@p11,@p12;
        DEALLOCATE PREPARE stmt;

      END IF;





      /* Mark _latest row as aggregated */
      set @latest=concat(statspref,'_stats_latest');
      set @latest_sql=CONCAT('update ',@latest,' set aggregated=true where snapshot_time<=>? and topology_context_id<=>? and topology_id<=>? and entity_type<=>? and property_type<=>? and property_subtype<=>?  and relation<=>? ');
      PREPARE stmt from @latest_sql;
      SET @p1=v_snapshot_time;
      SET @p2=v_topology_context_id;
      SET @p3=v_topology_id;
      SET @p4=v_entity_type;
      SET @p5=v_property_type;
      SET @p6=v_property_subtype;
      SET @p7=v_relation;

      EXECUTE stmt USING @p1,@p2,@p3,@p4,@p5,@p6,@p7;
      DEALLOCATE PREPARE stmt;

      -- loop until all rows aggregated
    END LOOP;
    close cur1;
    -- delete temporary view
    DROP VIEW mkt_stats_vw;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `populate_AllClusters_PreviousDayAggStats` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `populate_AllClusters_PreviousDayAggStats`()
  begin
    DECLARE done INT DEFAULT 0;
    DECLARE cur_clsuter_internal_name varchar(250);


    DECLARE clusters_iterator CURSOR FOR SELECT DISTINCT internal_name FROM cluster_members_yesterday WHERE group_type='PhysicalMachine';
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN clusters_iterator;

    REPEAT
      FETCH clusters_iterator INTO cur_clsuter_internal_name;
      IF NOT done THEN
        call clusterAggPreviousDay(cur_clsuter_internal_name);
      END IF;
    UNTIL done END REPEAT;

    CLOSE clusters_iterator;
  end ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_days` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_days`(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_day
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period day)
             from retention_policies where policy_name="retention_days") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_days_cluster` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_days_cluster`()
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM cluster_stats_by_day
             where recorded_on<(select date_sub(current_timestamp, interval retention_period day)
             from retention_policies where policy_name="retention_days") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_days_spend` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_days_spend`(IN spendpref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',spendpref,'_spend_by_day
             where snapshot_time<(select date_sub(current_timestamp, interval value day)
             from entity_attrs where name="numRetainedDays") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_hours` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_hours`(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_hour
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour)
             from retention_policies where policy_name="retention_hours") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_hours_spend` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_hours_spend`(IN spendpref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',spendpref,'_spend_by_hour
             where snapshot_time<(select date_sub(current_timestamp, interval value hour)
             from entity_attrs where name="numRetainedHours") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_latest` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_latest`(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_latest
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period hour)
             from retention_policies where policy_name="retention_latest_hours") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_months` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_months`(IN statspref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',statspref,'_stats_by_month
             where snapshot_time<(select date_sub(current_timestamp, interval retention_period month)
             from retention_policies where policy_name="retention_months") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_months_cluster` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_months_cluster`()
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM cluster_stats_by_month
             where recorded_on<(select date_sub(current_timestamp, interval retention_period month)
             from retention_policies where name="retention_months") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `purge_expired_months_spend` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `purge_expired_months_spend`(IN spendpref CHAR(10))
MODIFIES SQL DATA
  BEGIN
    SET @purge_sql=concat('DELETE FROM ',spendpref,'_spend_by_month
             where snapshot_time<(select date_sub(current_timestamp, interval value month)
             from entity_attrs where name="numRetainedMonths") limit 1000');

    PREPARE stmt from @purge_sql;
    REPEAT
      EXECUTE stmt;
    UNTIL ROW_COUNT() = 0 END REPEAT;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `rotate_partition` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `rotate_partition`(IN stats_table CHAR(30))
  BEGIN

    # sql statement to be executed
    DECLARE sql_statement varchar (1000);

    DECLARE done INT DEFAULT FALSE;

    # name of the partition that we are iterating through
    # partitions name follow this pattern: beforeYYYYMMDDHHmmSS
    DECLARE part_name CHAR(22);
    DECLARE num_seconds INT;
    DECLARE num_seconds_for_future  INT;
    DECLARE retention_type CHAR(20);

    # cursor for iterating over existing partitions
    # the select will return only the numeric part, removing the 'before' string
    # it will also remove the start and future partition from the result
    # the cursor will iterate over numbers/dates in increasing order
    DECLARE cur1 CURSOR FOR (select substring(PARTITION_NAME, 7) from information_schema.partitions where table_name=stats_table COLLATE utf8_unicode_ci and TABLE_SCHEMA=database() and substring(PARTITION_NAME, 7) <> '' order by PARTITION_NAME asc);

    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;

    # capture start time for partitioning performance measurement
    set @partitioning_id = md5(now());
    set @start_of_partitioning=now();

    # check which table we need to rotate, and set variables for it
    set retention_type = substring_index(stats_table, '_', -1);
    CASE retention_type
      WHEN 'latest' then
        select retention_period into num_seconds from retention_policies where policy_name='retention_latest_hours';
        set num_seconds = num_seconds *60*60;
        # set future to 2 hours
        set num_seconds_for_future = 2*60*60;

      WHEN 'hour' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_hours';
        set num_seconds = num_seconds *60*60;
        # set future to 8 hours
        set num_seconds_for_future = 8*60*60;

      WHEN 'day' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_days';
        set num_seconds = num_seconds *24*60*60;
        # set future to 3 days
        set num_seconds_for_future = 3*24*60*60;

      WHEN 'month' THEN
        select retention_period into num_seconds from retention_policies where policy_name='retention_months';
        set num_seconds = num_seconds * 31*24*60*60;
        # set future to 3 days
        set num_seconds_for_future = 3*24*60*60;
    END CASE;


    #calculate what should be the last partition from the past
    set @last_part := date_sub(current_timestamp, INTERVAL num_seconds SECOND);
    set @last_part_compact := YEAR(@last_part)*10000000000 + MONTH(@last_part)*100000000 + DAY(@last_part)*1000000 + hour(@last_part)*10000 + minute(@last_part)*100 + second(@last_part);

    # create future partitions for next X hours/days
    set @future_part := date_add(current_timestamp, INTERVAL num_seconds_for_future SECOND);
    set @future_part_compact := YEAR(@future_part)*10000000000 + MONTH(@future_part)*100000000 + DAY(@future_part)*1000000 + hour(@future_part)*10000 + minute(@future_part)*100 + second(@future_part);

    # var to store the maximum partition date existing right now
    set @max_part := 0;

    # iterate over the cursor and drop all the old partitions not needed anymore
    OPEN cur1;
    read_loop: LOOP
      FETCH cur1 INTO part_name;
      IF done THEN
        LEAVE read_loop;
      END IF;

      # if current partition is older than the last partition, drop it
      IF part_name < @last_part_compact THEN
        set @sql_statement = concat('alter table ', stats_table, ' DROP PARTITION before',part_name);
        PREPARE stmt from @sql_statement;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
      END IF;

      # set the current partition as the partition encountered with max date
      set @max_part := part_name;
    END LOOP;
    CLOSE cur1;

    # check if the maximum existing partition is even before the last partition that we need to have
    # in this case use the last partition as a starting point
    IF @max_part < @last_part_compact THEN
      set @max_part := @last_part_compact;
    END IF;

    # calculate the time period between partitions, given the number of total partitions
    # right now we are always trying to generate 40 partitions
    set @delta := (to_seconds(@future_part) - to_seconds(@last_part)) DIV 40;

    # reorganize the "future" partition by adding new partitions to it
    set @sql_statement = concat('alter table ', stats_table, ' REORGANIZE PARTITION future into (');

    # add the delta once
    set @add_part := date_add(@max_part, INTERVAL @delta SECOND);
    set @add_part_compact := YEAR(@add_part)*10000000000 + MONTH(@add_part)*100000000 + DAY(@add_part)*1000000 + hour(@add_part)*10000 + minute(@add_part)*100 + second(@add_part);

    # continue adding the delta until we reach the future date
    WHILE @add_part_compact <= @future_part_compact DO

      # append another partition
      set @sql_statement = concat(@sql_statement, 'partition before', @add_part_compact, ' VALUES LESS THAN (to_seconds(\'', @add_part, '\')), ');

      # increase the date by another delta
      set @add_part := date_add(@add_part, INTERVAL @delta SECOND);
      set @add_part_compact := YEAR(@add_part)*10000000000 + MONTH(@add_part)*100000000 + DAY(@add_part)*1000000 + hour(@add_part)*10000 + minute(@add_part)*100 + second(@add_part);

    END WHILE;

    # finish the alter partition statement
    set @sql_statement = concat(@sql_statement, ' partition future VALUES LESS THAN MAXVALUE);');
    # and print it out
    select @sql_statement;
    # execute it
    PREPARE stmt from @sql_statement;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    # capture end time of partitioning.  Log timings to standard out, and appl_performance table
    set @end_of_partitioning=now();
    select concat(now(),'   INFO: PERFORMANCE: Partitioning ID: ',@partitioning_id, ', Partitioning of: ,',stats_table,', Start time: ',@start_of_partitioning,', End time: ',@end_of_partitioning,', Total Time: ', time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)),' seconds') as '';
    insert into appl_performance (id, performance_type, entity_type, rows_aggregated, start_time, end_time, runtime_seconds) values (@partitioning_id, 'REPARTITION',stats_table,0,@start_of_partitioning,@end_of_partitioning,time_to_sec(timediff(@end_of_partitioning,@start_of_partitioning)));

  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `supplychain_stats_roll_up` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `supplychain_stats_roll_up`()
  BEGIN

    /*
   * delete the monthly record where the time is in the update month
   */
    SET @perform_monthly_delete_sql=concat('DELETE FROM supply_chain_stats_by_month',
                                           ' where DATE_FORMAT(supply_chain_stats_by_month.snapshot_time, "%Y-%m") <=>
	(Select DATE_FORMAT(MAX(supply_chain_stats_by_day.snapshot_time), "%Y-%m") as max_date',
                                           ' from supply_chain_stats_by_day)');

    PREPARE stmt from @perform_monthly_delete_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /*
   * filter the supply-chain stats by snapshot_time, ex. keep the max month.
   */
    SET @view_delete=concat('drop view if exists supply_chain_stats_by_day_time_vw');

    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_daily_filter_view=concat('create view supply_chain_stats_by_day_time_vw as',
                                          ' select s1.snapshot_time, s1.scope_type, s1.uuid, s1.property_type, s1.property_subtype, s1.value',
                                          ' from supply_chain_stats_by_day s1 where DATE_FORMAT(s1.snapshot_time, "%Y-%m") <=> ',
                                          '(Select DATE_FORMAT(MAX(s2.snapshot_time), "%Y-%m") as max_date  from supply_chain_stats_by_day s2)');

    PREPARE stmt from @perform_daily_filter_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /*
   * Group by the scope type, uuid, property subtype
   */
    SET @view_delete=concat('drop view if exists supply_chain_stats_by_day_time_groupby_vw;');
    PREPARE stmt from @view_delete;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    SET @perform_daily_filter_groupby_view=concat('create view supply_chain_stats_by_day_time_groupby_vw as',
                                                  ' select MAX(s1.snapshot_time), s1.scope_type, s1.uuid, s1.property_type, s1.property_subtype, CAST(AVG(s1.value) AS SIGNED)',
                                                  ' from supply_chain_stats_by_day_time_vw s1 group by s1.scope_type, s1.uuid, s1.property_subtype;');

    PREPARE stmt from @perform_daily_filter_groupby_view;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;

    /*
   * insert
   */
    set @monthly_insert_sql=concat('insert into supply_chain_stats_by_month',
                                   ' (snapshot_time, scope_type, uuid, property_type, property_subtype, value)',
                                   ' select supply_chain_stats_by_day_time_groupby_vw.`MAX(s1.snapshot_time)`,',
                                   ' supply_chain_stats_by_day_time_groupby_vw.scope_type, supply_chain_stats_by_day_time_groupby_vw.uuid,',
                                   ' supply_chain_stats_by_day_time_groupby_vw.property_type, supply_chain_stats_by_day_time_groupby_vw.property_subtype,',
                                   ' supply_chain_stats_by_day_time_groupby_vw.`CAST(AVG(s1.value) AS SIGNED)` from supply_chain_stats_by_day_time_groupby_vw');

    PREPARE stmt from @monthly_insert_sql;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `trigger_purge_expired` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `trigger_purge_expired`()
  BEGIN
    -- purge latest table records
    call purge_expired_latest('app');
    call purge_expired_latest('ch');
    call purge_expired_latest('cnt');
    call purge_expired_latest('dpod');
    call purge_expired_latest('ds');
    call purge_expired_latest('iom');
    call purge_expired_latest('pm');
    call purge_expired_latest('sc');
    call purge_expired_latest('sw');
    call purge_expired_latest('vdc');
    call purge_expired_latest('vm');
    call purge_expired_latest('vpod');

    -- purge _by_hour records
    call purge_expired_hours('app');
    call purge_expired_hours('ch');
    call purge_expired_hours('cnt');
    call purge_expired_hours('dpod');
    call purge_expired_hours('ds');
    call purge_expired_hours('iom');
    call purge_expired_hours('pm');
    call purge_expired_hours('sc');
    call purge_expired_hours('sw');
    call purge_expired_hours('vdc');
    call purge_expired_hours('vm');
    call purge_expired_hours('vpod');

    -- purge _by_days records
    call purge_expired_days('app');
    call purge_expired_days('ch');
    call purge_expired_days('cnt');
    call purge_expired_days('dpod');
    call purge_expired_days('ds');
    call purge_expired_days('iom');
    call purge_expired_days('pm');
    call purge_expired_days('sc');
    call purge_expired_days('sw');
    call purge_expired_days('vdc');
    call purge_expired_days('vm');
    call purge_expired_days('vpod');

    -- purge _by_months records
    call purge_expired_months('app');
    call purge_expired_months('ch');
    call purge_expired_months('cnt');
    call purge_expired_months('dpod');
    call purge_expired_months('ds');
    call purge_expired_months('iom');
    call purge_expired_months('pm');
    call purge_expired_months('sc');
    call purge_expired_months('sw');
    call purge_expired_months('vdc');
    call purge_expired_months('vm');
    call purge_expired_months('vpod');
  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `trigger_rotate_partition` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8 */ ;
/*!50003 SET character_set_results = utf8 */ ;
/*!50003 SET collation_connection  = utf8_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'IGNORE_SPACE,STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE DEFINER=CURRENT_USER PROCEDURE `trigger_rotate_partition`()
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

  END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `active_vms`
--

/*!50001 DROP TABLE IF EXISTS `active_vms`*/;
/*!50001 DROP VIEW IF EXISTS `active_vms`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `active_vms` AS select distinct `vm_stats_by_hour`.`uuid` AS `uuid` from `vm_stats_by_hour` where ((`vm_stats_by_hour`.`property_type` = 'VCPU') and (`vm_stats_by_hour`.`property_subtype` = 'utilization') and (`vm_stats_by_hour`.`max_value` >= 0.010)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `app_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `app_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,count(0) AS `samples`,count(0) AS `new_samples` from (`app_spend_by_hour` `a` left join `app_spend_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`provider_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `app_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `app_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,count(0) AS `samples`,count(0) AS `new_samples` from (`app_spend_by_hour` `a` left join `app_spend_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`provider_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_groups`
--

/*!50001 DROP TABLE IF EXISTS `app_groups`*/;
/*!50001 DROP VIEW IF EXISTS `app_groups`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_groups` AS select `entity_groups`.`entity_id` AS `entity_id`,`entity_groups`.`group_uuid` AS `group_uuid`,`entity_groups`.`internal_name` AS `internal_name`,`entity_groups`.`group_name` AS `group_name`,`entity_groups`.`group_type` AS `group_type` from `entity_groups` where (`entity_groups`.`group_type` = 'Application') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `app_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `app_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`app_stats_latest` `a` left join `app_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `app_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `app_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`app_stats_latest` `a` left join `app_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `app_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `app_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`app_spend_by_hour` `a` left join `app_spend_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`provider_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 5)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `app_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `app_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `app_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `app_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`app_spend_by_hour` `a` left join `app_spend_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`provider_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 6)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ch_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `ch_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ch_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ch_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ch_stats_by_hour` `a` left join `ch_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ch_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `ch_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ch_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ch_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ch_stats_by_hour` `a` left join `ch_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ch_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `ch_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ch_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ch_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`ch_stats_latest` `a` left join `ch_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ch_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `ch_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ch_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ch_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`ch_stats_latest` `a` left join `ch_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ch_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `ch_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ch_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ch_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ch_stats_by_day` `a` left join `ch_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ch_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `ch_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ch_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ch_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ch_stats_by_day` `a` left join `ch_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cluster_members_end_of_month`
--

/*!50001 DROP TABLE IF EXISTS `cluster_members_end_of_month`*/;
/*!50001 DROP VIEW IF EXISTS `cluster_members_end_of_month`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cluster_members_end_of_month` AS select `cluster_members`.`recorded_on` AS `recorded_on`,`cluster_members`.`group_uuid` AS `group_uuid`,`cluster_members`.`internal_name` AS `internal_name`,`cluster_members`.`group_name` AS `group_name`,`cluster_members`.`group_type` AS `group_type`,`cluster_members`.`member_uuid` AS `member_uuid`,`cluster_members`.`display_name` AS `display_name` from `cluster_members` where (`cluster_members`.`recorded_on` = `end_of_month`(`cluster_members`.`recorded_on`)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cluster_members_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `cluster_members_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `cluster_members_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cluster_members_yesterday` AS select `cluster_members`.`recorded_on` AS `recorded_on`,`cluster_members`.`group_uuid` AS `group_uuid`,`cluster_members`.`internal_name` AS `internal_name`,`cluster_members`.`group_name` AS `group_name`,`cluster_members`.`group_type` AS `group_type`,`cluster_members`.`member_uuid` AS `member_uuid`,`cluster_members`.`display_name` AS `display_name` from `cluster_members` where (`cluster_members`.`recorded_on` = cast((now() - interval 1 day) as date)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cluster_membership`
--

/*!50001 DROP TABLE IF EXISTS `cluster_membership`*/;
/*!50001 DROP VIEW IF EXISTS `cluster_membership`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cluster_membership` AS select distinct (cast(now() as date) - interval 1 day) AS `recorded_on`,`pm_group_members`.`group_uuid` AS `group_uuid`,`pm_group_members`.`internal_name` AS `internal_name`,`pm_group_members`.`group_name` AS `group_name`,`pm_group_members`.`group_type` AS `group_type`,`pm_group_members`.`member_uuid` AS `member_uuid`,`pm_group_members`.`display_name` AS `display_name` from (`pm_group_members` left join `cluster_membership_helper` on((`pm_group_members`.`group_uuid` = `cluster_membership_helper`.`uuid`))) where ((`pm_group_members`.`internal_name` like 'GROUP-PMsByCluster%') or (`cluster_membership_helper`.`uuid` is not null)) union all select distinct (cast(now() as date) - interval 1 day) AS `recorded_on`,`vm_group_members`.`group_uuid` AS `group_uuid`,`vm_group_members`.`internal_name` AS `internal_name`,`vm_group_members`.`group_name` AS `group_name`,`vm_group_members`.`group_type` AS `group_type`,`vm_group_members`.`member_uuid` AS `member_uuid`,`vm_group_members`.`display_name` AS `display_name` from (`vm_group_members` left join `cluster_membership_helper` on((`vm_group_members`.`group_uuid` = `cluster_membership_helper`.`uuid`))) where ((`vm_group_members`.`internal_name` like 'GROUP-VMsByCluster%') or (`cluster_membership_helper`.`uuid` is not null)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cluster_membership_helper`
--

/*!50001 DROP TABLE IF EXISTS `cluster_membership_helper`*/;
/*!50001 DROP VIEW IF EXISTS `cluster_membership_helper`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cluster_membership_helper` AS select distinct `entities`.`uuid` AS `uuid`,`entity_attrs`.`value` AS `val` from (`entity_attrs` join `entities` on((`entity_attrs`.`entity_entity_id` = `entities`.`id`))) where ((`entity_attrs`.`name` = 'groupStatsEnabled') and (`entity_attrs`.`value` = 'true')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cluster_stats_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `cluster_stats_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cluster_stats_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cluster_stats_monthly_ins_vw` AS select date_format(last_day(`a`.`recorded_on`),'%Y-%m-01') AS `recorded_on`,`a`.`internal_name` AS `internal_name`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`value`) AS `value` from (`cluster_stats_by_day` `a` left join `cluster_stats_by_month` `b` on(((date_format(last_day(`a`.`recorded_on`),'%Y-%m-01') <=> `b`.`recorded_on`) and (`a`.`internal_name` <=> `b`.`internal_name`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`recorded_on`) and isnull(`b`.`internal_name`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 2)) group by 1,2,3,4 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cluster_stats_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `cluster_stats_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cluster_stats_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cluster_stats_monthly_upd_vw` AS select date_format(last_day(`a`.`recorded_on`),'%Y-%m-01') AS `recorded_on`,`a`.`internal_name` AS `internal_name`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`value`) AS `value`,sum(`a`.`value`) AS `samples` from (`cluster_stats_by_day` `a` left join `cluster_stats_by_month` `b` on(((date_format(last_day(`a`.`recorded_on`),'%Y-%m-01') <=> `b`.`recorded_on`) and (`a`.`internal_name` <=> `b`.`internal_name`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`recorded_on` is not null) and (`b`.`internal_name` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cnt_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `cnt_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cnt_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cnt_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`cnt_stats_by_hour` `a` left join `cnt_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cnt_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `cnt_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cnt_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cnt_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`cnt_stats_by_hour` `a` left join `cnt_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cnt_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `cnt_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cnt_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cnt_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`cnt_stats_latest` `a` left join `cnt_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cnt_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `cnt_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cnt_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cnt_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`cnt_stats_latest` `a` left join `cnt_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cnt_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `cnt_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cnt_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cnt_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`cnt_stats_by_day` `a` left join `cnt_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `cnt_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `cnt_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `cnt_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `cnt_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`cnt_stats_by_day` `a` left join `cnt_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dpod_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `dpod_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `dpod_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `dpod_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`dpod_stats_by_hour` `a` left join `dpod_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dpod_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `dpod_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `dpod_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `dpod_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`dpod_stats_by_hour` `a` left join `dpod_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dpod_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `dpod_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `dpod_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `dpod_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`dpod_stats_latest` `a` left join `dpod_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dpod_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `dpod_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `dpod_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `dpod_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`dpod_stats_latest` `a` left join `dpod_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dpod_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `dpod_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `dpod_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `dpod_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`dpod_stats_by_day` `a` left join `dpod_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dpod_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `dpod_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `dpod_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `dpod_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`dpod_stats_by_day` `a` left join `dpod_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_capacity_by_day`
--

/*!50001 DROP TABLE IF EXISTS `ds_capacity_by_day`*/;
/*!50001 DROP VIEW IF EXISTS `ds_capacity_by_day`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_capacity_by_day` AS select 'Storage' AS `class_name`,`ds_stats_by_day`.`uuid` AS `uuid`,`ds_stats_by_day`.`producer_uuid` AS `producer_uuid`,`ds_stats_by_day`.`property_type` AS `property_type`,`ds_stats_by_day`.`avg_value` AS `utilization`,`ds_stats_by_day`.`capacity` AS `capacity`,round((`ds_stats_by_day`.`avg_value` * `ds_stats_by_day`.`capacity`),0) AS `used_capacity`,round(((1.0 - `ds_stats_by_day`.`avg_value`) * `ds_stats_by_day`.`capacity`),0) AS `available_capacity`,cast(`ds_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`ds_stats_by_day` join `ds_instances`) where ((`ds_stats_by_day`.`uuid` = `ds_instances`.`uuid`) and (`ds_stats_by_day`.`property_subtype` = 'utilization') and (`ds_stats_by_day`.`capacity` > 0.00)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_capacity_by_day_per_ds_group`
--

/*!50001 DROP TABLE IF EXISTS `ds_capacity_by_day_per_ds_group`*/;
/*!50001 DROP VIEW IF EXISTS `ds_capacity_by_day_per_ds_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_capacity_by_day_per_ds_group` AS select `ds_capacity_by_day`.`class_name` AS `class_name`,`ds_capacity_by_day`.`uuid` AS `uuid`,`ds_capacity_by_day`.`producer_uuid` AS `producer_uuid`,`ds_capacity_by_day`.`property_type` AS `property_type`,`ds_capacity_by_day`.`utilization` AS `utilization`,`ds_capacity_by_day`.`capacity` AS `capacity`,`ds_capacity_by_day`.`used_capacity` AS `used_capacity`,`ds_capacity_by_day`.`available_capacity` AS `available_capacity`,`ds_capacity_by_day`.`recorded_on` AS `recorded_on`,`ds_group_members`.`group_uuid` AS `group_uuid`,`ds_group_members`.`internal_name` AS `internal_name`,`ds_group_members`.`group_name` AS `group_name`,`ds_group_members`.`group_type` AS `group_type`,`ds_group_members`.`member_uuid` AS `member_uuid`,`ds_group_members`.`display_name` AS `display_name` from (`ds_capacity_by_day` join `ds_group_members`) where (`ds_group_members`.`member_uuid` = `ds_capacity_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_capacity_by_hour`
--

/*!50001 DROP TABLE IF EXISTS `ds_capacity_by_hour`*/;
/*!50001 DROP VIEW IF EXISTS `ds_capacity_by_hour`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_capacity_by_hour` AS select 'Storage' AS `class_name`,`ds_stats_by_hour`.`uuid` AS `uuid`,`ds_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`ds_stats_by_hour`.`property_type` AS `property_type`,`ds_stats_by_hour`.`avg_value` AS `utilization`,`ds_stats_by_hour`.`capacity` AS `capacity`,round((`ds_stats_by_hour`.`avg_value` * `ds_stats_by_hour`.`capacity`),0) AS `used_capacity`,round(((1.0 - `ds_stats_by_hour`.`avg_value`) * `ds_stats_by_hour`.`capacity`),0) AS `available_capacity`,cast(`ds_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`ds_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`ds_stats_by_hour` join `ds_instances`) where ((`ds_stats_by_hour`.`uuid` = `ds_instances`.`uuid`) and (`ds_stats_by_hour`.`property_subtype` = 'utilization') and (`ds_stats_by_hour`.`capacity` > 0.00)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `ds_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ds_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ds_stats_by_hour` `a` left join `ds_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `ds_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ds_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ds_stats_by_hour` `a` left join `ds_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_group_assns`
--

/*!50001 DROP TABLE IF EXISTS `ds_group_assns`*/;
/*!50001 DROP VIEW IF EXISTS `ds_group_assns`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_group_assns` AS select `entity_assns`.`id` AS `assn_id`,`ds_groups`.`group_uuid` AS `group_uuid`,`ds_groups`.`internal_name` AS `internal_name`,`ds_groups`.`group_name` AS `group_name`,`ds_groups`.`group_type` AS `group_type` from (`entity_assns` left join `ds_groups` on((`entity_assns`.`entity_entity_id` = `ds_groups`.`entity_id`))) where ((`entity_assns`.`name` = 'AllGroupMembers') and (`ds_groups`.`group_name` is not null) and (`ds_groups`.`group_type` is not null)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_group_members`
--

/*!50001 DROP TABLE IF EXISTS `ds_group_members`*/;
/*!50001 DROP VIEW IF EXISTS `ds_group_members`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_group_members` AS select distinct `ds_group_members_helper`.`group_uuid` AS `group_uuid`,`ds_group_members_helper`.`internal_name` AS `internal_name`,`ds_group_members_helper`.`group_name` AS `group_name`,`ds_group_members_helper`.`group_type` AS `group_type`,`entities`.`uuid` AS `member_uuid`,`entities`.`display_name` AS `display_name` from (`ds_group_members_helper` join `entities` on((`entities`.`id` = `ds_group_members_helper`.`entity_dest_id`))) where ((`entities`.`uuid` is not null) and (`entities`.`creation_class` = 'Storage')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_group_members_helper`
--

/*!50001 DROP TABLE IF EXISTS `ds_group_members_helper`*/;
/*!50001 DROP VIEW IF EXISTS `ds_group_members_helper`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_group_members_helper` AS select `entity_assns_members_entities`.`entity_dest_id` AS `entity_dest_id`,`ds_group_assns`.`group_uuid` AS `group_uuid`,`ds_group_assns`.`internal_name` AS `internal_name`,`ds_group_assns`.`group_name` AS `group_name`,`ds_group_assns`.`group_type` AS `group_type` from (`ds_group_assns` left join `entity_assns_members_entities` on((`ds_group_assns`.`assn_id` = `entity_assns_members_entities`.`entity_assn_src_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_groups`
--

/*!50001 DROP TABLE IF EXISTS `ds_groups`*/;
/*!50001 DROP VIEW IF EXISTS `ds_groups`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_groups` AS select `entity_groups`.`entity_id` AS `entity_id`,`entity_groups`.`group_uuid` AS `group_uuid`,`entity_groups`.`internal_name` AS `internal_name`,`entity_groups`.`group_name` AS `group_name`,`entity_groups`.`group_type` AS `group_type` from `entity_groups` where (`entity_groups`.`group_type` = 'Storage') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `ds_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ds_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`ds_stats_latest` `a` left join `ds_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `ds_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ds_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`ds_stats_latest` `a` left join `ds_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_instances`
--

/*!50001 DROP TABLE IF EXISTS `ds_instances`*/;
/*!50001 DROP VIEW IF EXISTS `ds_instances`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_instances` AS select `entities`.`name` AS `name`,`entities`.`display_name` AS `display_name`,`entities`.`uuid` AS `uuid` from `entities` where (`entities`.`creation_class` = 'Storage') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `ds_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ds_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ds_stats_by_day` `a` left join `ds_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `ds_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `ds_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`ds_stats_by_day` `a` left join `ds_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_stats_by_day_per_ds_group`
--

/*!50001 DROP TABLE IF EXISTS `ds_stats_by_day_per_ds_group`*/;
/*!50001 DROP VIEW IF EXISTS `ds_stats_by_day_per_ds_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_stats_by_day_per_ds_group` AS select `ds_group_members`.`group_uuid` AS `group_uuid`,`ds_group_members`.`internal_name` AS `internal_name`,`ds_group_members`.`group_name` AS `group_name`,`ds_group_members`.`group_type` AS `group_type`,`ds_group_members`.`member_uuid` AS `member_uuid`,`ds_group_members`.`display_name` AS `display_name`,`ds_stats_by_day`.`snapshot_time` AS `snapshot_time`,`ds_stats_by_day`.`uuid` AS `uuid`,`ds_stats_by_day`.`producer_uuid` AS `producer_uuid`,`ds_stats_by_day`.`property_type` AS `property_type`,`ds_stats_by_day`.`property_subtype` AS `property_subtype`,`ds_stats_by_day`.`capacity` AS `capacity`,`ds_stats_by_day`.`avg_value` AS `avg_value`,`ds_stats_by_day`.`min_value` AS `min_value`,`ds_stats_by_day`.`max_value` AS `max_value`,`ds_stats_by_day`.`relation` AS `relation`,`ds_stats_by_day`.`commodity_key` AS `commodity_key` from (`ds_group_members` join `ds_stats_by_day`) where (`ds_group_members`.`member_uuid` = `ds_stats_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_stats_by_hour_per_ds_group`
--

/*!50001 DROP TABLE IF EXISTS `ds_stats_by_hour_per_ds_group`*/;
/*!50001 DROP VIEW IF EXISTS `ds_stats_by_hour_per_ds_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_stats_by_hour_per_ds_group` AS select `ds_group_members`.`group_uuid` AS `group_uuid`,`ds_group_members`.`internal_name` AS `internal_name`,`ds_group_members`.`group_name` AS `group_name`,`ds_group_members`.`group_type` AS `group_type`,`ds_group_members`.`member_uuid` AS `member_uuid`,`ds_group_members`.`display_name` AS `display_name`,`ds_stats_by_hour`.`snapshot_time` AS `snapshot_time`,`ds_stats_by_hour`.`uuid` AS `uuid`,`ds_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`ds_stats_by_hour`.`property_type` AS `property_type`,`ds_stats_by_hour`.`property_subtype` AS `property_subtype`,`ds_stats_by_hour`.`capacity` AS `capacity`,`ds_stats_by_hour`.`avg_value` AS `avg_value`,`ds_stats_by_hour`.`min_value` AS `min_value`,`ds_stats_by_hour`.`max_value` AS `max_value`,`ds_stats_by_hour`.`relation` AS `relation`,`ds_stats_by_hour`.`commodity_key` AS `commodity_key` from (`ds_group_members` join `ds_stats_by_hour`) where (`ds_group_members`.`member_uuid` = `ds_stats_by_hour`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_util_info_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `ds_util_info_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `ds_util_info_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_util_info_yesterday` AS select `ds_util_stats_yesterday`.`snapshot_time` AS `snapshot_time`,`ds_instances`.`display_name` AS `display_name`,`ds_instances`.`uuid` AS `uuid`,`ds_util_stats_yesterday`.`producer_uuid` AS `producer_uuid`,`ds_util_stats_yesterday`.`property_type` AS `property_type`,`ds_util_stats_yesterday`.`property_subtype` AS `property_subtype`,`ds_util_stats_yesterday`.`capacity` AS `capacity`,`ds_util_stats_yesterday`.`avg_value` AS `avg_value`,`ds_util_stats_yesterday`.`min_value` AS `min_value`,`ds_util_stats_yesterday`.`max_value` AS `max_value` from (`ds_util_stats_yesterday` join `ds_instances`) where (`ds_util_stats_yesterday`.`uuid` = `ds_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ds_util_stats_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `ds_util_stats_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `ds_util_stats_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `ds_util_stats_yesterday` AS select `ds_stats_by_day`.`snapshot_time` AS `snapshot_time`,`ds_stats_by_day`.`uuid` AS `uuid`,`ds_stats_by_day`.`producer_uuid` AS `producer_uuid`,`ds_stats_by_day`.`property_type` AS `property_type`,`ds_stats_by_day`.`property_subtype` AS `property_subtype`,`ds_stats_by_day`.`capacity` AS `capacity`,`ds_stats_by_day`.`avg_value` AS `avg_value`,`ds_stats_by_day`.`min_value` AS `min_value`,`ds_stats_by_day`.`max_value` AS `max_value`,`ds_stats_by_day`.`relation` AS `relation`,`ds_stats_by_day`.`commodity_key` AS `commodity_key` from `ds_stats_by_day` where ((cast(`ds_stats_by_day`.`snapshot_time` as date) = (cast(now() as date) - interval 1 day)) and (`ds_stats_by_day`.`property_subtype` = 'utilization')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `entity_group_assns`
--

/*!50001 DROP TABLE IF EXISTS `entity_group_assns`*/;
/*!50001 DROP VIEW IF EXISTS `entity_group_assns`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `entity_group_assns` AS select `entity_assns`.`id` AS `assn_id`,`entity_groups`.`group_uuid` AS `group_uuid`,`entity_groups`.`internal_name` AS `internal_name`,`entity_groups`.`group_name` AS `group_name`,`entity_groups`.`group_type` AS `group_type` from (`entity_assns` left join `entity_groups` on((`entity_assns`.`entity_entity_id` = `entity_groups`.`entity_id`))) where ((`entity_assns`.`name` = 'AllGroupMembers') and (`entity_groups`.`group_name` is not null) and (`entity_groups`.`group_type` is not null)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `entity_group_members`
--

/*!50001 DROP TABLE IF EXISTS `entity_group_members`*/;
/*!50001 DROP VIEW IF EXISTS `entity_group_members`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `entity_group_members` AS select distinct `entity_group_members_helper`.`group_uuid` AS `group_uuid`,`entity_group_members_helper`.`internal_name` AS `internal_name`,`entity_group_members_helper`.`group_name` AS `group_name`,`entity_group_members_helper`.`group_type` AS `group_type`,`entities`.`uuid` AS `member_uuid`,`entities`.`display_name` AS `display_name` from (`entity_group_members_helper` join `entities` on((`entities`.`id` = `entity_group_members_helper`.`entity_dest_id`))) where (`entities`.`uuid` is not null) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `entity_group_members_helper`
--

/*!50001 DROP TABLE IF EXISTS `entity_group_members_helper`*/;
/*!50001 DROP VIEW IF EXISTS `entity_group_members_helper`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `entity_group_members_helper` AS select `entity_assns_members_entities`.`entity_dest_id` AS `entity_dest_id`,`entity_group_assns`.`group_uuid` AS `group_uuid`,`entity_group_assns`.`internal_name` AS `internal_name`,`entity_group_assns`.`group_name` AS `group_name`,`entity_group_assns`.`group_type` AS `group_type` from (`entity_group_assns` left join `entity_assns_members_entities` on((`entity_group_assns`.`assn_id` = `entity_assns_members_entities`.`entity_assn_src_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `entity_groups`
--

/*!50001 DROP TABLE IF EXISTS `entity_groups`*/;
/*!50001 DROP VIEW IF EXISTS `entity_groups`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `entity_groups` AS select `entities`.`id` AS `entity_id`,`entities`.`uuid` AS `group_uuid`,`entities`.`name` AS `internal_name`,`entities`.`display_name` AS `group_name`,max(if((`entity_attrs`.`name` = 'sETypeName'),`entity_attrs`.`value`,'')) AS `group_type` from (`entity_attrs` left join `entities` on((`entity_attrs`.`entity_entity_id` = `entities`.`id`))) where (`entity_attrs`.`name` = 'sETypeName') group by `entities`.`id` having ((`group_name` is not null) and (`group_name` <> '')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `iom_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `iom_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `iom_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `iom_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`iom_stats_by_hour` `a` left join `iom_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `iom_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `iom_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `iom_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `iom_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`iom_stats_by_hour` `a` left join `iom_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `iom_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `iom_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `iom_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `iom_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`iom_stats_latest` `a` left join `iom_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `iom_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `iom_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `iom_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `iom_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`iom_stats_latest` `a` left join `iom_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `iom_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `iom_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `iom_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `iom_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`iom_stats_by_day` `a` left join `iom_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `iom_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `iom_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `iom_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `iom_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`iom_stats_by_day` `a` left join `iom_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `notifications_risk`
--

/*!50001 DROP TABLE IF EXISTS `notifications_risk`*/;
/*!50001 DROP VIEW IF EXISTS `notifications_risk`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `notifications_risk` AS select `notifications`.`id` AS `id`,`notifications`.`clear_time` AS `clear_time`,`notifications`.`last_notify_time` AS `last_notify_time`,`notifications`.`severity` AS `severity`,`notifications`.`category` AS `category`,`notifications`.`name` AS `name`,`notifications`.`uuid` AS `uuid`,`notifications`.`importance` AS `importance`,`notifications`.`description` AS `description`,`notifications`.`notification_uuid` AS `notification_uuid`,substring_index(`notifications`.`name`,'::',-(1)) AS `risk` from `notifications` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_capacity_by_day`
--

/*!50001 DROP TABLE IF EXISTS `pm_capacity_by_day`*/;
/*!50001 DROP VIEW IF EXISTS `pm_capacity_by_day`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_capacity_by_day` AS select 'PhysicalMachine' AS `class_name`,`pm_stats_by_day`.`uuid` AS `uuid`,`pm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`pm_stats_by_day`.`property_type` AS `property_type`,`pm_stats_by_day`.`avg_value` AS `utilization`,`pm_stats_by_day`.`capacity` AS `capacity`,round((`pm_stats_by_day`.`avg_value` * `pm_stats_by_day`.`capacity`),0) AS `used_capacity`,round(((1.0 - `pm_stats_by_day`.`avg_value`) * `pm_stats_by_day`.`capacity`),0) AS `available_capacity`,cast(`pm_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`pm_stats_by_day` join `pm_instances`) where ((`pm_stats_by_day`.`uuid` = `pm_instances`.`uuid`) and (`pm_stats_by_day`.`property_subtype` = 'utilization') and (`pm_stats_by_day`.`capacity` > 0.00)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_capacity_by_day_per_pm_group`
--

/*!50001 DROP TABLE IF EXISTS `pm_capacity_by_day_per_pm_group`*/;
/*!50001 DROP VIEW IF EXISTS `pm_capacity_by_day_per_pm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_capacity_by_day_per_pm_group` AS select `pm_capacity_by_day`.`class_name` AS `class_name`,`pm_capacity_by_day`.`uuid` AS `uuid`,`pm_capacity_by_day`.`producer_uuid` AS `producer_uuid`,`pm_capacity_by_day`.`property_type` AS `property_type`,`pm_capacity_by_day`.`utilization` AS `utilization`,`pm_capacity_by_day`.`capacity` AS `capacity`,`pm_capacity_by_day`.`used_capacity` AS `used_capacity`,`pm_capacity_by_day`.`available_capacity` AS `available_capacity`,`pm_capacity_by_day`.`recorded_on` AS `recorded_on`,`pm_group_members`.`group_uuid` AS `group_uuid`,`pm_group_members`.`internal_name` AS `internal_name`,`pm_group_members`.`group_name` AS `group_name`,`pm_group_members`.`group_type` AS `group_type`,`pm_group_members`.`member_uuid` AS `member_uuid`,`pm_group_members`.`display_name` AS `display_name` from (`pm_capacity_by_day` join `pm_group_members`) where (`pm_group_members`.`member_uuid` = `pm_capacity_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_capacity_by_hour`
--

/*!50001 DROP TABLE IF EXISTS `pm_capacity_by_hour`*/;
/*!50001 DROP VIEW IF EXISTS `pm_capacity_by_hour`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_capacity_by_hour` AS select 'PhysicalMachine' AS `class_name`,`pm_instances`.`display_name` AS `display_name`,`pm_stats_by_hour`.`uuid` AS `uuid`,`pm_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`pm_stats_by_hour`.`property_type` AS `property_type`,`pm_stats_by_hour`.`avg_value` AS `utilization`,`pm_stats_by_hour`.`capacity` AS `capacity`,round((`pm_stats_by_hour`.`avg_value` * `pm_stats_by_hour`.`capacity`),0) AS `used_capacity`,round(((1.0 - `pm_stats_by_hour`.`avg_value`) * `pm_stats_by_hour`.`capacity`),0) AS `available_capacity`,cast(`pm_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`pm_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`pm_stats_by_hour` join `pm_instances`) where ((`pm_stats_by_hour`.`uuid` = `pm_instances`.`uuid`) and (`pm_stats_by_hour`.`property_subtype` = 'utilization') and (`pm_stats_by_hour`.`capacity` > 0.00)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `pm_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `pm_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`pm_stats_by_hour` `a` left join `pm_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `pm_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `pm_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`pm_stats_by_hour` `a` left join `pm_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_group_assns`
--

/*!50001 DROP TABLE IF EXISTS `pm_group_assns`*/;
/*!50001 DROP VIEW IF EXISTS `pm_group_assns`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_group_assns` AS select `entity_assns`.`id` AS `assn_id`,`pm_groups`.`group_uuid` AS `group_uuid`,`pm_groups`.`internal_name` AS `internal_name`,`pm_groups`.`group_name` AS `group_name`,`pm_groups`.`group_type` AS `group_type` from (`entity_assns` left join `pm_groups` on((`entity_assns`.`entity_entity_id` = `pm_groups`.`entity_id`))) where ((`entity_assns`.`name` = 'AllGroupMembers') and (`pm_groups`.`group_name` is not null) and (`pm_groups`.`group_type` is not null)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_group_members`
--

/*!50001 DROP TABLE IF EXISTS `pm_group_members`*/;
/*!50001 DROP VIEW IF EXISTS `pm_group_members`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_group_members` AS select distinct `pm_group_members_helper`.`group_uuid` AS `group_uuid`,`pm_group_members_helper`.`internal_name` AS `internal_name`,`pm_group_members_helper`.`group_name` AS `group_name`,`pm_group_members_helper`.`group_type` AS `group_type`,`entities`.`uuid` AS `member_uuid`,`entities`.`display_name` AS `display_name` from (`pm_group_members_helper` join `entities` on((`entities`.`id` = `pm_group_members_helper`.`entity_dest_id`))) where ((`entities`.`uuid` is not null) and (`entities`.`creation_class` = 'PhysicalMachine')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_group_members_helper`
--

/*!50001 DROP TABLE IF EXISTS `pm_group_members_helper`*/;
/*!50001 DROP VIEW IF EXISTS `pm_group_members_helper`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_group_members_helper` AS select `entity_assns_members_entities`.`entity_dest_id` AS `entity_dest_id`,`pm_group_assns`.`group_uuid` AS `group_uuid`,`pm_group_assns`.`internal_name` AS `internal_name`,`pm_group_assns`.`group_name` AS `group_name`,`pm_group_assns`.`group_type` AS `group_type` from (`pm_group_assns` left join `entity_assns_members_entities` on((`pm_group_assns`.`assn_id` = `entity_assns_members_entities`.`entity_assn_src_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_groups`
--

/*!50001 DROP TABLE IF EXISTS `pm_groups`*/;
/*!50001 DROP VIEW IF EXISTS `pm_groups`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_groups` AS select `entity_groups`.`entity_id` AS `entity_id`,`entity_groups`.`group_uuid` AS `group_uuid`,`entity_groups`.`internal_name` AS `internal_name`,`entity_groups`.`group_name` AS `group_name`,`entity_groups`.`group_type` AS `group_type` from `entity_groups` where (`entity_groups`.`group_type` = 'PhysicalMachine') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `pm_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `pm_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`pm_stats_latest` `a` left join `pm_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `pm_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `pm_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`pm_stats_latest` `a` left join `pm_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_instances`
--

/*!50001 DROP TABLE IF EXISTS `pm_instances`*/;
/*!50001 DROP VIEW IF EXISTS `pm_instances`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_instances` AS select `entities`.`name` AS `name`,`entities`.`display_name` AS `display_name`,`entities`.`uuid` AS `uuid` from `entities` where (`entities`.`creation_class` = 'PhysicalMachine') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `pm_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `pm_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`pm_stats_by_day` `a` left join `pm_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `pm_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `pm_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`pm_stats_by_day` `a` left join `pm_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_stats_by_day_per_pm_group`
--

/*!50001 DROP TABLE IF EXISTS `pm_stats_by_day_per_pm_group`*/;
/*!50001 DROP VIEW IF EXISTS `pm_stats_by_day_per_pm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_stats_by_day_per_pm_group` AS select `pm_group_members`.`group_uuid` AS `group_uuid`,`pm_group_members`.`internal_name` AS `internal_name`,`pm_group_members`.`group_name` AS `group_name`,`pm_group_members`.`group_type` AS `group_type`,`pm_group_members`.`member_uuid` AS `member_uuid`,`pm_group_members`.`display_name` AS `display_name`,`pm_stats_by_day`.`snapshot_time` AS `snapshot_time`,`pm_stats_by_day`.`uuid` AS `uuid`,`pm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`pm_stats_by_day`.`property_type` AS `property_type`,`pm_stats_by_day`.`property_subtype` AS `property_subtype`,`pm_stats_by_day`.`capacity` AS `capacity`,`pm_stats_by_day`.`avg_value` AS `avg_value`,`pm_stats_by_day`.`min_value` AS `min_value`,`pm_stats_by_day`.`max_value` AS `max_value`,`pm_stats_by_day`.`relation` AS `relation`,`pm_stats_by_day`.`commodity_key` AS `commodity_key` from (`pm_group_members` join `pm_stats_by_day`) where (`pm_group_members`.`member_uuid` = `pm_stats_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_stats_by_hour_per_pm_group`
--

/*!50001 DROP TABLE IF EXISTS `pm_stats_by_hour_per_pm_group`*/;
/*!50001 DROP VIEW IF EXISTS `pm_stats_by_hour_per_pm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_stats_by_hour_per_pm_group` AS select `pm_group_members`.`group_uuid` AS `group_uuid`,`pm_group_members`.`internal_name` AS `internal_name`,`pm_group_members`.`group_name` AS `group_name`,`pm_group_members`.`group_type` AS `group_type`,`pm_group_members`.`member_uuid` AS `member_uuid`,`pm_group_members`.`display_name` AS `display_name`,`pm_stats_by_hour`.`snapshot_time` AS `snapshot_time`,`pm_stats_by_hour`.`uuid` AS `uuid`,`pm_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`pm_stats_by_hour`.`property_type` AS `property_type`,`pm_stats_by_hour`.`property_subtype` AS `property_subtype`,`pm_stats_by_hour`.`capacity` AS `capacity`,`pm_stats_by_hour`.`avg_value` AS `avg_value`,`pm_stats_by_hour`.`min_value` AS `min_value`,`pm_stats_by_hour`.`max_value` AS `max_value`,`pm_stats_by_hour`.`relation` AS `relation`,`pm_stats_by_hour`.`commodity_key` AS `commodity_key` from (`pm_group_members` join `pm_stats_by_hour`) where (`pm_group_members`.`member_uuid` = `pm_stats_by_hour`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_util_info_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `pm_util_info_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `pm_util_info_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_util_info_yesterday` AS select `pm_util_stats_yesterday`.`snapshot_time` AS `snapshot_time`,`pm_instances`.`display_name` AS `display_name`,`pm_instances`.`uuid` AS `uuid`,`pm_util_stats_yesterday`.`producer_uuid` AS `producer_uuid`,`pm_util_stats_yesterday`.`property_type` AS `property_type`,`pm_util_stats_yesterday`.`property_subtype` AS `property_subtype`,`pm_util_stats_yesterday`.`capacity` AS `capacity`,`pm_util_stats_yesterday`.`avg_value` AS `avg_value`,`pm_util_stats_yesterday`.`min_value` AS `min_value`,`pm_util_stats_yesterday`.`max_value` AS `max_value` from (`pm_util_stats_yesterday` join `pm_instances`) where (`pm_util_stats_yesterday`.`uuid` = `pm_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_util_stats_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `pm_util_stats_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `pm_util_stats_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_util_stats_yesterday` AS select `pm_stats_by_day`.`snapshot_time` AS `snapshot_time`,`pm_stats_by_day`.`uuid` AS `uuid`,`pm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`pm_stats_by_day`.`property_type` AS `property_type`,`pm_stats_by_day`.`property_subtype` AS `property_subtype`,`pm_stats_by_day`.`capacity` AS `capacity`,`pm_stats_by_day`.`avg_value` AS `avg_value`,`pm_stats_by_day`.`min_value` AS `min_value`,`pm_stats_by_day`.`max_value` AS `max_value`,`pm_stats_by_day`.`relation` AS `relation`,`pm_stats_by_day`.`commodity_key` AS `commodity_key` from `pm_stats_by_day` where ((cast(`pm_stats_by_day`.`snapshot_time` as date) = (cast(now() as date) - interval 1 day)) and (`pm_stats_by_day`.`property_subtype` = 'utilization')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `pm_vm_count_by_day_per_pm_group`
--

/*!50001 DROP TABLE IF EXISTS `pm_vm_count_by_day_per_pm_group`*/;
/*!50001 DROP VIEW IF EXISTS `pm_vm_count_by_day_per_pm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `pm_vm_count_by_day_per_pm_group` AS select `pm_stats_by_day_per_pm_group`.`group_uuid` AS `group_uuid`,`pm_stats_by_day_per_pm_group`.`group_name` AS `group_name`,`pm_stats_by_day_per_pm_group`.`group_type` AS `group_type`,cast(`pm_stats_by_day_per_pm_group`.`snapshot_time` as date) AS `recorded_on`,'HostedVMs' AS `property_type`,'count' AS `property_subtype`,round(sum(`pm_stats_by_day_per_pm_group`.`avg_value`),0) AS `vm_count` from `pm_stats_by_day_per_pm_group` where ((`pm_stats_by_day_per_pm_group`.`group_type` = 'PhysicalMachine') and (`pm_stats_by_day_per_pm_group`.`property_type` = 'Produces')) group by `pm_stats_by_day_per_pm_group`.`group_name`,cast(`pm_stats_by_day_per_pm_group`.`snapshot_time` as date) order by `pm_stats_by_day_per_pm_group`.`group_name`,cast(`pm_stats_by_day_per_pm_group`.`snapshot_time` as date) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sc_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `sc_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sc_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sc_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sc_stats_by_hour` `a` left join `sc_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sc_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `sc_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sc_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sc_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sc_stats_by_hour` `a` left join `sc_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sc_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `sc_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sc_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sc_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`sc_stats_latest` `a` left join `sc_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sc_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `sc_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sc_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sc_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`sc_stats_latest` `a` left join `sc_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sc_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `sc_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sc_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sc_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sc_stats_by_day` `a` left join `sc_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sc_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `sc_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sc_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sc_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sc_stats_by_day` `a` left join `sc_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `service_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `service_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `service_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `service_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,count(0) AS `samples`,count(0) AS `new_samples` from (`service_spend_by_hour` `a` left join `service_spend_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`provider_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `service_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `service_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `service_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `service_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,count(0) AS `samples`,count(0) AS `new_samples` from (`service_spend_by_hour` `a` left join `service_spend_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`provider_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `service_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `service_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `service_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `service_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`service_spend_by_hour` `a` left join `service_spend_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`provider_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 5)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `service_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `service_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `service_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `service_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`service_spend_by_hour` `a` left join `service_spend_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`provider_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 6)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sw_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `sw_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sw_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sw_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sw_stats_by_hour` `a` left join `sw_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sw_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `sw_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sw_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sw_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sw_stats_by_hour` `a` left join `sw_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sw_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `sw_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sw_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sw_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`sw_stats_latest` `a` left join `sw_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sw_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `sw_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sw_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sw_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`sw_stats_latest` `a` left join `sw_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sw_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `sw_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sw_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sw_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sw_stats_by_day` `a` left join `sw_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `sw_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `sw_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `sw_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `sw_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`sw_stats_by_day` `a` left join `sw_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_ds_stats_by_day`
--

/*!50001 DROP TABLE IF EXISTS `user_ds_stats_by_day`*/;
/*!50001 DROP VIEW IF EXISTS `user_ds_stats_by_day`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_ds_stats_by_day` AS select `ds_instances`.`display_name` AS `instance_name`,`ds_stats_by_day`.`property_type` AS `property_type`,`ds_stats_by_day`.`property_subtype` AS `property_subtype`,`ds_stats_by_day`.`capacity` AS `capacity`,`ds_stats_by_day`.`avg_value` AS `avg_value`,`ds_stats_by_day`.`min_value` AS `min_value`,`ds_stats_by_day`.`max_value` AS `max_value`,cast(`ds_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`ds_stats_by_day` join `ds_instances`) where (`ds_stats_by_day`.`uuid` = `ds_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_ds_stats_by_hour`
--

/*!50001 DROP TABLE IF EXISTS `user_ds_stats_by_hour`*/;
/*!50001 DROP VIEW IF EXISTS `user_ds_stats_by_hour`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_ds_stats_by_hour` AS select `ds_instances`.`display_name` AS `instance_name`,`ds_stats_by_hour`.`property_type` AS `property_type`,`ds_stats_by_hour`.`property_subtype` AS `property_subtype`,`ds_stats_by_hour`.`capacity` AS `capacity`,`ds_stats_by_hour`.`avg_value` AS `avg_value`,`ds_stats_by_hour`.`min_value` AS `min_value`,`ds_stats_by_hour`.`max_value` AS `max_value`,cast(`ds_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`ds_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`ds_stats_by_hour` join `ds_instances`) where (`ds_stats_by_hour`.`uuid` = `ds_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_pm_stats_by_day`
--

/*!50001 DROP TABLE IF EXISTS `user_pm_stats_by_day`*/;
/*!50001 DROP VIEW IF EXISTS `user_pm_stats_by_day`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_pm_stats_by_day` AS select `pm_instances`.`display_name` AS `instance_name`,`pm_stats_by_day`.`property_type` AS `property_type`,`pm_stats_by_day`.`property_subtype` AS `property_subtype`,`pm_stats_by_day`.`capacity` AS `capacity`,`pm_stats_by_day`.`avg_value` AS `avg_value`,`pm_stats_by_day`.`min_value` AS `min_value`,`pm_stats_by_day`.`max_value` AS `max_value`,cast(`pm_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`pm_stats_by_day` join `pm_instances`) where (`pm_stats_by_day`.`uuid` = `pm_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_pm_stats_by_hour`
--

/*!50001 DROP TABLE IF EXISTS `user_pm_stats_by_hour`*/;
/*!50001 DROP VIEW IF EXISTS `user_pm_stats_by_hour`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_pm_stats_by_hour` AS select `pm_instances`.`display_name` AS `instance_name`,`pm_stats_by_hour`.`property_type` AS `property_type`,`pm_stats_by_hour`.`property_subtype` AS `property_subtype`,`pm_stats_by_hour`.`capacity` AS `capacity`,`pm_stats_by_hour`.`avg_value` AS `avg_value`,`pm_stats_by_hour`.`min_value` AS `min_value`,`pm_stats_by_hour`.`max_value` AS `max_value`,cast(`pm_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`pm_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`pm_stats_by_hour` join `pm_instances`) where (`pm_stats_by_hour`.`uuid` = `pm_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_vm_stats_by_day`
--

/*!50001 DROP TABLE IF EXISTS `user_vm_stats_by_day`*/;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_day`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_vm_stats_by_day` AS select `vm_instances`.`display_name` AS `instance_name`,`vm_stats_by_day`.`property_type` AS `property_type`,`vm_stats_by_day`.`property_subtype` AS `property_subtype`,`vm_stats_by_day`.`capacity` AS `capacity`,`vm_stats_by_day`.`avg_value` AS `avg_value`,`vm_stats_by_day`.`min_value` AS `min_value`,`vm_stats_by_day`.`max_value` AS `max_value`,cast(`vm_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`vm_stats_by_day` join `vm_instances`) where (`vm_stats_by_day`.`uuid` = `vm_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_vm_stats_by_day_per_group`
--

/*!50001 DROP TABLE IF EXISTS `user_vm_stats_by_day_per_group`*/;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_day_per_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_vm_stats_by_day_per_group` AS select `vm_group_members`.`group_name` AS `group_name`,`vm_group_members`.`display_name` AS `instance_name`,`vm_stats_by_day`.`property_type` AS `property_type`,`vm_stats_by_day`.`property_subtype` AS `property_subtype`,`vm_stats_by_day`.`capacity` AS `capacity`,`vm_stats_by_day`.`avg_value` AS `avg_value`,`vm_stats_by_day`.`min_value` AS `min_value`,`vm_stats_by_day`.`max_value` AS `max_value`,cast(`vm_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`vm_stats_by_day` join `vm_group_members`) where (`vm_group_members`.`member_uuid` = `vm_stats_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_vm_stats_by_hour`
--

/*!50001 DROP TABLE IF EXISTS `user_vm_stats_by_hour`*/;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_hour`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_vm_stats_by_hour` AS select `vm_instances`.`display_name` AS `instance_name`,`vm_stats_by_hour`.`property_type` AS `property_type`,`vm_stats_by_hour`.`property_subtype` AS `property_subtype`,`vm_stats_by_hour`.`capacity` AS `capacity`,`vm_stats_by_hour`.`avg_value` AS `avg_value`,`vm_stats_by_hour`.`min_value` AS `min_value`,`vm_stats_by_hour`.`max_value` AS `max_value`,cast(`vm_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`vm_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`vm_stats_by_hour` join `vm_instances`) where (`vm_stats_by_hour`.`uuid` = `vm_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `user_vm_stats_by_hour_per_group`
--

/*!50001 DROP TABLE IF EXISTS `user_vm_stats_by_hour_per_group`*/;
/*!50001 DROP VIEW IF EXISTS `user_vm_stats_by_hour_per_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `user_vm_stats_by_hour_per_group` AS select `vm_group_members`.`group_name` AS `group_name`,`vm_group_members`.`display_name` AS `instance_name`,`vm_stats_by_hour`.`property_type` AS `property_type`,`vm_stats_by_hour`.`property_subtype` AS `property_subtype`,`vm_stats_by_hour`.`capacity` AS `capacity`,`vm_stats_by_hour`.`avg_value` AS `avg_value`,`vm_stats_by_hour`.`min_value` AS `min_value`,`vm_stats_by_hour`.`max_value` AS `max_value`,cast(`vm_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`vm_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`vm_stats_by_hour` join `vm_group_members`) where (`vm_group_members`.`member_uuid` = `vm_stats_by_hour`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vdc_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vdc_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vdc_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vdc_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vdc_stats_by_hour` `a` left join `vdc_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vdc_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vdc_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vdc_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vdc_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vdc_stats_by_hour` `a` left join `vdc_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vdc_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vdc_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vdc_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vdc_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`vdc_stats_latest` `a` left join `vdc_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vdc_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vdc_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vdc_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vdc_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`vdc_stats_latest` `a` left join `vdc_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vdc_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vdc_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vdc_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vdc_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vdc_stats_by_day` `a` left join `vdc_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vdc_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vdc_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vdc_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vdc_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vdc_stats_by_day` `a` left join `vdc_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_capacity_by_day`
--

/*!50001 DROP TABLE IF EXISTS `vm_capacity_by_day`*/;
/*!50001 DROP VIEW IF EXISTS `vm_capacity_by_day`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_capacity_by_day` AS select 'VirtualMachine' AS `class_name`,`vm_stats_by_day`.`uuid` AS `uuid`,`vm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_day`.`property_type` AS `property_type`,`vm_stats_by_day`.`avg_value` AS `utilization`,`vm_stats_by_day`.`capacity` AS `capacity`,round((`vm_stats_by_day`.`avg_value` * `vm_stats_by_day`.`capacity`),0) AS `used_capacity`,round(((1.0 - `vm_stats_by_day`.`avg_value`) * `vm_stats_by_day`.`capacity`),0) AS `available_capacity`,cast(`vm_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`vm_stats_by_day` join `vm_instances`) where ((`vm_stats_by_day`.`uuid` = `vm_instances`.`uuid`) and (`vm_stats_by_day`.`property_subtype` = 'utilization') and (`vm_stats_by_day`.`capacity` > 0.00)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_capacity_by_day_per_vm_group`
--

/*!50001 DROP TABLE IF EXISTS `vm_capacity_by_day_per_vm_group`*/;
/*!50001 DROP VIEW IF EXISTS `vm_capacity_by_day_per_vm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_capacity_by_day_per_vm_group` AS select `vm_capacity_by_day`.`class_name` AS `class_name`,`vm_capacity_by_day`.`uuid` AS `uuid`,`vm_capacity_by_day`.`producer_uuid` AS `producer_uuid`,`vm_capacity_by_day`.`property_type` AS `property_type`,`vm_capacity_by_day`.`utilization` AS `utilization`,`vm_capacity_by_day`.`capacity` AS `capacity`,`vm_capacity_by_day`.`used_capacity` AS `used_capacity`,`vm_capacity_by_day`.`available_capacity` AS `available_capacity`,`vm_capacity_by_day`.`recorded_on` AS `recorded_on`,`vm_group_members`.`group_uuid` AS `group_uuid`,`vm_group_members`.`internal_name` AS `internal_name`,`vm_group_members`.`group_name` AS `group_name`,`vm_group_members`.`group_type` AS `group_type`,`vm_group_members`.`member_uuid` AS `member_uuid`,`vm_group_members`.`display_name` AS `display_name` from (`vm_capacity_by_day` join `vm_group_members`) where (`vm_group_members`.`member_uuid` = `vm_capacity_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_capacity_by_hour`
--

/*!50001 DROP TABLE IF EXISTS `vm_capacity_by_hour`*/;
/*!50001 DROP VIEW IF EXISTS `vm_capacity_by_hour`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_capacity_by_hour` AS select 'VirtualMachine' AS `class_name`,`vm_stats_by_hour`.`uuid` AS `uuid`,`vm_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_hour`.`property_type` AS `property_type`,`vm_stats_by_hour`.`avg_value` AS `utilization`,`vm_stats_by_hour`.`capacity` AS `capacity`,round((`vm_stats_by_hour`.`avg_value` * `vm_stats_by_hour`.`capacity`),0) AS `used_capacity`,round(((1.0 - `vm_stats_by_hour`.`avg_value`) * `vm_stats_by_hour`.`capacity`),0) AS `available_capacity`,cast(`vm_stats_by_hour`.`snapshot_time` as date) AS `recorded_on`,hour(`vm_stats_by_hour`.`snapshot_time`) AS `hour_number` from (`vm_stats_by_hour` join `vm_instances`) where ((`vm_stats_by_hour`.`uuid` = `vm_instances`.`uuid`) and (`vm_stats_by_hour`.`property_subtype` = 'utilization') and (`vm_stats_by_hour`.`capacity` > 0.00)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vm_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vm_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,count(0) AS `samples`,count(0) AS `new_samples` from (`vm_spend_by_hour` `a` left join `vm_spend_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`provider_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vm_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vm_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,count(0) AS `samples`,count(0) AS `new_samples` from (`vm_spend_by_hour` `a` left join `vm_spend_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`provider_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_group_assns`
--

/*!50001 DROP TABLE IF EXISTS `vm_group_assns`*/;
/*!50001 DROP VIEW IF EXISTS `vm_group_assns`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_group_assns` AS select `entity_assns`.`id` AS `assn_id`,`vm_groups`.`group_uuid` AS `group_uuid`,`vm_groups`.`internal_name` AS `internal_name`,`vm_groups`.`group_name` AS `group_name`,`vm_groups`.`group_type` AS `group_type` from (`entity_assns` left join `vm_groups` on((`entity_assns`.`entity_entity_id` = `vm_groups`.`entity_id`))) where ((`entity_assns`.`name` = 'AllGroupMembers') and (`vm_groups`.`group_name` is not null) and (`vm_groups`.`group_type` is not null)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_group_members`
--

/*!50001 DROP TABLE IF EXISTS `vm_group_members`*/;
/*!50001 DROP VIEW IF EXISTS `vm_group_members`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_group_members` AS select distinct `vm_group_members_helper`.`group_uuid` AS `group_uuid`,`vm_group_members_helper`.`internal_name` AS `internal_name`,`vm_group_members_helper`.`group_name` AS `group_name`,`vm_group_members_helper`.`group_type` AS `group_type`,`entities`.`uuid` AS `member_uuid`,`entities`.`display_name` AS `display_name` from (`vm_group_members_helper` join `entities` on((`entities`.`id` = `vm_group_members_helper`.`entity_dest_id`))) where ((`entities`.`uuid` is not null) and (`entities`.`creation_class` = 'VirtualMachine')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_group_members_agg`
--

/*!50001 DROP TABLE IF EXISTS `vm_group_members_agg`*/;
/*!50001 DROP VIEW IF EXISTS `vm_group_members_agg`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_group_members_agg` AS select distinct `vm_group_members_helper`.`group_uuid` AS `group_uuid`,`vm_group_members_helper`.`internal_name` AS `internal_name`,`vm_group_members_helper`.`group_name` AS `group_name`,`vm_group_members_helper`.`group_type` AS `group_type`,`entities`.`uuid` AS `member_uuid`,`entities`.`display_name` AS `display_name` from (`vm_group_members_helper` join `entities` on((`entities`.`id` = `vm_group_members_helper`.`entity_dest_id`))) where ((`entities`.`uuid` is not null) and (`vm_group_members_helper`.`group_name` like 'VMs_%') and (not((`vm_group_members_helper`.`internal_name` like '%VMsByStorage%'))) and (not((`vm_group_members_helper`.`internal_name` like '%VMsByNetwork%'))) and (`entities`.`creation_class` = 'VirtualMachine')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_group_members_helper`
--

/*!50001 DROP TABLE IF EXISTS `vm_group_members_helper`*/;
/*!50001 DROP VIEW IF EXISTS `vm_group_members_helper`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_group_members_helper` AS select `entity_assns_members_entities`.`entity_dest_id` AS `entity_dest_id`,`vm_group_assns`.`group_uuid` AS `group_uuid`,`vm_group_assns`.`internal_name` AS `internal_name`,`vm_group_assns`.`group_name` AS `group_name`,`vm_group_assns`.`group_type` AS `group_type` from (`vm_group_assns` left join `entity_assns_members_entities` on((`vm_group_assns`.`assn_id` = `entity_assns_members_entities`.`entity_assn_src_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_groups`
--

/*!50001 DROP TABLE IF EXISTS `vm_groups`*/;
/*!50001 DROP VIEW IF EXISTS `vm_groups`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_groups` AS select `entity_groups`.`entity_id` AS `entity_id`,`entity_groups`.`group_uuid` AS `group_uuid`,`entity_groups`.`internal_name` AS `internal_name`,`entity_groups`.`group_name` AS `group_name`,`entity_groups`.`group_type` AS `group_type` from `entity_groups` where (`entity_groups`.`group_type` = 'VirtualMachine') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vm_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vm_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`vm_stats_latest` `a` left join `vm_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vm_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vm_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`vm_stats_latest` `a` left join `vm_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_instances`
--

/*!50001 DROP TABLE IF EXISTS `vm_instances`*/;
/*!50001 DROP VIEW IF EXISTS `vm_instances`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_instances` AS select `entities`.`name` AS `name`,`entities`.`display_name` AS `display_name`,`entities`.`uuid` AS `uuid` from `entities` where (`entities`.`creation_class` = 'VirtualMachine') */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vm_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vm_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vm_spend_by_hour` `a` left join `vm_spend_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`provider_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and (`a`.`aggregated` = 5)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vm_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vm_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`provider_uuid` AS `provider_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,avg(`a`.`rate`) AS `rate`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vm_spend_by_hour` `a` left join `vm_spend_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`provider_uuid` <=> `b`.`provider_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`provider_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`a`.`aggregated` = 6)) group by 1,2,3,4,5 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_stats_by_day_per_vm_group`
--

/*!50001 DROP TABLE IF EXISTS `vm_stats_by_day_per_vm_group`*/;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_day_per_vm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_stats_by_day_per_vm_group` AS select `vm_group_members`.`group_uuid` AS `group_uuid`,`vm_group_members`.`internal_name` AS `internal_name`,`vm_group_members`.`group_name` AS `group_name`,`vm_group_members`.`group_type` AS `group_type`,`vm_group_members`.`member_uuid` AS `member_uuid`,`vm_group_members`.`display_name` AS `display_name`,`vm_stats_by_day`.`snapshot_time` AS `snapshot_time`,`vm_stats_by_day`.`uuid` AS `uuid`,`vm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_day`.`property_type` AS `property_type`,`vm_stats_by_day`.`property_subtype` AS `property_subtype`,`vm_stats_by_day`.`capacity` AS `capacity`,`vm_stats_by_day`.`avg_value` AS `avg_value`,`vm_stats_by_day`.`min_value` AS `min_value`,`vm_stats_by_day`.`max_value` AS `max_value`,`vm_stats_by_day`.`relation` AS `relation`,`vm_stats_by_day`.`commodity_key` AS `commodity_key` from (`vm_group_members` join `vm_stats_by_day`) where (`vm_group_members`.`member_uuid` = `vm_stats_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_stats_by_day_per_vm_group_agg`
--

/*!50001 DROP TABLE IF EXISTS `vm_stats_by_day_per_vm_group_agg`*/;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_day_per_vm_group_agg`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_stats_by_day_per_vm_group_agg` AS select `vm_group_members_agg`.`group_uuid` AS `group_uuid`,`vm_group_members_agg`.`internal_name` AS `internal_name`,`vm_group_members_agg`.`group_name` AS `group_name`,`vm_group_members_agg`.`group_type` AS `group_type`,`vm_group_members_agg`.`member_uuid` AS `member_uuid`,`vm_group_members_agg`.`display_name` AS `display_name`,`vm_stats_by_day`.`snapshot_time` AS `snapshot_time`,`vm_stats_by_day`.`uuid` AS `uuid`,`vm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_day`.`property_type` AS `property_type`,`vm_stats_by_day`.`property_subtype` AS `property_subtype`,`vm_stats_by_day`.`capacity` AS `capacity`,`vm_stats_by_day`.`avg_value` AS `avg_value`,`vm_stats_by_day`.`min_value` AS `min_value`,`vm_stats_by_day`.`max_value` AS `max_value`,`vm_stats_by_day`.`relation` AS `relation`,`vm_stats_by_day`.`commodity_key` AS `commodity_key` from (`vm_group_members_agg` join `vm_stats_by_day`) where (`vm_group_members_agg`.`member_uuid` = `vm_stats_by_day`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_stats_by_hour_per_vm_group`
--

/*!50001 DROP TABLE IF EXISTS `vm_stats_by_hour_per_vm_group`*/;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_hour_per_vm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_stats_by_hour_per_vm_group` AS select `vm_group_members`.`group_uuid` AS `group_uuid`,`vm_group_members`.`internal_name` AS `internal_name`,`vm_group_members`.`group_name` AS `group_name`,`vm_group_members`.`group_type` AS `group_type`,`vm_group_members`.`member_uuid` AS `member_uuid`,`vm_group_members`.`display_name` AS `display_name`,`vm_stats_by_hour`.`snapshot_time` AS `snapshot_time`,`vm_stats_by_hour`.`uuid` AS `uuid`,`vm_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_hour`.`property_type` AS `property_type`,`vm_stats_by_hour`.`property_subtype` AS `property_subtype`,`vm_stats_by_hour`.`capacity` AS `capacity`,`vm_stats_by_hour`.`avg_value` AS `avg_value`,`vm_stats_by_hour`.`min_value` AS `min_value`,`vm_stats_by_hour`.`max_value` AS `max_value`,`vm_stats_by_hour`.`relation` AS `relation`,`vm_stats_by_hour`.`commodity_key` AS `commodity_key` from (`vm_group_members` join `vm_stats_by_hour`) where (`vm_group_members`.`member_uuid` = `vm_stats_by_hour`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_stats_by_hour_per_vm_group_agg`
--

/*!50001 DROP TABLE IF EXISTS `vm_stats_by_hour_per_vm_group_agg`*/;
/*!50001 DROP VIEW IF EXISTS `vm_stats_by_hour_per_vm_group_agg`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_stats_by_hour_per_vm_group_agg` AS select `vm_group_members_agg`.`group_uuid` AS `group_uuid`,`vm_group_members_agg`.`internal_name` AS `internal_name`,`vm_group_members_agg`.`group_name` AS `group_name`,`vm_group_members_agg`.`group_type` AS `group_type`,`vm_group_members_agg`.`member_uuid` AS `member_uuid`,`vm_group_members_agg`.`display_name` AS `display_name`,`vm_stats_by_hour`.`snapshot_time` AS `snapshot_time`,`vm_stats_by_hour`.`uuid` AS `uuid`,`vm_stats_by_hour`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_hour`.`property_type` AS `property_type`,`vm_stats_by_hour`.`property_subtype` AS `property_subtype`,`vm_stats_by_hour`.`capacity` AS `capacity`,`vm_stats_by_hour`.`avg_value` AS `avg_value`,`vm_stats_by_hour`.`min_value` AS `min_value`,`vm_stats_by_hour`.`max_value` AS `max_value`,`vm_stats_by_hour`.`relation` AS `relation`,`vm_stats_by_hour`.`commodity_key` AS `commodity_key` from (`vm_group_members_agg` join `vm_stats_by_hour`) where (`vm_group_members_agg`.`member_uuid` = `vm_stats_by_hour`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_storage_used_by_day_per_vm_group`
--

/*!50001 DROP TABLE IF EXISTS `vm_storage_used_by_day_per_vm_group`*/;
/*!50001 DROP VIEW IF EXISTS `vm_storage_used_by_day_per_vm_group`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_storage_used_by_day_per_vm_group` AS select `vm_stats_by_day_per_vm_group_agg`.`group_uuid` AS `group_uuid`,`vm_stats_by_day_per_vm_group_agg`.`group_name` AS `group_name`,`vm_stats_by_day_per_vm_group_agg`.`group_type` AS `group_type`,cast(`vm_stats_by_day_per_vm_group_agg`.`snapshot_time` as date) AS `recorded_on`,`vm_stats_by_day_per_vm_group_agg`.`property_type` AS `property_type`,`vm_stats_by_day_per_vm_group_agg`.`property_subtype` AS `property_subtype`,round(sum(`vm_stats_by_day_per_vm_group_agg`.`avg_value`),0) AS `storage_used` from `vm_stats_by_day_per_vm_group_agg` where ((`vm_stats_by_day_per_vm_group_agg`.`property_type` = 'StorageAmount') and (`vm_stats_by_day_per_vm_group_agg`.`property_subtype` = 'used')) group by `vm_stats_by_day_per_vm_group_agg`.`group_name`,cast(`vm_stats_by_day_per_vm_group_agg`.`snapshot_time` as date) order by `vm_stats_by_day_per_vm_group_agg`.`group_name`,cast(`vm_stats_by_day_per_vm_group_agg`.`snapshot_time` as date) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_util_info_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `vm_util_info_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `vm_util_info_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_util_info_yesterday` AS select `vm_util_stats_yesterday`.`snapshot_time` AS `snapshot_time`,`vm_instances`.`display_name` AS `display_name`,`vm_instances`.`uuid` AS `uuid`,`vm_util_stats_yesterday`.`producer_uuid` AS `producer_uuid`,`vm_util_stats_yesterday`.`property_type` AS `property_type`,`vm_util_stats_yesterday`.`property_subtype` AS `property_subtype`,`vm_util_stats_yesterday`.`capacity` AS `capacity`,`vm_util_stats_yesterday`.`avg_value` AS `avg_value`,`vm_util_stats_yesterday`.`min_value` AS `min_value`,`vm_util_stats_yesterday`.`max_value` AS `max_value` from (`vm_util_stats_yesterday` join `vm_instances`) where (`vm_util_stats_yesterday`.`uuid` = `vm_instances`.`uuid`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vm_util_stats_yesterday`
--

/*!50001 DROP TABLE IF EXISTS `vm_util_stats_yesterday`*/;
/*!50001 DROP VIEW IF EXISTS `vm_util_stats_yesterday`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vm_util_stats_yesterday` AS select `vm_stats_by_day`.`snapshot_time` AS `snapshot_time`,`vm_stats_by_day`.`uuid` AS `uuid`,`vm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_day`.`property_type` AS `property_type`,`vm_stats_by_day`.`property_subtype` AS `property_subtype`,`vm_stats_by_day`.`capacity` AS `capacity`,`vm_stats_by_day`.`avg_value` AS `avg_value`,`vm_stats_by_day`.`min_value` AS `min_value`,`vm_stats_by_day`.`max_value` AS `max_value`,`vm_stats_by_day`.`relation` AS `relation`,`vm_stats_by_day`.`commodity_key` AS `commodity_key` from `vm_stats_by_day` where ((cast(`vm_stats_by_day`.`snapshot_time` as date) = (cast(now() as date) - interval 1 day)) and (`vm_stats_by_day`.`property_subtype` = 'utilization')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vpod_daily_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vpod_daily_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vpod_daily_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vpod_daily_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vpod_stats_by_hour` `a` left join `vpod_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vpod_daily_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vpod_daily_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vpod_daily_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vpod_daily_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vpod_stats_by_hour` `a` left join `vpod_stats_by_day` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vpod_hourly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vpod_hourly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vpod_hourly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vpod_hourly_ins_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`vpod_stats_latest` `a` left join `vpod_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vpod_hourly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vpod_hourly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vpod_hourly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vpod_hourly_upd_vw` AS select date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,count(0) AS `samples`,count(0) AS `new_samples` from (`vpod_stats_latest` `a` left join `vpod_stats_by_hour` `b` on(((date_format(`a`.`snapshot_time`,'%Y-%m-%d %H:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vpod_monthly_ins_vw`
--

/*!50001 DROP TABLE IF EXISTS `vpod_monthly_ins_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vpod_monthly_ins_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vpod_monthly_ins_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vpod_stats_by_day` `a` left join `vpod_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where (isnull(`b`.`snapshot_time`) and isnull(`b`.`uuid`) and isnull(`b`.`producer_uuid`) and isnull(`b`.`property_type`) and isnull(`b`.`property_subtype`) and isnull(`b`.`relation`) and isnull(`b`.`commodity_key`) and (`a`.`aggregated` = 2)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `vpod_monthly_upd_vw`
--

/*!50001 DROP TABLE IF EXISTS `vpod_monthly_upd_vw`*/;
/*!50001 DROP VIEW IF EXISTS `vpod_monthly_upd_vw`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8 */;
/*!50001 SET character_set_results     = utf8 */;
/*!50001 SET collation_connection      = utf8_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
  /*!50013 DEFINER=CURRENT_USER SQL SECURITY DEFINER */
  /*!50001 VIEW `vpod_monthly_upd_vw` AS select date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d %H:00:00') AS `snapshot_time`,`a`.`uuid` AS `uuid`,`a`.`producer_uuid` AS `producer_uuid`,`a`.`property_type` AS `property_type`,`a`.`property_subtype` AS `property_subtype`,`a`.`relation` AS `relation`,`a`.`commodity_key` AS `commodity_key`,max(`a`.`capacity`) AS `capacity`,min(`a`.`min_value`) AS `min_value`,max(`a`.`max_value`) AS `max_value`,avg(`a`.`avg_value`) AS `avg_value`,sum(`a`.`samples`) AS `samples`,sum(`a`.`new_samples`) AS `new_samples` from (`vpod_stats_by_day` `a` left join `vpod_stats_by_month` `b` on(((date_format(last_day(`a`.`snapshot_time`),'%Y-%m-%d 00:00:00') <=> `b`.`snapshot_time`) and (`a`.`uuid` <=> `b`.`uuid`) and (`a`.`producer_uuid` <=> `b`.`producer_uuid`) and (`a`.`property_type` <=> `b`.`property_type`) and (`a`.`property_subtype` <=> `b`.`property_subtype`) and (`a`.`relation` <=> `b`.`relation`) and (`a`.`commodity_key` <=> `b`.`commodity_key`)))) where ((`b`.`snapshot_time` is not null) and (`b`.`uuid` is not null) and (`b`.`producer_uuid` is not null) and (`b`.`property_type` is not null) and (`b`.`property_subtype` is not null) and (`b`.`relation` is not null) and (`b`.`commodity_key` is not null) and (`a`.`aggregated` = 3)) group by 1,2,3,4,5,6,7 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2018-03-09 12:35:46

-- OM-32915 Create utility views to support legacy reporting in XL

-- Utilization is missing in the historical stats tables
-- The following views mimic utilization stats by calculating utilization based on used / capacity
-- The BiRT report templates which execute utilization queries have been changed to reference the following utilization views

create view vm_stats_by_day_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                      capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                           (max_value/capacity)*100 as max_value, relation, commodity_key from vm_stats_by_day where property_subtype='used';

create view pm_stats_by_day_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                      capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                           (max_value/capacity)*100 as max_value, relation, commodity_key from pm_stats_by_day where property_subtype='used';

create view ds_stats_by_day_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                      capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                           (max_value/capacity)*100 as max_value, relation, commodity_key from ds_stats_by_day where property_subtype='used';

create view vm_stats_by_hour_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                       capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                            (max_value/capacity)*100 as max_value, relation, commodity_key from vm_stats_by_hour where property_subtype='used';

create view pm_stats_by_hour_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                       capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                            (max_value/capacity)*100 as max_value, relation, commodity_key from pm_stats_by_hour where property_subtype='used';

create view vm_stats_by_month_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                        capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                             (max_value/capacity)*100 as max_value, relation, commodity_key from vm_stats_by_month where property_subtype='used';

CREATE VIEW pm_stats_by_month_util as select snapshot_time, uuid,producer_uuid,property_type,'utilization' as property_subtype,
                                        capacity, (avg_value/capacity)*100 as avg_value, (min_value/capacity)*100 as min_value,
                                                                                             (max_value/capacity)*100 as max_value, relation, commodity_key from pm_stats_by_month where property_subtype='used';


-- Capacity views based on historical stats utilization are missing in XL
-- The following views mimic capacity views by querying the utilization views
-- The BiRT report templates which execute capacity queries based on utilization have been changed to reference the following capacity views

CREATE VIEW `vm_capacity_by_day_util` AS select 'VirtualMachine' AS `class_name`,`vm_stats_by_day_util`.`uuid` AS `uuid`,`vm_stats_by_day_util`.`producer_uuid` AS `producer_uuid`,
                                                `vm_stats_by_day_util`.`property_type` AS `property_type`,`vm_stats_by_day_util`.`avg_value` AS `utilization`,`vm_stats_by_day_util`.`capacity` AS `capacity`,
                                                round((`vm_stats_by_day_util`.`avg_value` * `vm_stats_by_day_util`.`capacity`),0) AS `used_capacity`,
                                                round(((1.0 - `vm_stats_by_day_util`.`avg_value`) * `vm_stats_by_day_util`.`capacity`),0) AS `available_capacity`,cast(`vm_stats_by_day_util`.`snapshot_time` as date) AS `recorded_on`
                                         from (`vm_stats_by_day_util` join `vm_instances`) where ((`vm_stats_by_day_util`.`uuid` = `vm_instances`.`uuid`) and (`vm_stats_by_day_util`.`property_subtype` = 'utilization')
                                                                                                  and (`vm_stats_by_day_util`.`capacity` > 0.00));

CREATE VIEW `pm_capacity_by_day_util` AS select 'PhysicalMachine' AS `class_name`,`pm_stats_by_day_util`.`uuid` AS `uuid`,`pm_stats_by_day_util`.`producer_uuid` AS `producer_uuid`,
                                                `pm_stats_by_day_util`.`property_type` AS `property_type`,`pm_stats_by_day_util`.`avg_value` AS `utilization`,`pm_stats_by_day_util`.`capacity` AS `capacity`,
                                                round((`pm_stats_by_day_util`.`avg_value` * `pm_stats_by_day_util`.`capacity`),0) AS `used_capacity`,
                                                round(((1.0 - `pm_stats_by_day_util`.`avg_value`) * `pm_stats_by_day_util`.`capacity`),0) AS `available_capacity`,cast(`pm_stats_by_day_util`.`snapshot_time` as date) AS `recorded_on`
                                         from (`pm_stats_by_day_util` join `pm_instances`) where ((`pm_stats_by_day_util`.`uuid` = `pm_instances`.`uuid`) and (`pm_stats_by_day_util`.`property_subtype` = 'utilization')
                                                                                                  and (`pm_stats_by_day_util`.`capacity` > 0.00));

CREATE VIEW `ds_capacity_by_day_util` AS select 'Storage' AS `class_name`,`ds_stats_by_day_util`.`uuid` AS `uuid`,`ds_stats_by_day_util`.`producer_uuid` AS `producer_uuid`,
                                                `ds_stats_by_day_util`.`property_type` AS `property_type`,`ds_stats_by_day_util`.`avg_value` AS `utilization`,`ds_stats_by_day_util`.`capacity` AS `capacity`,
                                                round((`ds_stats_by_day_util`.`avg_value` * `ds_stats_by_day_util`.`capacity`),0) AS `used_capacity`,
                                                round(((1.0 - `ds_stats_by_day_util`.`avg_value`) * `ds_stats_by_day_util`.`capacity`),0) AS `available_capacity`,cast(`ds_stats_by_day_util`.`snapshot_time` as date) AS `recorded_on`
                                         from (`ds_stats_by_day_util` join `ds_instances`) where ((`ds_stats_by_day_util`.`uuid` = `ds_instances`.`uuid`) and (`ds_stats_by_day_util`.`property_subtype` = 'utilization')
                                                                                                  and (`ds_stats_by_day_util`.`capacity` > 0.00));
