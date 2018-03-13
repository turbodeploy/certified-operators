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
PARTITION before20180110071308 VALUES LESS THAN (63682787588) ENGINE = InnoDB,
PARTITION before20180111210108 VALUES LESS THAN (63682923668) ENGINE = InnoDB,
PARTITION before20180113104908 VALUES LESS THAN (63683059748) ENGINE = InnoDB,
PARTITION before20180115003708 VALUES LESS THAN (63683195828) ENGINE = InnoDB,
PARTITION before20180116142508 VALUES LESS THAN (63683331908) ENGINE = InnoDB,
PARTITION before20180118041308 VALUES LESS THAN (63683467988) ENGINE = InnoDB,
PARTITION before20180119180108 VALUES LESS THAN (63683604068) ENGINE = InnoDB,
PARTITION before20180121074908 VALUES LESS THAN (63683740148) ENGINE = InnoDB,
PARTITION before20180122213708 VALUES LESS THAN (63683876228) ENGINE = InnoDB,
PARTITION before20180124112508 VALUES LESS THAN (63684012308) ENGINE = InnoDB,
PARTITION before20180126011308 VALUES LESS THAN (63684148388) ENGINE = InnoDB,
PARTITION before20180127150108 VALUES LESS THAN (63684284468) ENGINE = InnoDB,
PARTITION before20180129044908 VALUES LESS THAN (63684420548) ENGINE = InnoDB,
PARTITION before20180130183708 VALUES LESS THAN (63684556628) ENGINE = InnoDB,
PARTITION before20180201082508 VALUES LESS THAN (63684692708) ENGINE = InnoDB,
PARTITION before20180202221308 VALUES LESS THAN (63684828788) ENGINE = InnoDB,
PARTITION before20180204120108 VALUES LESS THAN (63684964868) ENGINE = InnoDB,
PARTITION before20180206014908 VALUES LESS THAN (63685100948) ENGINE = InnoDB,
PARTITION before20180207153708 VALUES LESS THAN (63685237028) ENGINE = InnoDB,
PARTITION before20180209052508 VALUES LESS THAN (63685373108) ENGINE = InnoDB,
PARTITION before20180210191308 VALUES LESS THAN (63685509188) ENGINE = InnoDB,
PARTITION before20180212090108 VALUES LESS THAN (63685645268) ENGINE = InnoDB,
PARTITION before20180213224908 VALUES LESS THAN (63685781348) ENGINE = InnoDB,
PARTITION before20180215123708 VALUES LESS THAN (63685917428) ENGINE = InnoDB,
PARTITION before20180217022508 VALUES LESS THAN (63686053508) ENGINE = InnoDB,
PARTITION before20180218161308 VALUES LESS THAN (63686189588) ENGINE = InnoDB,
PARTITION before20180220060108 VALUES LESS THAN (63686325668) ENGINE = InnoDB,
PARTITION before20180221194908 VALUES LESS THAN (63686461748) ENGINE = InnoDB,
PARTITION before20180223093708 VALUES LESS THAN (63686597828) ENGINE = InnoDB,
PARTITION before20180224232508 VALUES LESS THAN (63686733908) ENGINE = InnoDB,
PARTITION before20180226131308 VALUES LESS THAN (63686869988) ENGINE = InnoDB,
PARTITION before20180228030108 VALUES LESS THAN (63687006068) ENGINE = InnoDB,
PARTITION before20180301164908 VALUES LESS THAN (63687142148) ENGINE = InnoDB,
PARTITION before20180303063708 VALUES LESS THAN (63687278228) ENGINE = InnoDB,
PARTITION before20180304202508 VALUES LESS THAN (63687414308) ENGINE = InnoDB,
PARTITION before20180306101308 VALUES LESS THAN (63687550388) ENGINE = InnoDB,
PARTITION before20180308000108 VALUES LESS THAN (63687686468) ENGINE = InnoDB,
PARTITION before20180309134908 VALUES LESS THAN (63687822548) ENGINE = InnoDB,
PARTITION before20180311033708 VALUES LESS THAN (63687958628) ENGINE = InnoDB,
PARTITION before20180312172508 VALUES LESS THAN (63688094708) ENGINE = InnoDB,
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
PARTITION before20180306192509 VALUES LESS THAN (63687583509) ENGINE = InnoDB,
PARTITION before20180306212509 VALUES LESS THAN (63687590709) ENGINE = InnoDB,
PARTITION before20180306232509 VALUES LESS THAN (63687597909) ENGINE = InnoDB,
PARTITION before20180307012509 VALUES LESS THAN (63687605109) ENGINE = InnoDB,
PARTITION before20180307032509 VALUES LESS THAN (63687612309) ENGINE = InnoDB,
PARTITION before20180307052509 VALUES LESS THAN (63687619509) ENGINE = InnoDB,
PARTITION before20180307072509 VALUES LESS THAN (63687626709) ENGINE = InnoDB,
PARTITION before20180307092509 VALUES LESS THAN (63687633909) ENGINE = InnoDB,
PARTITION before20180307112509 VALUES LESS THAN (63687641109) ENGINE = InnoDB,
PARTITION before20180307132509 VALUES LESS THAN (63687648309) ENGINE = InnoDB,
PARTITION before20180307152509 VALUES LESS THAN (63687655509) ENGINE = InnoDB,
PARTITION before20180307172509 VALUES LESS THAN (63687662709) ENGINE = InnoDB,
PARTITION before20180307192509 VALUES LESS THAN (63687669909) ENGINE = InnoDB,
PARTITION before20180307212509 VALUES LESS THAN (63687677109) ENGINE = InnoDB,
PARTITION before20180307232509 VALUES LESS THAN (63687684309) ENGINE = InnoDB,
PARTITION before20180308012509 VALUES LESS THAN (63687691509) ENGINE = InnoDB,
PARTITION before20180308032509 VALUES LESS THAN (63687698709) ENGINE = InnoDB,
PARTITION before20180308052509 VALUES LESS THAN (63687705909) ENGINE = InnoDB,
PARTITION before20180308072509 VALUES LESS THAN (63687713109) ENGINE = InnoDB,
PARTITION before20180308092509 VALUES LESS THAN (63687720309) ENGINE = InnoDB,
PARTITION before20180308112509 VALUES LESS THAN (63687727509) ENGINE = InnoDB,
PARTITION before20180308132509 VALUES LESS THAN (63687734709) ENGINE = InnoDB,
PARTITION before20180308152509 VALUES LESS THAN (63687741909) ENGINE = InnoDB,
PARTITION before20180308172509 VALUES LESS THAN (63687749109) ENGINE = InnoDB,
PARTITION before20180308192509 VALUES LESS THAN (63687756309) ENGINE = InnoDB,
PARTITION before20180308212509 VALUES LESS THAN (63687763509) ENGINE = InnoDB,
PARTITION before20180308232509 VALUES LESS THAN (63687770709) ENGINE = InnoDB,
PARTITION before20180309012509 VALUES LESS THAN (63687777909) ENGINE = InnoDB,
PARTITION before20180309032509 VALUES LESS THAN (63687785109) ENGINE = InnoDB,
PARTITION before20180309052509 VALUES LESS THAN (63687792309) ENGINE = InnoDB,
PARTITION before20180309072509 VALUES LESS THAN (63687799509) ENGINE = InnoDB,
PARTITION before20180309092509 VALUES LESS THAN (63687806709) ENGINE = InnoDB,
PARTITION before20180309112509 VALUES LESS THAN (63687813909) ENGINE = InnoDB,
PARTITION before20180309132509 VALUES LESS THAN (63687821109) ENGINE = InnoDB,
PARTITION before20180309152509 VALUES LESS THAN (63687828309) ENGINE = InnoDB,
PARTITION before20180309172509 VALUES LESS THAN (63687835509) ENGINE = InnoDB,
PARTITION before20180309192509 VALUES LESS THAN (63687842709) ENGINE = InnoDB,
PARTITION before20180309212509 VALUES LESS THAN (63687849909) ENGINE = InnoDB,
PARTITION before20180309232509 VALUES LESS THAN (63687857109) ENGINE = InnoDB,
PARTITION before20180310012509 VALUES LESS THAN (63687864309) ENGINE = InnoDB,
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
PARTITION before20160314093710 VALUES LESS THAN (63625167430) ENGINE = InnoDB,
PARTITION before20160402014910 VALUES LESS THAN (63626780950) ENGINE = InnoDB,
PARTITION before20160420180110 VALUES LESS THAN (63628394470) ENGINE = InnoDB,
PARTITION before20160509101310 VALUES LESS THAN (63630007990) ENGINE = InnoDB,
PARTITION before20160528022510 VALUES LESS THAN (63631621510) ENGINE = InnoDB,
PARTITION before20160615183710 VALUES LESS THAN (63633235030) ENGINE = InnoDB,
PARTITION before20160704104910 VALUES LESS THAN (63634848550) ENGINE = InnoDB,
PARTITION before20160723030110 VALUES LESS THAN (63636462070) ENGINE = InnoDB,
PARTITION before20160810191310 VALUES LESS THAN (63638075590) ENGINE = InnoDB,
PARTITION before20160829112510 VALUES LESS THAN (63639689110) ENGINE = InnoDB,
PARTITION before20160917033710 VALUES LESS THAN (63641302630) ENGINE = InnoDB,
PARTITION before20161005194910 VALUES LESS THAN (63642916150) ENGINE = InnoDB,
PARTITION before20161024120110 VALUES LESS THAN (63644529670) ENGINE = InnoDB,
PARTITION before20161112041310 VALUES LESS THAN (63646143190) ENGINE = InnoDB,
PARTITION before20161130202510 VALUES LESS THAN (63647756710) ENGINE = InnoDB,
PARTITION before20161219123710 VALUES LESS THAN (63649370230) ENGINE = InnoDB,
PARTITION before20170107044910 VALUES LESS THAN (63650983750) ENGINE = InnoDB,
PARTITION before20170125210110 VALUES LESS THAN (63652597270) ENGINE = InnoDB,
PARTITION before20170213131310 VALUES LESS THAN (63654210790) ENGINE = InnoDB,
PARTITION before20170304052510 VALUES LESS THAN (63655824310) ENGINE = InnoDB,
PARTITION before20170322213710 VALUES LESS THAN (63657437830) ENGINE = InnoDB,
PARTITION before20170410134910 VALUES LESS THAN (63659051350) ENGINE = InnoDB,
PARTITION before20170429060110 VALUES LESS THAN (63660664870) ENGINE = InnoDB,
PARTITION before20170517221310 VALUES LESS THAN (63662278390) ENGINE = InnoDB,
PARTITION before20170605142510 VALUES LESS THAN (63663891910) ENGINE = InnoDB,
PARTITION before20170624063710 VALUES LESS THAN (63665505430) ENGINE = InnoDB,
PARTITION before20170712224910 VALUES LESS THAN (63667118950) ENGINE = InnoDB,
PARTITION before20170731150110 VALUES LESS THAN (63668732470) ENGINE = InnoDB,
PARTITION before20170819071310 VALUES LESS THAN (63670345990) ENGINE = InnoDB,
PARTITION before20170906232510 VALUES LESS THAN (63671959510) ENGINE = InnoDB,
PARTITION before20170925153710 VALUES LESS THAN (63673573030) ENGINE = InnoDB,
PARTITION before20171014074910 VALUES LESS THAN (63675186550) ENGINE = InnoDB,
PARTITION before20171102000110 VALUES LESS THAN (63676800070) ENGINE = InnoDB,
PARTITION before20171120161310 VALUES LESS THAN (63678413590) ENGINE = InnoDB,
PARTITION before20171209082510 VALUES LESS THAN (63680027110) ENGINE = InnoDB,
PARTITION before20171228003710 VALUES LESS THAN (63681640630) ENGINE = InnoDB,
PARTITION before20180115164910 VALUES LESS THAN (63683254150) ENGINE = InnoDB,
PARTITION before20180203090110 VALUES LESS THAN (63684867670) ENGINE = InnoDB,
PARTITION before20180222011310 VALUES LESS THAN (63686481190) ENGINE = InnoDB,
PARTITION before20180312172510 VALUES LESS THAN (63688094710) ENGINE = InnoDB,
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
PARTITION before20180309153707 VALUES LESS THAN (63687829027) ENGINE = InnoDB,
PARTITION before20180309154307 VALUES LESS THAN (63687829387) ENGINE = InnoDB,
PARTITION before20180309154907 VALUES LESS THAN (63687829747) ENGINE = InnoDB,
PARTITION before20180309155507 VALUES LESS THAN (63687830107) ENGINE = InnoDB,
PARTITION before20180309160107 VALUES LESS THAN (63687830467) ENGINE = InnoDB,
PARTITION before20180309160707 VALUES LESS THAN (63687830827) ENGINE = InnoDB,
PARTITION before20180309161307 VALUES LESS THAN (63687831187) ENGINE = InnoDB,
PARTITION before20180309161907 VALUES LESS THAN (63687831547) ENGINE = InnoDB,
PARTITION before20180309162507 VALUES LESS THAN (63687831907) ENGINE = InnoDB,
PARTITION before20180309163107 VALUES LESS THAN (63687832267) ENGINE = InnoDB,
PARTITION before20180309163707 VALUES LESS THAN (63687832627) ENGINE = InnoDB,
PARTITION before20180309164307 VALUES LESS THAN (63687832987) ENGINE = InnoDB,
PARTITION before20180309164907 VALUES LESS THAN (63687833347) ENGINE = InnoDB,
PARTITION before20180309165507 VALUES LESS THAN (63687833707) ENGINE = InnoDB,
PARTITION before20180309170107 VALUES LESS THAN (63687834067) ENGINE = InnoDB,
PARTITION before20180309170707 VALUES LESS THAN (63687834427) ENGINE = InnoDB,
PARTITION before20180309171307 VALUES LESS THAN (63687834787) ENGINE = InnoDB,
PARTITION before20180309171907 VALUES LESS THAN (63687835147) ENGINE = InnoDB,
PARTITION before20180309172507 VALUES LESS THAN (63687835507) ENGINE = InnoDB,
PARTITION before20180309173107 VALUES LESS THAN (63687835867) ENGINE = InnoDB,
PARTITION before20180309173707 VALUES LESS THAN (63687836227) ENGINE = InnoDB,
PARTITION before20180309174307 VALUES LESS THAN (63687836587) ENGINE = InnoDB,
PARTITION before20180309174907 VALUES LESS THAN (63687836947) ENGINE = InnoDB,
PARTITION before20180309175507 VALUES LESS THAN (63687837307) ENGINE = InnoDB,
PARTITION before20180309180107 VALUES LESS THAN (63687837667) ENGINE = InnoDB,
PARTITION before20180309180707 VALUES LESS THAN (63687838027) ENGINE = InnoDB,
PARTITION before20180309181307 VALUES LESS THAN (63687838387) ENGINE = InnoDB,
PARTITION before20180309181907 VALUES LESS THAN (63687838747) ENGINE = InnoDB,
PARTITION before20180309182507 VALUES LESS THAN (63687839107) ENGINE = InnoDB,
PARTITION before20180309183107 VALUES LESS THAN (63687839467) ENGINE = InnoDB,
PARTITION before20180309183707 VALUES LESS THAN (63687839827) ENGINE = InnoDB,
PARTITION before20180309184307 VALUES LESS THAN (63687840187) ENGINE = InnoDB,
PARTITION before20180309184907 VALUES LESS THAN (63687840547) ENGINE = InnoDB,
PARTITION before20180309185507 VALUES LESS THAN (63687840907) ENGINE = InnoDB,
PARTITION before20180309190107 VALUES LESS THAN (63687841267) ENGINE = InnoDB,
PARTITION before20180309190707 VALUES LESS THAN (63687841627) ENGINE = InnoDB,
PARTITION before20180309191307 VALUES LESS THAN (63687841987) ENGINE = InnoDB,
PARTITION before20180309191907 VALUES LESS THAN (63687842347) ENGINE = InnoDB,
PARTITION before20180309192507 VALUES LESS THAN (63687842707) ENGINE = InnoDB,
PARTITION before20180309193107 VALUES LESS THAN (63687843067) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_app_primary_keys BEFORE INSERT ON app_stats_latest
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
-- Dumping data for table `appl_performance`
--

LOCK TABLES `appl_performance` WRITE;
/*!40000 ALTER TABLE `appl_performance` DISABLE KEYS */;
INSERT INTO `appl_performance` VALUES ('2018-03-09 17:25:04','3f0a76a5a7fe9255d309516306fbfe1a','REPARTITION','market_stats_latest',0,'2018-03-09 17:25:02','2018-03-09 17:25:04',2),('2018-03-09 17:25:05','f834dd03876163f2da4451ee68b24e2c','REPARTITION','market_stats_by_day',0,'2018-03-09 17:25:04','2018-03-09 17:25:05',1),('2018-03-09 17:25:06','5ab60dcb4be723c954e2acc238c5c53c','REPARTITION','market_stats_by_hour',0,'2018-03-09 17:25:05','2018-03-09 17:25:06',1),('2018-03-09 17:25:07','467e6736e8ae961b352797ee91b688a2','REPARTITION','market_stats_by_month',0,'2018-03-09 17:25:06','2018-03-09 17:25:07',1),('2018-03-09 17:25:08','b42836b08811ee1c96cd909c7c2aa354','REPARTITION','app_stats_latest',0,'2018-03-09 17:25:07','2018-03-09 17:25:08',1),('2018-03-09 17:25:09','8c0116066abd857d8ecafe13f6d2cfbb','REPARTITION','app_stats_by_day',0,'2018-03-09 17:25:08','2018-03-09 17:25:09',1),('2018-03-09 17:25:10','1bc1be43629e06c0ccfa5cac5427521e','REPARTITION','app_stats_by_hour',0,'2018-03-09 17:25:09','2018-03-09 17:25:10',1),('2018-03-09 17:25:11','ac790776410160e5c00506bae620c235','REPARTITION','app_stats_by_month',0,'2018-03-09 17:25:10','2018-03-09 17:25:11',1),('2018-03-09 17:25:12','203cfec68debb03573ae2b7d7f6a0070','REPARTITION','ch_stats_latest',0,'2018-03-09 17:25:11','2018-03-09 17:25:12',1),('2018-03-09 17:25:12','aff8ca7a415789e4c63dc6d323a80992','REPARTITION','ch_stats_by_day',0,'2018-03-09 17:25:12','2018-03-09 17:25:12',0),('2018-03-09 17:25:13','aff8ca7a415789e4c63dc6d323a80992','REPARTITION','ch_stats_by_hour',0,'2018-03-09 17:25:12','2018-03-09 17:25:13',1),('2018-03-09 17:25:14','c47c4c0eb883ecbd613ddccc5c6f4168','REPARTITION','ch_stats_by_month',0,'2018-03-09 17:25:13','2018-03-09 17:25:14',1),('2018-03-09 17:25:15','a7708b825ff28ed6d618c90960414031','REPARTITION','cnt_stats_latest',0,'2018-03-09 17:25:14','2018-03-09 17:25:15',1),('2018-03-09 17:25:16','3ecd768ab498b7563b7f3da5ae662e98','REPARTITION','cnt_stats_by_day',0,'2018-03-09 17:25:15','2018-03-09 17:25:16',1),('2018-03-09 17:25:16','947c0033f10a1d8c1122280ecb051bb0','REPARTITION','cnt_stats_by_hour',0,'2018-03-09 17:25:16','2018-03-09 17:25:16',0),('2018-03-09 17:25:17','947c0033f10a1d8c1122280ecb051bb0','REPARTITION','cnt_stats_by_month',0,'2018-03-09 17:25:16','2018-03-09 17:25:17',1),('2018-03-09 17:25:18','7f67db18a2b5e241796cb32a8f20a980','REPARTITION','dpod_stats_latest',0,'2018-03-09 17:25:17','2018-03-09 17:25:18',1),('2018-03-09 17:25:19','b87559e658da6f25652240f208bc4f31','REPARTITION','dpod_stats_by_day',0,'2018-03-09 17:25:18','2018-03-09 17:25:19',1),('2018-03-09 17:25:19','b3f3d7ba9a0c04f04d7d9fbee9db6c28','REPARTITION','dpod_stats_by_hour',0,'2018-03-09 17:25:19','2018-03-09 17:25:19',0),('2018-03-09 17:25:20','b3f3d7ba9a0c04f04d7d9fbee9db6c28','REPARTITION','dpod_stats_by_month',0,'2018-03-09 17:25:19','2018-03-09 17:25:20',1),('2018-03-09 17:25:21','2b8b8603498691b8c4b5c115d940f0a2','REPARTITION','ds_stats_latest',0,'2018-03-09 17:25:20','2018-03-09 17:25:21',1),('2018-03-09 17:25:22','f5663b08643a70be1d84b99e5416836d','REPARTITION','ds_stats_by_day',0,'2018-03-09 17:25:21','2018-03-09 17:25:22',1),('2018-03-09 17:25:23','6c6b25ccb8683d4cc3b7aa11b45c5278','REPARTITION','ds_stats_by_hour',0,'2018-03-09 17:25:22','2018-03-09 17:25:23',1),('2018-03-09 17:25:24','6695a9c3128afea832f2477fd45d9d63','REPARTITION','ds_stats_by_month',0,'2018-03-09 17:25:23','2018-03-09 17:25:24',1),('2018-03-09 17:25:24','fab825f7cbd404bc19fb2e1461f1189f','REPARTITION','iom_stats_latest',0,'2018-03-09 17:25:24','2018-03-09 17:25:24',0),('2018-03-09 17:25:25','fab825f7cbd404bc19fb2e1461f1189f','REPARTITION','iom_stats_by_day',0,'2018-03-09 17:25:24','2018-03-09 17:25:25',1),('2018-03-09 17:25:26','c86b388ce2a4d30c05dfb2a205128bcb','REPARTITION','iom_stats_by_hour',0,'2018-03-09 17:25:25','2018-03-09 17:25:26',1),('2018-03-09 17:25:27','9bf496ae61d85aaff18dcae831eee1b6','REPARTITION','iom_stats_by_month',0,'2018-03-09 17:25:26','2018-03-09 17:25:27',1),('2018-03-09 17:25:28','be790736a8e7512bca2607c5cd26a148','REPARTITION','pm_stats_latest',0,'2018-03-09 17:25:27','2018-03-09 17:25:28',1),('2018-03-09 17:25:29','773bd35135175bb54ceb76a3c18d8dbf','REPARTITION','pm_stats_by_day',0,'2018-03-09 17:25:28','2018-03-09 17:25:29',1),('2018-03-09 17:25:30','c2d600b34ff947f348e55d5a174b902e','REPARTITION','pm_stats_by_hour',0,'2018-03-09 17:25:29','2018-03-09 17:25:30',1),('2018-03-09 17:25:31','e30171cb731b333a32e5002b969edc5f','REPARTITION','pm_stats_by_month',0,'2018-03-09 17:25:30','2018-03-09 17:25:31',1),('2018-03-09 17:25:32','66015ccf82e803bb8e2f4cc2f0a5fb33','REPARTITION','sc_stats_latest',0,'2018-03-09 17:25:31','2018-03-09 17:25:32',1),('2018-03-09 17:25:33','8c46844f20177c8e3c0adf5e4417af70','REPARTITION','sc_stats_by_day',0,'2018-03-09 17:25:32','2018-03-09 17:25:33',1),('2018-03-09 17:25:33','a8425ea8d32f3d68e20b15c3a0d67a93','REPARTITION','sc_stats_by_hour',0,'2018-03-09 17:25:33','2018-03-09 17:25:33',0),('2018-03-09 17:25:34','a8425ea8d32f3d68e20b15c3a0d67a93','REPARTITION','sc_stats_by_month',0,'2018-03-09 17:25:33','2018-03-09 17:25:34',1),('2018-03-09 17:25:35','6806f9b5a716649a703d8ff4c7720d9a','REPARTITION','sw_stats_latest',0,'2018-03-09 17:25:34','2018-03-09 17:25:35',1),('2018-03-09 17:25:36','d5a5001828e87b8fbac61790f912dec9','REPARTITION','sw_stats_by_day',0,'2018-03-09 17:25:35','2018-03-09 17:25:36',1),('2018-03-09 17:25:37','64011716422c53e5cba1417e0f79e9f9','REPARTITION','sw_stats_by_hour',0,'2018-03-09 17:25:36','2018-03-09 17:25:37',1),('2018-03-09 17:25:38','6d52c7620409d4ddc2d23fce07d3f856','REPARTITION','sw_stats_by_month',0,'2018-03-09 17:25:37','2018-03-09 17:25:38',1),('2018-03-09 17:25:39','984dbe910ac8d962e305873db41320e6','REPARTITION','vdc_stats_latest',0,'2018-03-09 17:25:38','2018-03-09 17:25:39',1),('2018-03-09 17:25:40','9d335bbfdd28eee97edaf84a0a92e14b','REPARTITION','vdc_stats_by_day',0,'2018-03-09 17:25:39','2018-03-09 17:25:40',1),('2018-03-09 17:25:41','b9b40f2f4a0538702dec2444bd46825b','REPARTITION','vdc_stats_by_hour',0,'2018-03-09 17:25:40','2018-03-09 17:25:41',1),('2018-03-09 17:25:42','654271e6392ee293e1d828b8d7b10e9d','REPARTITION','vdc_stats_by_month',0,'2018-03-09 17:25:41','2018-03-09 17:25:42',1),('2018-03-09 17:25:43','87b949e4cb718fe0a6fcfdd66c9e2334','REPARTITION','vm_stats_latest',0,'2018-03-09 17:25:42','2018-03-09 17:25:43',1),('2018-03-09 17:25:45','fab7bd4fecf21082d8eb4e83642c6611','REPARTITION','vm_stats_by_day',0,'2018-03-09 17:25:43','2018-03-09 17:25:45',2),('2018-03-09 17:25:46','d2f8020eb564cabbf2f3b4d2918f742f','REPARTITION','vm_stats_by_hour',0,'2018-03-09 17:25:45','2018-03-09 17:25:46',1),('2018-03-09 17:25:48','9a017648c1feba74cbbdcbb10d916f4e','REPARTITION','vm_stats_by_month',0,'2018-03-09 17:25:46','2018-03-09 17:25:48',2),('2018-03-09 17:25:49','7e2416cd84fe0bb1764163a0f41418f1','REPARTITION','vpod_stats_latest',0,'2018-03-09 17:25:48','2018-03-09 17:25:49',1),('2018-03-09 17:25:50','85e8e96eceb21e39f68b7aacd2f927dd','REPARTITION','vpod_stats_by_day',0,'2018-03-09 17:25:49','2018-03-09 17:25:50',1),('2018-03-09 17:25:52','7d89f7a6fbd4e695deb2aee41c849941','REPARTITION','vpod_stats_by_hour',0,'2018-03-09 17:25:50','2018-03-09 17:25:52',2),('2018-03-09 17:25:53','79d45b1f94cefcb4d4f5072d39ad2d7f','REPARTITION','vpod_stats_by_month',0,'2018-03-09 17:25:52','2018-03-09 17:25:53',1),('2018-03-09 17:25:55','65d745f9c17c6f7d487d2865f11a5677','REPARTITION','market_stats_latest',0,'2018-03-09 17:25:55','2018-03-09 17:25:55',0),('2018-03-09 17:25:55','65d745f9c17c6f7d487d2865f11a5677','REPARTITION','market_stats_by_day',0,'2018-03-09 17:25:55','2018-03-09 17:25:55',0),('2018-03-09 17:25:55','65d745f9c17c6f7d487d2865f11a5677','REPARTITION','market_stats_by_hour',0,'2018-03-09 17:25:55','2018-03-09 17:25:55',0),('2018-03-09 17:25:56','65d745f9c17c6f7d487d2865f11a5677','REPARTITION','market_stats_by_month',0,'2018-03-09 17:25:55','2018-03-09 17:25:56',1),('2018-03-09 17:25:56','5d8ed0ce0bbb1e38a5fbb1a8d71f55d9','REPARTITION','app_stats_latest',0,'2018-03-09 17:25:56','2018-03-09 17:25:56',0),('2018-03-09 17:25:56','5d8ed0ce0bbb1e38a5fbb1a8d71f55d9','REPARTITION','app_stats_by_day',0,'2018-03-09 17:25:56','2018-03-09 17:25:56',0),('2018-03-09 17:25:56','5d8ed0ce0bbb1e38a5fbb1a8d71f55d9','REPARTITION','app_stats_by_hour',0,'2018-03-09 17:25:56','2018-03-09 17:25:56',0),('2018-03-09 17:25:57','5d8ed0ce0bbb1e38a5fbb1a8d71f55d9','REPARTITION','app_stats_by_month',0,'2018-03-09 17:25:56','2018-03-09 17:25:57',1),('2018-03-09 17:25:57','b0087bf6200355d836b4ddf22438f8c1','REPARTITION','ch_stats_latest',0,'2018-03-09 17:25:57','2018-03-09 17:25:57',0),('2018-03-09 17:25:57','b0087bf6200355d836b4ddf22438f8c1','REPARTITION','ch_stats_by_day',0,'2018-03-09 17:25:57','2018-03-09 17:25:57',0),('2018-03-09 17:25:57','b0087bf6200355d836b4ddf22438f8c1','REPARTITION','ch_stats_by_hour',0,'2018-03-09 17:25:57','2018-03-09 17:25:57',0),('2018-03-09 17:25:57','b0087bf6200355d836b4ddf22438f8c1','REPARTITION','ch_stats_by_month',0,'2018-03-09 17:25:57','2018-03-09 17:25:57',0),('2018-03-09 17:25:58','b0087bf6200355d836b4ddf22438f8c1','REPARTITION','cnt_stats_latest',0,'2018-03-09 17:25:57','2018-03-09 17:25:58',1),('2018-03-09 17:25:58','b78b11d1d10ac5fe0d25730099b205ef','REPARTITION','cnt_stats_by_day',0,'2018-03-09 17:25:58','2018-03-09 17:25:58',0),('2018-03-09 17:25:58','b78b11d1d10ac5fe0d25730099b205ef','REPARTITION','cnt_stats_by_hour',0,'2018-03-09 17:25:58','2018-03-09 17:25:58',0),('2018-03-09 17:25:58','b78b11d1d10ac5fe0d25730099b205ef','REPARTITION','cnt_stats_by_month',0,'2018-03-09 17:25:58','2018-03-09 17:25:58',0),('2018-03-09 17:25:59','b78b11d1d10ac5fe0d25730099b205ef','REPARTITION','dpod_stats_latest',0,'2018-03-09 17:25:58','2018-03-09 17:25:59',1),('2018-03-09 17:25:59','f59f51cb3cb923807d12696559d0cf76','REPARTITION','dpod_stats_by_day',0,'2018-03-09 17:25:59','2018-03-09 17:25:59',0),('2018-03-09 17:25:59','f59f51cb3cb923807d12696559d0cf76','REPARTITION','dpod_stats_by_hour',0,'2018-03-09 17:25:59','2018-03-09 17:25:59',0),('2018-03-09 17:25:59','f59f51cb3cb923807d12696559d0cf76','REPARTITION','dpod_stats_by_month',0,'2018-03-09 17:25:59','2018-03-09 17:25:59',0),('2018-03-09 17:26:00','f59f51cb3cb923807d12696559d0cf76','REPARTITION','ds_stats_latest',0,'2018-03-09 17:25:59','2018-03-09 17:26:00',1),('2018-03-09 17:26:00','0e139d6f515754af84d70b5e00b16e5e','REPARTITION','ds_stats_by_day',0,'2018-03-09 17:26:00','2018-03-09 17:26:00',0),('2018-03-09 17:26:00','0e139d6f515754af84d70b5e00b16e5e','REPARTITION','ds_stats_by_hour',0,'2018-03-09 17:26:00','2018-03-09 17:26:00',0),('2018-03-09 17:26:01','0e139d6f515754af84d70b5e00b16e5e','REPARTITION','ds_stats_by_month',0,'2018-03-09 17:26:00','2018-03-09 17:26:01',1),('2018-03-09 17:26:01','b9a1e11fd4cb081b049384881a42124d','REPARTITION','iom_stats_latest',0,'2018-03-09 17:26:01','2018-03-09 17:26:01',0),('2018-03-09 17:26:01','b9a1e11fd4cb081b049384881a42124d','REPARTITION','iom_stats_by_day',0,'2018-03-09 17:26:01','2018-03-09 17:26:01',0),('2018-03-09 17:26:01','b9a1e11fd4cb081b049384881a42124d','REPARTITION','iom_stats_by_hour',0,'2018-03-09 17:26:01','2018-03-09 17:26:01',0),('2018-03-09 17:26:02','b9a1e11fd4cb081b049384881a42124d','REPARTITION','iom_stats_by_month',0,'2018-03-09 17:26:01','2018-03-09 17:26:02',1),('2018-03-09 17:26:02','51470c74329c30ba9ef65290be262f32','REPARTITION','pm_stats_latest',0,'2018-03-09 17:26:02','2018-03-09 17:26:02',0),('2018-03-09 17:26:03','51470c74329c30ba9ef65290be262f32','REPARTITION','pm_stats_by_day',0,'2018-03-09 17:26:02','2018-03-09 17:26:03',1),('2018-03-09 17:26:03','6ae8ac89398213dbf9e60d9df21574bf','REPARTITION','pm_stats_by_hour',0,'2018-03-09 17:26:03','2018-03-09 17:26:03',0),('2018-03-09 17:26:03','6ae8ac89398213dbf9e60d9df21574bf','REPARTITION','pm_stats_by_month',0,'2018-03-09 17:26:03','2018-03-09 17:26:03',0),('2018-03-09 17:26:03','6ae8ac89398213dbf9e60d9df21574bf','REPARTITION','sc_stats_latest',0,'2018-03-09 17:26:03','2018-03-09 17:26:03',0),('2018-03-09 17:26:04','6ae8ac89398213dbf9e60d9df21574bf','REPARTITION','sc_stats_by_day',0,'2018-03-09 17:26:03','2018-03-09 17:26:04',1),('2018-03-09 17:26:04','07d56d6a7447df8071a0b554a3480627','REPARTITION','sc_stats_by_hour',0,'2018-03-09 17:26:04','2018-03-09 17:26:04',0),('2018-03-09 17:26:04','07d56d6a7447df8071a0b554a3480627','REPARTITION','sc_stats_by_month',0,'2018-03-09 17:26:04','2018-03-09 17:26:04',0),('2018-03-09 17:26:04','07d56d6a7447df8071a0b554a3480627','REPARTITION','sw_stats_latest',0,'2018-03-09 17:26:04','2018-03-09 17:26:04',0),('2018-03-09 17:26:04','07d56d6a7447df8071a0b554a3480627','REPARTITION','sw_stats_by_day',0,'2018-03-09 17:26:04','2018-03-09 17:26:04',0),('2018-03-09 17:26:04','07d56d6a7447df8071a0b554a3480627','REPARTITION','sw_stats_by_hour',0,'2018-03-09 17:26:04','2018-03-09 17:26:04',0),('2018-03-09 17:26:05','07d56d6a7447df8071a0b554a3480627','REPARTITION','sw_stats_by_month',0,'2018-03-09 17:26:04','2018-03-09 17:26:05',1),('2018-03-09 17:26:05','db89733d01719acfd7880582899caf75','REPARTITION','vdc_stats_latest',0,'2018-03-09 17:26:05','2018-03-09 17:26:05',0),('2018-03-09 17:26:05','db89733d01719acfd7880582899caf75','REPARTITION','vdc_stats_by_day',0,'2018-03-09 17:26:05','2018-03-09 17:26:05',0),('2018-03-09 17:26:05','db89733d01719acfd7880582899caf75','REPARTITION','vdc_stats_by_hour',0,'2018-03-09 17:26:05','2018-03-09 17:26:05',0),('2018-03-09 17:26:06','db89733d01719acfd7880582899caf75','REPARTITION','vdc_stats_by_month',0,'2018-03-09 17:26:05','2018-03-09 17:26:06',1),('2018-03-09 17:26:06','db85e9d7927b614657e060d41b31f2bf','REPARTITION','vm_stats_latest',0,'2018-03-09 17:26:06','2018-03-09 17:26:06',0),('2018-03-09 17:26:06','db85e9d7927b614657e060d41b31f2bf','REPARTITION','vm_stats_by_day',0,'2018-03-09 17:26:06','2018-03-09 17:26:06',0),('2018-03-09 17:26:07','db85e9d7927b614657e060d41b31f2bf','REPARTITION','vm_stats_by_hour',0,'2018-03-09 17:26:06','2018-03-09 17:26:07',1),('2018-03-09 17:26:07','f91c3f47beb9b0df229900b974601fd0','REPARTITION','vm_stats_by_month',0,'2018-03-09 17:26:07','2018-03-09 17:26:07',0),('2018-03-09 17:26:07','f91c3f47beb9b0df229900b974601fd0','REPARTITION','vpod_stats_latest',0,'2018-03-09 17:26:07','2018-03-09 17:26:07',0),('2018-03-09 17:26:07','f91c3f47beb9b0df229900b974601fd0','REPARTITION','vpod_stats_by_day',0,'2018-03-09 17:26:07','2018-03-09 17:26:07',0),('2018-03-09 17:26:08','f91c3f47beb9b0df229900b974601fd0','REPARTITION','vpod_stats_by_hour',0,'2018-03-09 17:26:07','2018-03-09 17:26:08',1),('2018-03-09 17:26:08','b5ff94decf8d2ba620a03dd3889a10e8','REPARTITION','vpod_stats_by_month',0,'2018-03-09 17:26:08','2018-03-09 17:26:08',0),('2018-03-09 17:32:36','912bec8c795c199067acb8958be1f0de','REPARTITION','market_stats_latest',0,'2018-03-09 17:32:36','2018-03-09 17:32:36',0),('2018-03-09 17:32:36','912bec8c795c199067acb8958be1f0de','REPARTITION','market_stats_by_day',0,'2018-03-09 17:32:36','2018-03-09 17:32:36',0),('2018-03-09 17:32:37','912bec8c795c199067acb8958be1f0de','REPARTITION','market_stats_by_hour',0,'2018-03-09 17:32:36','2018-03-09 17:32:37',1),('2018-03-09 17:32:37','8e4e8fa9e029f36228e0d556c9fb86d3','REPARTITION','market_stats_by_month',0,'2018-03-09 17:32:37','2018-03-09 17:32:37',0),('2018-03-09 17:32:37','8e4e8fa9e029f36228e0d556c9fb86d3','REPARTITION','app_stats_latest',0,'2018-03-09 17:32:37','2018-03-09 17:32:37',0),('2018-03-09 17:32:37','8e4e8fa9e029f36228e0d556c9fb86d3','REPARTITION','app_stats_by_day',0,'2018-03-09 17:32:37','2018-03-09 17:32:37',0),('2018-03-09 17:32:37','8e4e8fa9e029f36228e0d556c9fb86d3','REPARTITION','app_stats_by_hour',0,'2018-03-09 17:32:37','2018-03-09 17:32:37',0),('2018-03-09 17:32:37','8e4e8fa9e029f36228e0d556c9fb86d3','REPARTITION','app_stats_by_month',0,'2018-03-09 17:32:37','2018-03-09 17:32:37',0),('2018-03-09 17:32:38','8e4e8fa9e029f36228e0d556c9fb86d3','REPARTITION','ch_stats_latest',0,'2018-03-09 17:32:37','2018-03-09 17:32:38',1),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','ch_stats_by_day',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','ch_stats_by_hour',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','ch_stats_by_month',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','cnt_stats_latest',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','cnt_stats_by_day',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','cnt_stats_by_hour',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:38','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','cnt_stats_by_month',0,'2018-03-09 17:32:38','2018-03-09 17:32:38',0),('2018-03-09 17:32:40','50163ac0f75a387f9cf5bda4d61736fd','REPARTITION','cpod_stats_latest',0,'2018-03-09 17:32:38','2018-03-09 17:32:40',2),('2018-03-09 17:32:40','fa7a42efb5d27d69c4e2d8b05cfc205d','REPARTITION','cpod_stats_by_day',0,'2018-03-09 17:32:40','2018-03-09 17:32:40',0),('2018-03-09 17:32:41','fa7a42efb5d27d69c4e2d8b05cfc205d','REPARTITION','cpod_stats_by_hour',0,'2018-03-09 17:32:40','2018-03-09 17:32:41',1),('2018-03-09 17:32:42','3d34fce2a6368d94067398170b0a2ab2','REPARTITION','cpod_stats_by_month',0,'2018-03-09 17:32:41','2018-03-09 17:32:42',1),('2018-03-09 17:32:42','39c2de564c80aff36247872fd46a792c','REPARTITION','dpod_stats_latest',0,'2018-03-09 17:32:42','2018-03-09 17:32:42',0),('2018-03-09 17:32:42','39c2de564c80aff36247872fd46a792c','REPARTITION','dpod_stats_by_day',0,'2018-03-09 17:32:42','2018-03-09 17:32:42',0),('2018-03-09 17:32:42','39c2de564c80aff36247872fd46a792c','REPARTITION','dpod_stats_by_hour',0,'2018-03-09 17:32:42','2018-03-09 17:32:42',0),('2018-03-09 17:32:42','39c2de564c80aff36247872fd46a792c','REPARTITION','dpod_stats_by_month',0,'2018-03-09 17:32:42','2018-03-09 17:32:42',0),('2018-03-09 17:32:44','39c2de564c80aff36247872fd46a792c','REPARTITION','da_stats_latest',0,'2018-03-09 17:32:42','2018-03-09 17:32:44',2),('2018-03-09 17:32:51','3c0f711607cd95b063278ebe357f8fd3','REPARTITION','da_stats_by_day',0,'2018-03-09 17:32:44','2018-03-09 17:32:51',7),('2018-03-09 17:32:52','b5baaea583db8a77b0cc000e4b9ee798','REPARTITION','da_stats_by_hour',0,'2018-03-09 17:32:51','2018-03-09 17:32:52',1),('2018-03-09 17:32:54','36597ff7cd24ece650ba96cab9e0af4e','REPARTITION','da_stats_by_month',0,'2018-03-09 17:32:52','2018-03-09 17:32:54',2),('2018-03-09 17:32:54','7cf2589a8503cd12f138a57f9cedcd51','REPARTITION','ds_stats_latest',0,'2018-03-09 17:32:54','2018-03-09 17:32:54',0),('2018-03-09 17:32:55','7cf2589a8503cd12f138a57f9cedcd51','REPARTITION','ds_stats_by_day',0,'2018-03-09 17:32:54','2018-03-09 17:32:55',1),('2018-03-09 17:32:55','c8022186bd950a2647769b596c088689','REPARTITION','ds_stats_by_hour',0,'2018-03-09 17:32:55','2018-03-09 17:32:55',0),('2018-03-09 17:32:55','c8022186bd950a2647769b596c088689','REPARTITION','ds_stats_by_month',0,'2018-03-09 17:32:55','2018-03-09 17:32:55',0),('2018-03-09 17:32:55','c8022186bd950a2647769b596c088689','REPARTITION','iom_stats_latest',0,'2018-03-09 17:32:55','2018-03-09 17:32:55',0),('2018-03-09 17:32:55','c8022186bd950a2647769b596c088689','REPARTITION','iom_stats_by_day',0,'2018-03-09 17:32:55','2018-03-09 17:32:55',0),('2018-03-09 17:32:56','c8022186bd950a2647769b596c088689','REPARTITION','iom_stats_by_hour',0,'2018-03-09 17:32:55','2018-03-09 17:32:56',1),('2018-03-09 17:32:56','a0f091196a645009e3602e89747469ed','REPARTITION','iom_stats_by_month',0,'2018-03-09 17:32:56','2018-03-09 17:32:56',0),('2018-03-09 17:32:59','a0f091196a645009e3602e89747469ed','REPARTITION','lp_stats_latest',0,'2018-03-09 17:32:56','2018-03-09 17:32:59',3),('2018-03-09 17:33:00','c51c3feb08ad68a18b782b91f9a38303','REPARTITION','lp_stats_by_day',0,'2018-03-09 17:32:59','2018-03-09 17:33:00',1),('2018-03-09 17:33:01','f2c92e8d77e10a2d754eea500219f407','REPARTITION','lp_stats_by_hour',0,'2018-03-09 17:33:00','2018-03-09 17:33:01',1),('2018-03-09 17:33:02','248e48b2ce5e6465587374829b8599fa','REPARTITION','lp_stats_by_month',0,'2018-03-09 17:33:01','2018-03-09 17:33:02',1),('2018-03-09 17:33:02','2274d7460befc763c324d8d7cb215bde','REPARTITION','pm_stats_latest',0,'2018-03-09 17:33:02','2018-03-09 17:33:02',0),('2018-03-09 17:33:02','2274d7460befc763c324d8d7cb215bde','REPARTITION','pm_stats_by_day',0,'2018-03-09 17:33:02','2018-03-09 17:33:02',0),('2018-03-09 17:33:02','2274d7460befc763c324d8d7cb215bde','REPARTITION','pm_stats_by_hour',0,'2018-03-09 17:33:02','2018-03-09 17:33:02',0),('2018-03-09 17:33:02','2274d7460befc763c324d8d7cb215bde','REPARTITION','pm_stats_by_month',0,'2018-03-09 17:33:02','2018-03-09 17:33:02',0),('2018-03-09 17:33:02','2274d7460befc763c324d8d7cb215bde','REPARTITION','sc_stats_latest',0,'2018-03-09 17:33:02','2018-03-09 17:33:02',0),('2018-03-09 17:33:03','2274d7460befc763c324d8d7cb215bde','REPARTITION','sc_stats_by_day',0,'2018-03-09 17:33:02','2018-03-09 17:33:03',1),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','sc_stats_by_hour',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','sc_stats_by_month',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','sw_stats_latest',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','sw_stats_by_day',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','sw_stats_by_hour',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','sw_stats_by_month',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','vdc_stats_latest',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','vdc_stats_by_day',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:03','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','vdc_stats_by_hour',0,'2018-03-09 17:33:03','2018-03-09 17:33:03',0),('2018-03-09 17:33:04','f315a7b5a30a6c28bdf80bd565eed13b','REPARTITION','vdc_stats_by_month',0,'2018-03-09 17:33:03','2018-03-09 17:33:04',1),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vm_stats_latest',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vm_stats_by_day',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vm_stats_by_hour',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vm_stats_by_month',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vpod_stats_latest',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vpod_stats_by_day',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vpod_stats_by_hour',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0),('2018-03-09 17:33:04','36b61c0764aab2057e45fe15683f48a9','REPARTITION','vpod_stats_by_month',0,'2018-03-09 17:33:04','2018-03-09 17:33:04',0);
/*!40000 ALTER TABLE `appl_performance` ENABLE KEYS */;
UNLOCK TABLES;

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
PARTITION before20180110071312 VALUES LESS THAN (63682787592) ENGINE = InnoDB,
PARTITION before20180111210112 VALUES LESS THAN (63682923672) ENGINE = InnoDB,
PARTITION before20180113104912 VALUES LESS THAN (63683059752) ENGINE = InnoDB,
PARTITION before20180115003712 VALUES LESS THAN (63683195832) ENGINE = InnoDB,
PARTITION before20180116142512 VALUES LESS THAN (63683331912) ENGINE = InnoDB,
PARTITION before20180118041312 VALUES LESS THAN (63683467992) ENGINE = InnoDB,
PARTITION before20180119180112 VALUES LESS THAN (63683604072) ENGINE = InnoDB,
PARTITION before20180121074912 VALUES LESS THAN (63683740152) ENGINE = InnoDB,
PARTITION before20180122213712 VALUES LESS THAN (63683876232) ENGINE = InnoDB,
PARTITION before20180124112512 VALUES LESS THAN (63684012312) ENGINE = InnoDB,
PARTITION before20180126011312 VALUES LESS THAN (63684148392) ENGINE = InnoDB,
PARTITION before20180127150112 VALUES LESS THAN (63684284472) ENGINE = InnoDB,
PARTITION before20180129044912 VALUES LESS THAN (63684420552) ENGINE = InnoDB,
PARTITION before20180130183712 VALUES LESS THAN (63684556632) ENGINE = InnoDB,
PARTITION before20180201082512 VALUES LESS THAN (63684692712) ENGINE = InnoDB,
PARTITION before20180202221312 VALUES LESS THAN (63684828792) ENGINE = InnoDB,
PARTITION before20180204120112 VALUES LESS THAN (63684964872) ENGINE = InnoDB,
PARTITION before20180206014912 VALUES LESS THAN (63685100952) ENGINE = InnoDB,
PARTITION before20180207153712 VALUES LESS THAN (63685237032) ENGINE = InnoDB,
PARTITION before20180209052512 VALUES LESS THAN (63685373112) ENGINE = InnoDB,
PARTITION before20180210191312 VALUES LESS THAN (63685509192) ENGINE = InnoDB,
PARTITION before20180212090112 VALUES LESS THAN (63685645272) ENGINE = InnoDB,
PARTITION before20180213224912 VALUES LESS THAN (63685781352) ENGINE = InnoDB,
PARTITION before20180215123712 VALUES LESS THAN (63685917432) ENGINE = InnoDB,
PARTITION before20180217022512 VALUES LESS THAN (63686053512) ENGINE = InnoDB,
PARTITION before20180218161312 VALUES LESS THAN (63686189592) ENGINE = InnoDB,
PARTITION before20180220060112 VALUES LESS THAN (63686325672) ENGINE = InnoDB,
PARTITION before20180221194912 VALUES LESS THAN (63686461752) ENGINE = InnoDB,
PARTITION before20180223093712 VALUES LESS THAN (63686597832) ENGINE = InnoDB,
PARTITION before20180224232512 VALUES LESS THAN (63686733912) ENGINE = InnoDB,
PARTITION before20180226131312 VALUES LESS THAN (63686869992) ENGINE = InnoDB,
PARTITION before20180228030112 VALUES LESS THAN (63687006072) ENGINE = InnoDB,
PARTITION before20180301164912 VALUES LESS THAN (63687142152) ENGINE = InnoDB,
PARTITION before20180303063712 VALUES LESS THAN (63687278232) ENGINE = InnoDB,
PARTITION before20180304202512 VALUES LESS THAN (63687414312) ENGINE = InnoDB,
PARTITION before20180306101312 VALUES LESS THAN (63687550392) ENGINE = InnoDB,
PARTITION before20180308000112 VALUES LESS THAN (63687686472) ENGINE = InnoDB,
PARTITION before20180309134912 VALUES LESS THAN (63687822552) ENGINE = InnoDB,
PARTITION before20180311033712 VALUES LESS THAN (63687958632) ENGINE = InnoDB,
PARTITION before20180312172512 VALUES LESS THAN (63688094712) ENGINE = InnoDB,
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
PARTITION before20180306192512 VALUES LESS THAN (63687583512) ENGINE = InnoDB,
PARTITION before20180306212512 VALUES LESS THAN (63687590712) ENGINE = InnoDB,
PARTITION before20180306232512 VALUES LESS THAN (63687597912) ENGINE = InnoDB,
PARTITION before20180307012512 VALUES LESS THAN (63687605112) ENGINE = InnoDB,
PARTITION before20180307032512 VALUES LESS THAN (63687612312) ENGINE = InnoDB,
PARTITION before20180307052512 VALUES LESS THAN (63687619512) ENGINE = InnoDB,
PARTITION before20180307072512 VALUES LESS THAN (63687626712) ENGINE = InnoDB,
PARTITION before20180307092512 VALUES LESS THAN (63687633912) ENGINE = InnoDB,
PARTITION before20180307112512 VALUES LESS THAN (63687641112) ENGINE = InnoDB,
PARTITION before20180307132512 VALUES LESS THAN (63687648312) ENGINE = InnoDB,
PARTITION before20180307152512 VALUES LESS THAN (63687655512) ENGINE = InnoDB,
PARTITION before20180307172512 VALUES LESS THAN (63687662712) ENGINE = InnoDB,
PARTITION before20180307192512 VALUES LESS THAN (63687669912) ENGINE = InnoDB,
PARTITION before20180307212512 VALUES LESS THAN (63687677112) ENGINE = InnoDB,
PARTITION before20180307232512 VALUES LESS THAN (63687684312) ENGINE = InnoDB,
PARTITION before20180308012512 VALUES LESS THAN (63687691512) ENGINE = InnoDB,
PARTITION before20180308032512 VALUES LESS THAN (63687698712) ENGINE = InnoDB,
PARTITION before20180308052512 VALUES LESS THAN (63687705912) ENGINE = InnoDB,
PARTITION before20180308072512 VALUES LESS THAN (63687713112) ENGINE = InnoDB,
PARTITION before20180308092512 VALUES LESS THAN (63687720312) ENGINE = InnoDB,
PARTITION before20180308112512 VALUES LESS THAN (63687727512) ENGINE = InnoDB,
PARTITION before20180308132512 VALUES LESS THAN (63687734712) ENGINE = InnoDB,
PARTITION before20180308152512 VALUES LESS THAN (63687741912) ENGINE = InnoDB,
PARTITION before20180308172512 VALUES LESS THAN (63687749112) ENGINE = InnoDB,
PARTITION before20180308192512 VALUES LESS THAN (63687756312) ENGINE = InnoDB,
PARTITION before20180308212512 VALUES LESS THAN (63687763512) ENGINE = InnoDB,
PARTITION before20180308232512 VALUES LESS THAN (63687770712) ENGINE = InnoDB,
PARTITION before20180309012512 VALUES LESS THAN (63687777912) ENGINE = InnoDB,
PARTITION before20180309032512 VALUES LESS THAN (63687785112) ENGINE = InnoDB,
PARTITION before20180309052512 VALUES LESS THAN (63687792312) ENGINE = InnoDB,
PARTITION before20180309072512 VALUES LESS THAN (63687799512) ENGINE = InnoDB,
PARTITION before20180309092512 VALUES LESS THAN (63687806712) ENGINE = InnoDB,
PARTITION before20180309112512 VALUES LESS THAN (63687813912) ENGINE = InnoDB,
PARTITION before20180309132512 VALUES LESS THAN (63687821112) ENGINE = InnoDB,
PARTITION before20180309152512 VALUES LESS THAN (63687828312) ENGINE = InnoDB,
PARTITION before20180309172512 VALUES LESS THAN (63687835512) ENGINE = InnoDB,
PARTITION before20180309192512 VALUES LESS THAN (63687842712) ENGINE = InnoDB,
PARTITION before20180309212512 VALUES LESS THAN (63687849912) ENGINE = InnoDB,
PARTITION before20180309232512 VALUES LESS THAN (63687857112) ENGINE = InnoDB,
PARTITION before20180310012512 VALUES LESS THAN (63687864312) ENGINE = InnoDB,
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
PARTITION before20160314093713 VALUES LESS THAN (63625167433) ENGINE = InnoDB,
PARTITION before20160402014913 VALUES LESS THAN (63626780953) ENGINE = InnoDB,
PARTITION before20160420180113 VALUES LESS THAN (63628394473) ENGINE = InnoDB,
PARTITION before20160509101313 VALUES LESS THAN (63630007993) ENGINE = InnoDB,
PARTITION before20160528022513 VALUES LESS THAN (63631621513) ENGINE = InnoDB,
PARTITION before20160615183713 VALUES LESS THAN (63633235033) ENGINE = InnoDB,
PARTITION before20160704104913 VALUES LESS THAN (63634848553) ENGINE = InnoDB,
PARTITION before20160723030113 VALUES LESS THAN (63636462073) ENGINE = InnoDB,
PARTITION before20160810191313 VALUES LESS THAN (63638075593) ENGINE = InnoDB,
PARTITION before20160829112513 VALUES LESS THAN (63639689113) ENGINE = InnoDB,
PARTITION before20160917033713 VALUES LESS THAN (63641302633) ENGINE = InnoDB,
PARTITION before20161005194913 VALUES LESS THAN (63642916153) ENGINE = InnoDB,
PARTITION before20161024120113 VALUES LESS THAN (63644529673) ENGINE = InnoDB,
PARTITION before20161112041313 VALUES LESS THAN (63646143193) ENGINE = InnoDB,
PARTITION before20161130202513 VALUES LESS THAN (63647756713) ENGINE = InnoDB,
PARTITION before20161219123713 VALUES LESS THAN (63649370233) ENGINE = InnoDB,
PARTITION before20170107044913 VALUES LESS THAN (63650983753) ENGINE = InnoDB,
PARTITION before20170125210113 VALUES LESS THAN (63652597273) ENGINE = InnoDB,
PARTITION before20170213131313 VALUES LESS THAN (63654210793) ENGINE = InnoDB,
PARTITION before20170304052513 VALUES LESS THAN (63655824313) ENGINE = InnoDB,
PARTITION before20170322213713 VALUES LESS THAN (63657437833) ENGINE = InnoDB,
PARTITION before20170410134913 VALUES LESS THAN (63659051353) ENGINE = InnoDB,
PARTITION before20170429060113 VALUES LESS THAN (63660664873) ENGINE = InnoDB,
PARTITION before20170517221313 VALUES LESS THAN (63662278393) ENGINE = InnoDB,
PARTITION before20170605142513 VALUES LESS THAN (63663891913) ENGINE = InnoDB,
PARTITION before20170624063713 VALUES LESS THAN (63665505433) ENGINE = InnoDB,
PARTITION before20170712224913 VALUES LESS THAN (63667118953) ENGINE = InnoDB,
PARTITION before20170731150113 VALUES LESS THAN (63668732473) ENGINE = InnoDB,
PARTITION before20170819071313 VALUES LESS THAN (63670345993) ENGINE = InnoDB,
PARTITION before20170906232513 VALUES LESS THAN (63671959513) ENGINE = InnoDB,
PARTITION before20170925153713 VALUES LESS THAN (63673573033) ENGINE = InnoDB,
PARTITION before20171014074913 VALUES LESS THAN (63675186553) ENGINE = InnoDB,
PARTITION before20171102000113 VALUES LESS THAN (63676800073) ENGINE = InnoDB,
PARTITION before20171120161313 VALUES LESS THAN (63678413593) ENGINE = InnoDB,
PARTITION before20171209082513 VALUES LESS THAN (63680027113) ENGINE = InnoDB,
PARTITION before20171228003713 VALUES LESS THAN (63681640633) ENGINE = InnoDB,
PARTITION before20180115164913 VALUES LESS THAN (63683254153) ENGINE = InnoDB,
PARTITION before20180203090113 VALUES LESS THAN (63684867673) ENGINE = InnoDB,
PARTITION before20180222011313 VALUES LESS THAN (63686481193) ENGINE = InnoDB,
PARTITION before20180312172513 VALUES LESS THAN (63688094713) ENGINE = InnoDB,
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
PARTITION before20180309153711 VALUES LESS THAN (63687829031) ENGINE = InnoDB,
PARTITION before20180309154311 VALUES LESS THAN (63687829391) ENGINE = InnoDB,
PARTITION before20180309154911 VALUES LESS THAN (63687829751) ENGINE = InnoDB,
PARTITION before20180309155511 VALUES LESS THAN (63687830111) ENGINE = InnoDB,
PARTITION before20180309160111 VALUES LESS THAN (63687830471) ENGINE = InnoDB,
PARTITION before20180309160711 VALUES LESS THAN (63687830831) ENGINE = InnoDB,
PARTITION before20180309161311 VALUES LESS THAN (63687831191) ENGINE = InnoDB,
PARTITION before20180309161911 VALUES LESS THAN (63687831551) ENGINE = InnoDB,
PARTITION before20180309162511 VALUES LESS THAN (63687831911) ENGINE = InnoDB,
PARTITION before20180309163111 VALUES LESS THAN (63687832271) ENGINE = InnoDB,
PARTITION before20180309163711 VALUES LESS THAN (63687832631) ENGINE = InnoDB,
PARTITION before20180309164311 VALUES LESS THAN (63687832991) ENGINE = InnoDB,
PARTITION before20180309164911 VALUES LESS THAN (63687833351) ENGINE = InnoDB,
PARTITION before20180309165511 VALUES LESS THAN (63687833711) ENGINE = InnoDB,
PARTITION before20180309170111 VALUES LESS THAN (63687834071) ENGINE = InnoDB,
PARTITION before20180309170711 VALUES LESS THAN (63687834431) ENGINE = InnoDB,
PARTITION before20180309171311 VALUES LESS THAN (63687834791) ENGINE = InnoDB,
PARTITION before20180309171911 VALUES LESS THAN (63687835151) ENGINE = InnoDB,
PARTITION before20180309172511 VALUES LESS THAN (63687835511) ENGINE = InnoDB,
PARTITION before20180309173111 VALUES LESS THAN (63687835871) ENGINE = InnoDB,
PARTITION before20180309173711 VALUES LESS THAN (63687836231) ENGINE = InnoDB,
PARTITION before20180309174311 VALUES LESS THAN (63687836591) ENGINE = InnoDB,
PARTITION before20180309174911 VALUES LESS THAN (63687836951) ENGINE = InnoDB,
PARTITION before20180309175511 VALUES LESS THAN (63687837311) ENGINE = InnoDB,
PARTITION before20180309180111 VALUES LESS THAN (63687837671) ENGINE = InnoDB,
PARTITION before20180309180711 VALUES LESS THAN (63687838031) ENGINE = InnoDB,
PARTITION before20180309181311 VALUES LESS THAN (63687838391) ENGINE = InnoDB,
PARTITION before20180309181911 VALUES LESS THAN (63687838751) ENGINE = InnoDB,
PARTITION before20180309182511 VALUES LESS THAN (63687839111) ENGINE = InnoDB,
PARTITION before20180309183111 VALUES LESS THAN (63687839471) ENGINE = InnoDB,
PARTITION before20180309183711 VALUES LESS THAN (63687839831) ENGINE = InnoDB,
PARTITION before20180309184311 VALUES LESS THAN (63687840191) ENGINE = InnoDB,
PARTITION before20180309184911 VALUES LESS THAN (63687840551) ENGINE = InnoDB,
PARTITION before20180309185511 VALUES LESS THAN (63687840911) ENGINE = InnoDB,
PARTITION before20180309190111 VALUES LESS THAN (63687841271) ENGINE = InnoDB,
PARTITION before20180309190711 VALUES LESS THAN (63687841631) ENGINE = InnoDB,
PARTITION before20180309191311 VALUES LESS THAN (63687841991) ENGINE = InnoDB,
PARTITION before20180309191911 VALUES LESS THAN (63687842351) ENGINE = InnoDB,
PARTITION before20180309192511 VALUES LESS THAN (63687842711) ENGINE = InnoDB,
PARTITION before20180309193111 VALUES LESS THAN (63687843071) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_ch_primary_keys BEFORE INSERT ON ch_stats_latest
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
PARTITION before20180110071315 VALUES LESS THAN (63682787595) ENGINE = InnoDB,
PARTITION before20180111210115 VALUES LESS THAN (63682923675) ENGINE = InnoDB,
PARTITION before20180113104915 VALUES LESS THAN (63683059755) ENGINE = InnoDB,
PARTITION before20180115003715 VALUES LESS THAN (63683195835) ENGINE = InnoDB,
PARTITION before20180116142515 VALUES LESS THAN (63683331915) ENGINE = InnoDB,
PARTITION before20180118041315 VALUES LESS THAN (63683467995) ENGINE = InnoDB,
PARTITION before20180119180115 VALUES LESS THAN (63683604075) ENGINE = InnoDB,
PARTITION before20180121074915 VALUES LESS THAN (63683740155) ENGINE = InnoDB,
PARTITION before20180122213715 VALUES LESS THAN (63683876235) ENGINE = InnoDB,
PARTITION before20180124112515 VALUES LESS THAN (63684012315) ENGINE = InnoDB,
PARTITION before20180126011315 VALUES LESS THAN (63684148395) ENGINE = InnoDB,
PARTITION before20180127150115 VALUES LESS THAN (63684284475) ENGINE = InnoDB,
PARTITION before20180129044915 VALUES LESS THAN (63684420555) ENGINE = InnoDB,
PARTITION before20180130183715 VALUES LESS THAN (63684556635) ENGINE = InnoDB,
PARTITION before20180201082515 VALUES LESS THAN (63684692715) ENGINE = InnoDB,
PARTITION before20180202221315 VALUES LESS THAN (63684828795) ENGINE = InnoDB,
PARTITION before20180204120115 VALUES LESS THAN (63684964875) ENGINE = InnoDB,
PARTITION before20180206014915 VALUES LESS THAN (63685100955) ENGINE = InnoDB,
PARTITION before20180207153715 VALUES LESS THAN (63685237035) ENGINE = InnoDB,
PARTITION before20180209052515 VALUES LESS THAN (63685373115) ENGINE = InnoDB,
PARTITION before20180210191315 VALUES LESS THAN (63685509195) ENGINE = InnoDB,
PARTITION before20180212090115 VALUES LESS THAN (63685645275) ENGINE = InnoDB,
PARTITION before20180213224915 VALUES LESS THAN (63685781355) ENGINE = InnoDB,
PARTITION before20180215123715 VALUES LESS THAN (63685917435) ENGINE = InnoDB,
PARTITION before20180217022515 VALUES LESS THAN (63686053515) ENGINE = InnoDB,
PARTITION before20180218161315 VALUES LESS THAN (63686189595) ENGINE = InnoDB,
PARTITION before20180220060115 VALUES LESS THAN (63686325675) ENGINE = InnoDB,
PARTITION before20180221194915 VALUES LESS THAN (63686461755) ENGINE = InnoDB,
PARTITION before20180223093715 VALUES LESS THAN (63686597835) ENGINE = InnoDB,
PARTITION before20180224232515 VALUES LESS THAN (63686733915) ENGINE = InnoDB,
PARTITION before20180226131315 VALUES LESS THAN (63686869995) ENGINE = InnoDB,
PARTITION before20180228030115 VALUES LESS THAN (63687006075) ENGINE = InnoDB,
PARTITION before20180301164915 VALUES LESS THAN (63687142155) ENGINE = InnoDB,
PARTITION before20180303063715 VALUES LESS THAN (63687278235) ENGINE = InnoDB,
PARTITION before20180304202515 VALUES LESS THAN (63687414315) ENGINE = InnoDB,
PARTITION before20180306101315 VALUES LESS THAN (63687550395) ENGINE = InnoDB,
PARTITION before20180308000115 VALUES LESS THAN (63687686475) ENGINE = InnoDB,
PARTITION before20180309134915 VALUES LESS THAN (63687822555) ENGINE = InnoDB,
PARTITION before20180311033715 VALUES LESS THAN (63687958635) ENGINE = InnoDB,
PARTITION before20180312172515 VALUES LESS THAN (63688094715) ENGINE = InnoDB,
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
PARTITION before20180306192516 VALUES LESS THAN (63687583516) ENGINE = InnoDB,
PARTITION before20180306212516 VALUES LESS THAN (63687590716) ENGINE = InnoDB,
PARTITION before20180306232516 VALUES LESS THAN (63687597916) ENGINE = InnoDB,
PARTITION before20180307012516 VALUES LESS THAN (63687605116) ENGINE = InnoDB,
PARTITION before20180307032516 VALUES LESS THAN (63687612316) ENGINE = InnoDB,
PARTITION before20180307052516 VALUES LESS THAN (63687619516) ENGINE = InnoDB,
PARTITION before20180307072516 VALUES LESS THAN (63687626716) ENGINE = InnoDB,
PARTITION before20180307092516 VALUES LESS THAN (63687633916) ENGINE = InnoDB,
PARTITION before20180307112516 VALUES LESS THAN (63687641116) ENGINE = InnoDB,
PARTITION before20180307132516 VALUES LESS THAN (63687648316) ENGINE = InnoDB,
PARTITION before20180307152516 VALUES LESS THAN (63687655516) ENGINE = InnoDB,
PARTITION before20180307172516 VALUES LESS THAN (63687662716) ENGINE = InnoDB,
PARTITION before20180307192516 VALUES LESS THAN (63687669916) ENGINE = InnoDB,
PARTITION before20180307212516 VALUES LESS THAN (63687677116) ENGINE = InnoDB,
PARTITION before20180307232516 VALUES LESS THAN (63687684316) ENGINE = InnoDB,
PARTITION before20180308012516 VALUES LESS THAN (63687691516) ENGINE = InnoDB,
PARTITION before20180308032516 VALUES LESS THAN (63687698716) ENGINE = InnoDB,
PARTITION before20180308052516 VALUES LESS THAN (63687705916) ENGINE = InnoDB,
PARTITION before20180308072516 VALUES LESS THAN (63687713116) ENGINE = InnoDB,
PARTITION before20180308092516 VALUES LESS THAN (63687720316) ENGINE = InnoDB,
PARTITION before20180308112516 VALUES LESS THAN (63687727516) ENGINE = InnoDB,
PARTITION before20180308132516 VALUES LESS THAN (63687734716) ENGINE = InnoDB,
PARTITION before20180308152516 VALUES LESS THAN (63687741916) ENGINE = InnoDB,
PARTITION before20180308172516 VALUES LESS THAN (63687749116) ENGINE = InnoDB,
PARTITION before20180308192516 VALUES LESS THAN (63687756316) ENGINE = InnoDB,
PARTITION before20180308212516 VALUES LESS THAN (63687763516) ENGINE = InnoDB,
PARTITION before20180308232516 VALUES LESS THAN (63687770716) ENGINE = InnoDB,
PARTITION before20180309012516 VALUES LESS THAN (63687777916) ENGINE = InnoDB,
PARTITION before20180309032516 VALUES LESS THAN (63687785116) ENGINE = InnoDB,
PARTITION before20180309052516 VALUES LESS THAN (63687792316) ENGINE = InnoDB,
PARTITION before20180309072516 VALUES LESS THAN (63687799516) ENGINE = InnoDB,
PARTITION before20180309092516 VALUES LESS THAN (63687806716) ENGINE = InnoDB,
PARTITION before20180309112516 VALUES LESS THAN (63687813916) ENGINE = InnoDB,
PARTITION before20180309132516 VALUES LESS THAN (63687821116) ENGINE = InnoDB,
PARTITION before20180309152516 VALUES LESS THAN (63687828316) ENGINE = InnoDB,
PARTITION before20180309172516 VALUES LESS THAN (63687835516) ENGINE = InnoDB,
PARTITION before20180309192516 VALUES LESS THAN (63687842716) ENGINE = InnoDB,
PARTITION before20180309212516 VALUES LESS THAN (63687849916) ENGINE = InnoDB,
PARTITION before20180309232516 VALUES LESS THAN (63687857116) ENGINE = InnoDB,
PARTITION before20180310012516 VALUES LESS THAN (63687864316) ENGINE = InnoDB,
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
PARTITION before20160314093716 VALUES LESS THAN (63625167436) ENGINE = InnoDB,
PARTITION before20160402014916 VALUES LESS THAN (63626780956) ENGINE = InnoDB,
PARTITION before20160420180116 VALUES LESS THAN (63628394476) ENGINE = InnoDB,
PARTITION before20160509101316 VALUES LESS THAN (63630007996) ENGINE = InnoDB,
PARTITION before20160528022516 VALUES LESS THAN (63631621516) ENGINE = InnoDB,
PARTITION before20160615183716 VALUES LESS THAN (63633235036) ENGINE = InnoDB,
PARTITION before20160704104916 VALUES LESS THAN (63634848556) ENGINE = InnoDB,
PARTITION before20160723030116 VALUES LESS THAN (63636462076) ENGINE = InnoDB,
PARTITION before20160810191316 VALUES LESS THAN (63638075596) ENGINE = InnoDB,
PARTITION before20160829112516 VALUES LESS THAN (63639689116) ENGINE = InnoDB,
PARTITION before20160917033716 VALUES LESS THAN (63641302636) ENGINE = InnoDB,
PARTITION before20161005194916 VALUES LESS THAN (63642916156) ENGINE = InnoDB,
PARTITION before20161024120116 VALUES LESS THAN (63644529676) ENGINE = InnoDB,
PARTITION before20161112041316 VALUES LESS THAN (63646143196) ENGINE = InnoDB,
PARTITION before20161130202516 VALUES LESS THAN (63647756716) ENGINE = InnoDB,
PARTITION before20161219123716 VALUES LESS THAN (63649370236) ENGINE = InnoDB,
PARTITION before20170107044916 VALUES LESS THAN (63650983756) ENGINE = InnoDB,
PARTITION before20170125210116 VALUES LESS THAN (63652597276) ENGINE = InnoDB,
PARTITION before20170213131316 VALUES LESS THAN (63654210796) ENGINE = InnoDB,
PARTITION before20170304052516 VALUES LESS THAN (63655824316) ENGINE = InnoDB,
PARTITION before20170322213716 VALUES LESS THAN (63657437836) ENGINE = InnoDB,
PARTITION before20170410134916 VALUES LESS THAN (63659051356) ENGINE = InnoDB,
PARTITION before20170429060116 VALUES LESS THAN (63660664876) ENGINE = InnoDB,
PARTITION before20170517221316 VALUES LESS THAN (63662278396) ENGINE = InnoDB,
PARTITION before20170605142516 VALUES LESS THAN (63663891916) ENGINE = InnoDB,
PARTITION before20170624063716 VALUES LESS THAN (63665505436) ENGINE = InnoDB,
PARTITION before20170712224916 VALUES LESS THAN (63667118956) ENGINE = InnoDB,
PARTITION before20170731150116 VALUES LESS THAN (63668732476) ENGINE = InnoDB,
PARTITION before20170819071316 VALUES LESS THAN (63670345996) ENGINE = InnoDB,
PARTITION before20170906232516 VALUES LESS THAN (63671959516) ENGINE = InnoDB,
PARTITION before20170925153716 VALUES LESS THAN (63673573036) ENGINE = InnoDB,
PARTITION before20171014074916 VALUES LESS THAN (63675186556) ENGINE = InnoDB,
PARTITION before20171102000116 VALUES LESS THAN (63676800076) ENGINE = InnoDB,
PARTITION before20171120161316 VALUES LESS THAN (63678413596) ENGINE = InnoDB,
PARTITION before20171209082516 VALUES LESS THAN (63680027116) ENGINE = InnoDB,
PARTITION before20171228003716 VALUES LESS THAN (63681640636) ENGINE = InnoDB,
PARTITION before20180115164916 VALUES LESS THAN (63683254156) ENGINE = InnoDB,
PARTITION before20180203090116 VALUES LESS THAN (63684867676) ENGINE = InnoDB,
PARTITION before20180222011316 VALUES LESS THAN (63686481196) ENGINE = InnoDB,
PARTITION before20180312172516 VALUES LESS THAN (63688094716) ENGINE = InnoDB,
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
PARTITION before20180309153714 VALUES LESS THAN (63687829034) ENGINE = InnoDB,
PARTITION before20180309154314 VALUES LESS THAN (63687829394) ENGINE = InnoDB,
PARTITION before20180309154914 VALUES LESS THAN (63687829754) ENGINE = InnoDB,
PARTITION before20180309155514 VALUES LESS THAN (63687830114) ENGINE = InnoDB,
PARTITION before20180309160114 VALUES LESS THAN (63687830474) ENGINE = InnoDB,
PARTITION before20180309160714 VALUES LESS THAN (63687830834) ENGINE = InnoDB,
PARTITION before20180309161314 VALUES LESS THAN (63687831194) ENGINE = InnoDB,
PARTITION before20180309161914 VALUES LESS THAN (63687831554) ENGINE = InnoDB,
PARTITION before20180309162514 VALUES LESS THAN (63687831914) ENGINE = InnoDB,
PARTITION before20180309163114 VALUES LESS THAN (63687832274) ENGINE = InnoDB,
PARTITION before20180309163714 VALUES LESS THAN (63687832634) ENGINE = InnoDB,
PARTITION before20180309164314 VALUES LESS THAN (63687832994) ENGINE = InnoDB,
PARTITION before20180309164914 VALUES LESS THAN (63687833354) ENGINE = InnoDB,
PARTITION before20180309165514 VALUES LESS THAN (63687833714) ENGINE = InnoDB,
PARTITION before20180309170114 VALUES LESS THAN (63687834074) ENGINE = InnoDB,
PARTITION before20180309170714 VALUES LESS THAN (63687834434) ENGINE = InnoDB,
PARTITION before20180309171314 VALUES LESS THAN (63687834794) ENGINE = InnoDB,
PARTITION before20180309171914 VALUES LESS THAN (63687835154) ENGINE = InnoDB,
PARTITION before20180309172514 VALUES LESS THAN (63687835514) ENGINE = InnoDB,
PARTITION before20180309173114 VALUES LESS THAN (63687835874) ENGINE = InnoDB,
PARTITION before20180309173714 VALUES LESS THAN (63687836234) ENGINE = InnoDB,
PARTITION before20180309174314 VALUES LESS THAN (63687836594) ENGINE = InnoDB,
PARTITION before20180309174914 VALUES LESS THAN (63687836954) ENGINE = InnoDB,
PARTITION before20180309175514 VALUES LESS THAN (63687837314) ENGINE = InnoDB,
PARTITION before20180309180114 VALUES LESS THAN (63687837674) ENGINE = InnoDB,
PARTITION before20180309180714 VALUES LESS THAN (63687838034) ENGINE = InnoDB,
PARTITION before20180309181314 VALUES LESS THAN (63687838394) ENGINE = InnoDB,
PARTITION before20180309181914 VALUES LESS THAN (63687838754) ENGINE = InnoDB,
PARTITION before20180309182514 VALUES LESS THAN (63687839114) ENGINE = InnoDB,
PARTITION before20180309183114 VALUES LESS THAN (63687839474) ENGINE = InnoDB,
PARTITION before20180309183714 VALUES LESS THAN (63687839834) ENGINE = InnoDB,
PARTITION before20180309184314 VALUES LESS THAN (63687840194) ENGINE = InnoDB,
PARTITION before20180309184914 VALUES LESS THAN (63687840554) ENGINE = InnoDB,
PARTITION before20180309185514 VALUES LESS THAN (63687840914) ENGINE = InnoDB,
PARTITION before20180309190114 VALUES LESS THAN (63687841274) ENGINE = InnoDB,
PARTITION before20180309190714 VALUES LESS THAN (63687841634) ENGINE = InnoDB,
PARTITION before20180309191314 VALUES LESS THAN (63687841994) ENGINE = InnoDB,
PARTITION before20180309191914 VALUES LESS THAN (63687842354) ENGINE = InnoDB,
PARTITION before20180309192514 VALUES LESS THAN (63687842714) ENGINE = InnoDB,
PARTITION before20180309193114 VALUES LESS THAN (63687843074) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_cnt_primary_keys BEFORE INSERT ON cnt_stats_latest
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
PARTITION before20180110072040 VALUES LESS THAN (63682788040) ENGINE = InnoDB,
PARTITION before20180111210840 VALUES LESS THAN (63682924120) ENGINE = InnoDB,
PARTITION before20180113105640 VALUES LESS THAN (63683060200) ENGINE = InnoDB,
PARTITION before20180115004440 VALUES LESS THAN (63683196280) ENGINE = InnoDB,
PARTITION before20180116143240 VALUES LESS THAN (63683332360) ENGINE = InnoDB,
PARTITION before20180118042040 VALUES LESS THAN (63683468440) ENGINE = InnoDB,
PARTITION before20180119180840 VALUES LESS THAN (63683604520) ENGINE = InnoDB,
PARTITION before20180121075640 VALUES LESS THAN (63683740600) ENGINE = InnoDB,
PARTITION before20180122214440 VALUES LESS THAN (63683876680) ENGINE = InnoDB,
PARTITION before20180124113240 VALUES LESS THAN (63684012760) ENGINE = InnoDB,
PARTITION before20180126012040 VALUES LESS THAN (63684148840) ENGINE = InnoDB,
PARTITION before20180127150840 VALUES LESS THAN (63684284920) ENGINE = InnoDB,
PARTITION before20180129045640 VALUES LESS THAN (63684421000) ENGINE = InnoDB,
PARTITION before20180130184440 VALUES LESS THAN (63684557080) ENGINE = InnoDB,
PARTITION before20180201083240 VALUES LESS THAN (63684693160) ENGINE = InnoDB,
PARTITION before20180202222040 VALUES LESS THAN (63684829240) ENGINE = InnoDB,
PARTITION before20180204120840 VALUES LESS THAN (63684965320) ENGINE = InnoDB,
PARTITION before20180206015640 VALUES LESS THAN (63685101400) ENGINE = InnoDB,
PARTITION before20180207154440 VALUES LESS THAN (63685237480) ENGINE = InnoDB,
PARTITION before20180209053240 VALUES LESS THAN (63685373560) ENGINE = InnoDB,
PARTITION before20180210192040 VALUES LESS THAN (63685509640) ENGINE = InnoDB,
PARTITION before20180212090840 VALUES LESS THAN (63685645720) ENGINE = InnoDB,
PARTITION before20180213225640 VALUES LESS THAN (63685781800) ENGINE = InnoDB,
PARTITION before20180215124440 VALUES LESS THAN (63685917880) ENGINE = InnoDB,
PARTITION before20180217023240 VALUES LESS THAN (63686053960) ENGINE = InnoDB,
PARTITION before20180218162040 VALUES LESS THAN (63686190040) ENGINE = InnoDB,
PARTITION before20180220060840 VALUES LESS THAN (63686326120) ENGINE = InnoDB,
PARTITION before20180221195640 VALUES LESS THAN (63686462200) ENGINE = InnoDB,
PARTITION before20180223094440 VALUES LESS THAN (63686598280) ENGINE = InnoDB,
PARTITION before20180224233240 VALUES LESS THAN (63686734360) ENGINE = InnoDB,
PARTITION before20180226132040 VALUES LESS THAN (63686870440) ENGINE = InnoDB,
PARTITION before20180228030840 VALUES LESS THAN (63687006520) ENGINE = InnoDB,
PARTITION before20180301165640 VALUES LESS THAN (63687142600) ENGINE = InnoDB,
PARTITION before20180303064440 VALUES LESS THAN (63687278680) ENGINE = InnoDB,
PARTITION before20180304203240 VALUES LESS THAN (63687414760) ENGINE = InnoDB,
PARTITION before20180306102040 VALUES LESS THAN (63687550840) ENGINE = InnoDB,
PARTITION before20180308000840 VALUES LESS THAN (63687686920) ENGINE = InnoDB,
PARTITION before20180309135640 VALUES LESS THAN (63687823000) ENGINE = InnoDB,
PARTITION before20180311034440 VALUES LESS THAN (63687959080) ENGINE = InnoDB,
PARTITION before20180312173240 VALUES LESS THAN (63688095160) ENGINE = InnoDB,
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
PARTITION before20180306193240 VALUES LESS THAN (63687583960) ENGINE = InnoDB,
PARTITION before20180306213240 VALUES LESS THAN (63687591160) ENGINE = InnoDB,
PARTITION before20180306233240 VALUES LESS THAN (63687598360) ENGINE = InnoDB,
PARTITION before20180307013240 VALUES LESS THAN (63687605560) ENGINE = InnoDB,
PARTITION before20180307033240 VALUES LESS THAN (63687612760) ENGINE = InnoDB,
PARTITION before20180307053240 VALUES LESS THAN (63687619960) ENGINE = InnoDB,
PARTITION before20180307073240 VALUES LESS THAN (63687627160) ENGINE = InnoDB,
PARTITION before20180307093240 VALUES LESS THAN (63687634360) ENGINE = InnoDB,
PARTITION before20180307113240 VALUES LESS THAN (63687641560) ENGINE = InnoDB,
PARTITION before20180307133240 VALUES LESS THAN (63687648760) ENGINE = InnoDB,
PARTITION before20180307153240 VALUES LESS THAN (63687655960) ENGINE = InnoDB,
PARTITION before20180307173240 VALUES LESS THAN (63687663160) ENGINE = InnoDB,
PARTITION before20180307193240 VALUES LESS THAN (63687670360) ENGINE = InnoDB,
PARTITION before20180307213240 VALUES LESS THAN (63687677560) ENGINE = InnoDB,
PARTITION before20180307233240 VALUES LESS THAN (63687684760) ENGINE = InnoDB,
PARTITION before20180308013240 VALUES LESS THAN (63687691960) ENGINE = InnoDB,
PARTITION before20180308033240 VALUES LESS THAN (63687699160) ENGINE = InnoDB,
PARTITION before20180308053240 VALUES LESS THAN (63687706360) ENGINE = InnoDB,
PARTITION before20180308073240 VALUES LESS THAN (63687713560) ENGINE = InnoDB,
PARTITION before20180308093240 VALUES LESS THAN (63687720760) ENGINE = InnoDB,
PARTITION before20180308113240 VALUES LESS THAN (63687727960) ENGINE = InnoDB,
PARTITION before20180308133240 VALUES LESS THAN (63687735160) ENGINE = InnoDB,
PARTITION before20180308153240 VALUES LESS THAN (63687742360) ENGINE = InnoDB,
PARTITION before20180308173240 VALUES LESS THAN (63687749560) ENGINE = InnoDB,
PARTITION before20180308193240 VALUES LESS THAN (63687756760) ENGINE = InnoDB,
PARTITION before20180308213240 VALUES LESS THAN (63687763960) ENGINE = InnoDB,
PARTITION before20180308233240 VALUES LESS THAN (63687771160) ENGINE = InnoDB,
PARTITION before20180309013240 VALUES LESS THAN (63687778360) ENGINE = InnoDB,
PARTITION before20180309033240 VALUES LESS THAN (63687785560) ENGINE = InnoDB,
PARTITION before20180309053240 VALUES LESS THAN (63687792760) ENGINE = InnoDB,
PARTITION before20180309073240 VALUES LESS THAN (63687799960) ENGINE = InnoDB,
PARTITION before20180309093240 VALUES LESS THAN (63687807160) ENGINE = InnoDB,
PARTITION before20180309113240 VALUES LESS THAN (63687814360) ENGINE = InnoDB,
PARTITION before20180309133240 VALUES LESS THAN (63687821560) ENGINE = InnoDB,
PARTITION before20180309153240 VALUES LESS THAN (63687828760) ENGINE = InnoDB,
PARTITION before20180309173240 VALUES LESS THAN (63687835960) ENGINE = InnoDB,
PARTITION before20180309193240 VALUES LESS THAN (63687843160) ENGINE = InnoDB,
PARTITION before20180309213240 VALUES LESS THAN (63687850360) ENGINE = InnoDB,
PARTITION before20180309233240 VALUES LESS THAN (63687857560) ENGINE = InnoDB,
PARTITION before20180310013240 VALUES LESS THAN (63687864760) ENGINE = InnoDB,
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
PARTITION before20160314094441 VALUES LESS THAN (63625167881) ENGINE = InnoDB,
PARTITION before20160402015641 VALUES LESS THAN (63626781401) ENGINE = InnoDB,
PARTITION before20160420180841 VALUES LESS THAN (63628394921) ENGINE = InnoDB,
PARTITION before20160509102041 VALUES LESS THAN (63630008441) ENGINE = InnoDB,
PARTITION before20160528023241 VALUES LESS THAN (63631621961) ENGINE = InnoDB,
PARTITION before20160615184441 VALUES LESS THAN (63633235481) ENGINE = InnoDB,
PARTITION before20160704105641 VALUES LESS THAN (63634849001) ENGINE = InnoDB,
PARTITION before20160723030841 VALUES LESS THAN (63636462521) ENGINE = InnoDB,
PARTITION before20160810192041 VALUES LESS THAN (63638076041) ENGINE = InnoDB,
PARTITION before20160829113241 VALUES LESS THAN (63639689561) ENGINE = InnoDB,
PARTITION before20160917034441 VALUES LESS THAN (63641303081) ENGINE = InnoDB,
PARTITION before20161005195641 VALUES LESS THAN (63642916601) ENGINE = InnoDB,
PARTITION before20161024120841 VALUES LESS THAN (63644530121) ENGINE = InnoDB,
PARTITION before20161112042041 VALUES LESS THAN (63646143641) ENGINE = InnoDB,
PARTITION before20161130203241 VALUES LESS THAN (63647757161) ENGINE = InnoDB,
PARTITION before20161219124441 VALUES LESS THAN (63649370681) ENGINE = InnoDB,
PARTITION before20170107045641 VALUES LESS THAN (63650984201) ENGINE = InnoDB,
PARTITION before20170125210841 VALUES LESS THAN (63652597721) ENGINE = InnoDB,
PARTITION before20170213132041 VALUES LESS THAN (63654211241) ENGINE = InnoDB,
PARTITION before20170304053241 VALUES LESS THAN (63655824761) ENGINE = InnoDB,
PARTITION before20170322214441 VALUES LESS THAN (63657438281) ENGINE = InnoDB,
PARTITION before20170410135641 VALUES LESS THAN (63659051801) ENGINE = InnoDB,
PARTITION before20170429060841 VALUES LESS THAN (63660665321) ENGINE = InnoDB,
PARTITION before20170517222041 VALUES LESS THAN (63662278841) ENGINE = InnoDB,
PARTITION before20170605143241 VALUES LESS THAN (63663892361) ENGINE = InnoDB,
PARTITION before20170624064441 VALUES LESS THAN (63665505881) ENGINE = InnoDB,
PARTITION before20170712225641 VALUES LESS THAN (63667119401) ENGINE = InnoDB,
PARTITION before20170731150841 VALUES LESS THAN (63668732921) ENGINE = InnoDB,
PARTITION before20170819072041 VALUES LESS THAN (63670346441) ENGINE = InnoDB,
PARTITION before20170906233241 VALUES LESS THAN (63671959961) ENGINE = InnoDB,
PARTITION before20170925154441 VALUES LESS THAN (63673573481) ENGINE = InnoDB,
PARTITION before20171014075641 VALUES LESS THAN (63675187001) ENGINE = InnoDB,
PARTITION before20171102000841 VALUES LESS THAN (63676800521) ENGINE = InnoDB,
PARTITION before20171120162041 VALUES LESS THAN (63678414041) ENGINE = InnoDB,
PARTITION before20171209083241 VALUES LESS THAN (63680027561) ENGINE = InnoDB,
PARTITION before20171228004441 VALUES LESS THAN (63681641081) ENGINE = InnoDB,
PARTITION before20180115165641 VALUES LESS THAN (63683254601) ENGINE = InnoDB,
PARTITION before20180203090841 VALUES LESS THAN (63684868121) ENGINE = InnoDB,
PARTITION before20180222012041 VALUES LESS THAN (63686481641) ENGINE = InnoDB,
PARTITION before20180312173241 VALUES LESS THAN (63688095161) ENGINE = InnoDB,
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
PARTITION before20180309153838 VALUES LESS THAN (63687829118) ENGINE = InnoDB,
PARTITION before20180309154438 VALUES LESS THAN (63687829478) ENGINE = InnoDB,
PARTITION before20180309155038 VALUES LESS THAN (63687829838) ENGINE = InnoDB,
PARTITION before20180309155638 VALUES LESS THAN (63687830198) ENGINE = InnoDB,
PARTITION before20180309160238 VALUES LESS THAN (63687830558) ENGINE = InnoDB,
PARTITION before20180309160838 VALUES LESS THAN (63687830918) ENGINE = InnoDB,
PARTITION before20180309161438 VALUES LESS THAN (63687831278) ENGINE = InnoDB,
PARTITION before20180309162038 VALUES LESS THAN (63687831638) ENGINE = InnoDB,
PARTITION before20180309162638 VALUES LESS THAN (63687831998) ENGINE = InnoDB,
PARTITION before20180309163238 VALUES LESS THAN (63687832358) ENGINE = InnoDB,
PARTITION before20180309163838 VALUES LESS THAN (63687832718) ENGINE = InnoDB,
PARTITION before20180309164438 VALUES LESS THAN (63687833078) ENGINE = InnoDB,
PARTITION before20180309165038 VALUES LESS THAN (63687833438) ENGINE = InnoDB,
PARTITION before20180309165638 VALUES LESS THAN (63687833798) ENGINE = InnoDB,
PARTITION before20180309170238 VALUES LESS THAN (63687834158) ENGINE = InnoDB,
PARTITION before20180309170838 VALUES LESS THAN (63687834518) ENGINE = InnoDB,
PARTITION before20180309171438 VALUES LESS THAN (63687834878) ENGINE = InnoDB,
PARTITION before20180309172038 VALUES LESS THAN (63687835238) ENGINE = InnoDB,
PARTITION before20180309172638 VALUES LESS THAN (63687835598) ENGINE = InnoDB,
PARTITION before20180309173238 VALUES LESS THAN (63687835958) ENGINE = InnoDB,
PARTITION before20180309173838 VALUES LESS THAN (63687836318) ENGINE = InnoDB,
PARTITION before20180309174438 VALUES LESS THAN (63687836678) ENGINE = InnoDB,
PARTITION before20180309175038 VALUES LESS THAN (63687837038) ENGINE = InnoDB,
PARTITION before20180309175638 VALUES LESS THAN (63687837398) ENGINE = InnoDB,
PARTITION before20180309180238 VALUES LESS THAN (63687837758) ENGINE = InnoDB,
PARTITION before20180309180838 VALUES LESS THAN (63687838118) ENGINE = InnoDB,
PARTITION before20180309181438 VALUES LESS THAN (63687838478) ENGINE = InnoDB,
PARTITION before20180309182038 VALUES LESS THAN (63687838838) ENGINE = InnoDB,
PARTITION before20180309182638 VALUES LESS THAN (63687839198) ENGINE = InnoDB,
PARTITION before20180309183238 VALUES LESS THAN (63687839558) ENGINE = InnoDB,
PARTITION before20180309183838 VALUES LESS THAN (63687839918) ENGINE = InnoDB,
PARTITION before20180309184438 VALUES LESS THAN (63687840278) ENGINE = InnoDB,
PARTITION before20180309185038 VALUES LESS THAN (63687840638) ENGINE = InnoDB,
PARTITION before20180309185638 VALUES LESS THAN (63687840998) ENGINE = InnoDB,
PARTITION before20180309190238 VALUES LESS THAN (63687841358) ENGINE = InnoDB,
PARTITION before20180309190838 VALUES LESS THAN (63687841718) ENGINE = InnoDB,
PARTITION before20180309191438 VALUES LESS THAN (63687842078) ENGINE = InnoDB,
PARTITION before20180309192038 VALUES LESS THAN (63687842438) ENGINE = InnoDB,
PARTITION before20180309192638 VALUES LESS THAN (63687842798) ENGINE = InnoDB,
PARTITION before20180309193238 VALUES LESS THAN (63687843158) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_cpod_primary_keys BEFORE INSERT ON cpod_stats_latest
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
PARTITION before20180110072044 VALUES LESS THAN (63682788044) ENGINE = InnoDB,
PARTITION before20180111210844 VALUES LESS THAN (63682924124) ENGINE = InnoDB,
PARTITION before20180113105644 VALUES LESS THAN (63683060204) ENGINE = InnoDB,
PARTITION before20180115004444 VALUES LESS THAN (63683196284) ENGINE = InnoDB,
PARTITION before20180116143244 VALUES LESS THAN (63683332364) ENGINE = InnoDB,
PARTITION before20180118042044 VALUES LESS THAN (63683468444) ENGINE = InnoDB,
PARTITION before20180119180844 VALUES LESS THAN (63683604524) ENGINE = InnoDB,
PARTITION before20180121075644 VALUES LESS THAN (63683740604) ENGINE = InnoDB,
PARTITION before20180122214444 VALUES LESS THAN (63683876684) ENGINE = InnoDB,
PARTITION before20180124113244 VALUES LESS THAN (63684012764) ENGINE = InnoDB,
PARTITION before20180126012044 VALUES LESS THAN (63684148844) ENGINE = InnoDB,
PARTITION before20180127150844 VALUES LESS THAN (63684284924) ENGINE = InnoDB,
PARTITION before20180129045644 VALUES LESS THAN (63684421004) ENGINE = InnoDB,
PARTITION before20180130184444 VALUES LESS THAN (63684557084) ENGINE = InnoDB,
PARTITION before20180201083244 VALUES LESS THAN (63684693164) ENGINE = InnoDB,
PARTITION before20180202222044 VALUES LESS THAN (63684829244) ENGINE = InnoDB,
PARTITION before20180204120844 VALUES LESS THAN (63684965324) ENGINE = InnoDB,
PARTITION before20180206015644 VALUES LESS THAN (63685101404) ENGINE = InnoDB,
PARTITION before20180207154444 VALUES LESS THAN (63685237484) ENGINE = InnoDB,
PARTITION before20180209053244 VALUES LESS THAN (63685373564) ENGINE = InnoDB,
PARTITION before20180210192044 VALUES LESS THAN (63685509644) ENGINE = InnoDB,
PARTITION before20180212090844 VALUES LESS THAN (63685645724) ENGINE = InnoDB,
PARTITION before20180213225644 VALUES LESS THAN (63685781804) ENGINE = InnoDB,
PARTITION before20180215124444 VALUES LESS THAN (63685917884) ENGINE = InnoDB,
PARTITION before20180217023244 VALUES LESS THAN (63686053964) ENGINE = InnoDB,
PARTITION before20180218162044 VALUES LESS THAN (63686190044) ENGINE = InnoDB,
PARTITION before20180220060844 VALUES LESS THAN (63686326124) ENGINE = InnoDB,
PARTITION before20180221195644 VALUES LESS THAN (63686462204) ENGINE = InnoDB,
PARTITION before20180223094444 VALUES LESS THAN (63686598284) ENGINE = InnoDB,
PARTITION before20180224233244 VALUES LESS THAN (63686734364) ENGINE = InnoDB,
PARTITION before20180226132044 VALUES LESS THAN (63686870444) ENGINE = InnoDB,
PARTITION before20180228030844 VALUES LESS THAN (63687006524) ENGINE = InnoDB,
PARTITION before20180301165644 VALUES LESS THAN (63687142604) ENGINE = InnoDB,
PARTITION before20180303064444 VALUES LESS THAN (63687278684) ENGINE = InnoDB,
PARTITION before20180304203244 VALUES LESS THAN (63687414764) ENGINE = InnoDB,
PARTITION before20180306102044 VALUES LESS THAN (63687550844) ENGINE = InnoDB,
PARTITION before20180308000844 VALUES LESS THAN (63687686924) ENGINE = InnoDB,
PARTITION before20180309135644 VALUES LESS THAN (63687823004) ENGINE = InnoDB,
PARTITION before20180311034444 VALUES LESS THAN (63687959084) ENGINE = InnoDB,
PARTITION before20180312173244 VALUES LESS THAN (63688095164) ENGINE = InnoDB,
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
PARTITION before20180306193251 VALUES LESS THAN (63687583971) ENGINE = InnoDB,
PARTITION before20180306213251 VALUES LESS THAN (63687591171) ENGINE = InnoDB,
PARTITION before20180306233251 VALUES LESS THAN (63687598371) ENGINE = InnoDB,
PARTITION before20180307013251 VALUES LESS THAN (63687605571) ENGINE = InnoDB,
PARTITION before20180307033251 VALUES LESS THAN (63687612771) ENGINE = InnoDB,
PARTITION before20180307053251 VALUES LESS THAN (63687619971) ENGINE = InnoDB,
PARTITION before20180307073251 VALUES LESS THAN (63687627171) ENGINE = InnoDB,
PARTITION before20180307093251 VALUES LESS THAN (63687634371) ENGINE = InnoDB,
PARTITION before20180307113251 VALUES LESS THAN (63687641571) ENGINE = InnoDB,
PARTITION before20180307133251 VALUES LESS THAN (63687648771) ENGINE = InnoDB,
PARTITION before20180307153251 VALUES LESS THAN (63687655971) ENGINE = InnoDB,
PARTITION before20180307173251 VALUES LESS THAN (63687663171) ENGINE = InnoDB,
PARTITION before20180307193251 VALUES LESS THAN (63687670371) ENGINE = InnoDB,
PARTITION before20180307213251 VALUES LESS THAN (63687677571) ENGINE = InnoDB,
PARTITION before20180307233251 VALUES LESS THAN (63687684771) ENGINE = InnoDB,
PARTITION before20180308013251 VALUES LESS THAN (63687691971) ENGINE = InnoDB,
PARTITION before20180308033251 VALUES LESS THAN (63687699171) ENGINE = InnoDB,
PARTITION before20180308053251 VALUES LESS THAN (63687706371) ENGINE = InnoDB,
PARTITION before20180308073251 VALUES LESS THAN (63687713571) ENGINE = InnoDB,
PARTITION before20180308093251 VALUES LESS THAN (63687720771) ENGINE = InnoDB,
PARTITION before20180308113251 VALUES LESS THAN (63687727971) ENGINE = InnoDB,
PARTITION before20180308133251 VALUES LESS THAN (63687735171) ENGINE = InnoDB,
PARTITION before20180308153251 VALUES LESS THAN (63687742371) ENGINE = InnoDB,
PARTITION before20180308173251 VALUES LESS THAN (63687749571) ENGINE = InnoDB,
PARTITION before20180308193251 VALUES LESS THAN (63687756771) ENGINE = InnoDB,
PARTITION before20180308213251 VALUES LESS THAN (63687763971) ENGINE = InnoDB,
PARTITION before20180308233251 VALUES LESS THAN (63687771171) ENGINE = InnoDB,
PARTITION before20180309013251 VALUES LESS THAN (63687778371) ENGINE = InnoDB,
PARTITION before20180309033251 VALUES LESS THAN (63687785571) ENGINE = InnoDB,
PARTITION before20180309053251 VALUES LESS THAN (63687792771) ENGINE = InnoDB,
PARTITION before20180309073251 VALUES LESS THAN (63687799971) ENGINE = InnoDB,
PARTITION before20180309093251 VALUES LESS THAN (63687807171) ENGINE = InnoDB,
PARTITION before20180309113251 VALUES LESS THAN (63687814371) ENGINE = InnoDB,
PARTITION before20180309133251 VALUES LESS THAN (63687821571) ENGINE = InnoDB,
PARTITION before20180309153251 VALUES LESS THAN (63687828771) ENGINE = InnoDB,
PARTITION before20180309173251 VALUES LESS THAN (63687835971) ENGINE = InnoDB,
PARTITION before20180309193251 VALUES LESS THAN (63687843171) ENGINE = InnoDB,
PARTITION before20180309213251 VALUES LESS THAN (63687850371) ENGINE = InnoDB,
PARTITION before20180309233251 VALUES LESS THAN (63687857571) ENGINE = InnoDB,
PARTITION before20180310013251 VALUES LESS THAN (63687864771) ENGINE = InnoDB,
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
PARTITION before20160314094452 VALUES LESS THAN (63625167892) ENGINE = InnoDB,
PARTITION before20160402015652 VALUES LESS THAN (63626781412) ENGINE = InnoDB,
PARTITION before20160420180852 VALUES LESS THAN (63628394932) ENGINE = InnoDB,
PARTITION before20160509102052 VALUES LESS THAN (63630008452) ENGINE = InnoDB,
PARTITION before20160528023252 VALUES LESS THAN (63631621972) ENGINE = InnoDB,
PARTITION before20160615184452 VALUES LESS THAN (63633235492) ENGINE = InnoDB,
PARTITION before20160704105652 VALUES LESS THAN (63634849012) ENGINE = InnoDB,
PARTITION before20160723030852 VALUES LESS THAN (63636462532) ENGINE = InnoDB,
PARTITION before20160810192052 VALUES LESS THAN (63638076052) ENGINE = InnoDB,
PARTITION before20160829113252 VALUES LESS THAN (63639689572) ENGINE = InnoDB,
PARTITION before20160917034452 VALUES LESS THAN (63641303092) ENGINE = InnoDB,
PARTITION before20161005195652 VALUES LESS THAN (63642916612) ENGINE = InnoDB,
PARTITION before20161024120852 VALUES LESS THAN (63644530132) ENGINE = InnoDB,
PARTITION before20161112042052 VALUES LESS THAN (63646143652) ENGINE = InnoDB,
PARTITION before20161130203252 VALUES LESS THAN (63647757172) ENGINE = InnoDB,
PARTITION before20161219124452 VALUES LESS THAN (63649370692) ENGINE = InnoDB,
PARTITION before20170107045652 VALUES LESS THAN (63650984212) ENGINE = InnoDB,
PARTITION before20170125210852 VALUES LESS THAN (63652597732) ENGINE = InnoDB,
PARTITION before20170213132052 VALUES LESS THAN (63654211252) ENGINE = InnoDB,
PARTITION before20170304053252 VALUES LESS THAN (63655824772) ENGINE = InnoDB,
PARTITION before20170322214452 VALUES LESS THAN (63657438292) ENGINE = InnoDB,
PARTITION before20170410135652 VALUES LESS THAN (63659051812) ENGINE = InnoDB,
PARTITION before20170429060852 VALUES LESS THAN (63660665332) ENGINE = InnoDB,
PARTITION before20170517222052 VALUES LESS THAN (63662278852) ENGINE = InnoDB,
PARTITION before20170605143252 VALUES LESS THAN (63663892372) ENGINE = InnoDB,
PARTITION before20170624064452 VALUES LESS THAN (63665505892) ENGINE = InnoDB,
PARTITION before20170712225652 VALUES LESS THAN (63667119412) ENGINE = InnoDB,
PARTITION before20170731150852 VALUES LESS THAN (63668732932) ENGINE = InnoDB,
PARTITION before20170819072052 VALUES LESS THAN (63670346452) ENGINE = InnoDB,
PARTITION before20170906233252 VALUES LESS THAN (63671959972) ENGINE = InnoDB,
PARTITION before20170925154452 VALUES LESS THAN (63673573492) ENGINE = InnoDB,
PARTITION before20171014075652 VALUES LESS THAN (63675187012) ENGINE = InnoDB,
PARTITION before20171102000852 VALUES LESS THAN (63676800532) ENGINE = InnoDB,
PARTITION before20171120162052 VALUES LESS THAN (63678414052) ENGINE = InnoDB,
PARTITION before20171209083252 VALUES LESS THAN (63680027572) ENGINE = InnoDB,
PARTITION before20171228004452 VALUES LESS THAN (63681641092) ENGINE = InnoDB,
PARTITION before20180115165652 VALUES LESS THAN (63683254612) ENGINE = InnoDB,
PARTITION before20180203090852 VALUES LESS THAN (63684868132) ENGINE = InnoDB,
PARTITION before20180222012052 VALUES LESS THAN (63686481652) ENGINE = InnoDB,
PARTITION before20180312173252 VALUES LESS THAN (63688095172) ENGINE = InnoDB,
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
PARTITION before20180309153842 VALUES LESS THAN (63687829122) ENGINE = InnoDB,
PARTITION before20180309154442 VALUES LESS THAN (63687829482) ENGINE = InnoDB,
PARTITION before20180309155042 VALUES LESS THAN (63687829842) ENGINE = InnoDB,
PARTITION before20180309155642 VALUES LESS THAN (63687830202) ENGINE = InnoDB,
PARTITION before20180309160242 VALUES LESS THAN (63687830562) ENGINE = InnoDB,
PARTITION before20180309160842 VALUES LESS THAN (63687830922) ENGINE = InnoDB,
PARTITION before20180309161442 VALUES LESS THAN (63687831282) ENGINE = InnoDB,
PARTITION before20180309162042 VALUES LESS THAN (63687831642) ENGINE = InnoDB,
PARTITION before20180309162642 VALUES LESS THAN (63687832002) ENGINE = InnoDB,
PARTITION before20180309163242 VALUES LESS THAN (63687832362) ENGINE = InnoDB,
PARTITION before20180309163842 VALUES LESS THAN (63687832722) ENGINE = InnoDB,
PARTITION before20180309164442 VALUES LESS THAN (63687833082) ENGINE = InnoDB,
PARTITION before20180309165042 VALUES LESS THAN (63687833442) ENGINE = InnoDB,
PARTITION before20180309165642 VALUES LESS THAN (63687833802) ENGINE = InnoDB,
PARTITION before20180309170242 VALUES LESS THAN (63687834162) ENGINE = InnoDB,
PARTITION before20180309170842 VALUES LESS THAN (63687834522) ENGINE = InnoDB,
PARTITION before20180309171442 VALUES LESS THAN (63687834882) ENGINE = InnoDB,
PARTITION before20180309172042 VALUES LESS THAN (63687835242) ENGINE = InnoDB,
PARTITION before20180309172642 VALUES LESS THAN (63687835602) ENGINE = InnoDB,
PARTITION before20180309173242 VALUES LESS THAN (63687835962) ENGINE = InnoDB,
PARTITION before20180309173842 VALUES LESS THAN (63687836322) ENGINE = InnoDB,
PARTITION before20180309174442 VALUES LESS THAN (63687836682) ENGINE = InnoDB,
PARTITION before20180309175042 VALUES LESS THAN (63687837042) ENGINE = InnoDB,
PARTITION before20180309175642 VALUES LESS THAN (63687837402) ENGINE = InnoDB,
PARTITION before20180309180242 VALUES LESS THAN (63687837762) ENGINE = InnoDB,
PARTITION before20180309180842 VALUES LESS THAN (63687838122) ENGINE = InnoDB,
PARTITION before20180309181442 VALUES LESS THAN (63687838482) ENGINE = InnoDB,
PARTITION before20180309182042 VALUES LESS THAN (63687838842) ENGINE = InnoDB,
PARTITION before20180309182642 VALUES LESS THAN (63687839202) ENGINE = InnoDB,
PARTITION before20180309183242 VALUES LESS THAN (63687839562) ENGINE = InnoDB,
PARTITION before20180309183842 VALUES LESS THAN (63687839922) ENGINE = InnoDB,
PARTITION before20180309184442 VALUES LESS THAN (63687840282) ENGINE = InnoDB,
PARTITION before20180309185042 VALUES LESS THAN (63687840642) ENGINE = InnoDB,
PARTITION before20180309185642 VALUES LESS THAN (63687841002) ENGINE = InnoDB,
PARTITION before20180309190242 VALUES LESS THAN (63687841362) ENGINE = InnoDB,
PARTITION before20180309190842 VALUES LESS THAN (63687841722) ENGINE = InnoDB,
PARTITION before20180309191442 VALUES LESS THAN (63687842082) ENGINE = InnoDB,
PARTITION before20180309192042 VALUES LESS THAN (63687842442) ENGINE = InnoDB,
PARTITION before20180309192642 VALUES LESS THAN (63687842802) ENGINE = InnoDB,
PARTITION before20180309193242 VALUES LESS THAN (63687843162) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_da_primary_keys BEFORE INSERT ON da_stats_latest
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
PARTITION before20180110071318 VALUES LESS THAN (63682787598) ENGINE = InnoDB,
PARTITION before20180111210118 VALUES LESS THAN (63682923678) ENGINE = InnoDB,
PARTITION before20180113104918 VALUES LESS THAN (63683059758) ENGINE = InnoDB,
PARTITION before20180115003718 VALUES LESS THAN (63683195838) ENGINE = InnoDB,
PARTITION before20180116142518 VALUES LESS THAN (63683331918) ENGINE = InnoDB,
PARTITION before20180118041318 VALUES LESS THAN (63683467998) ENGINE = InnoDB,
PARTITION before20180119180118 VALUES LESS THAN (63683604078) ENGINE = InnoDB,
PARTITION before20180121074918 VALUES LESS THAN (63683740158) ENGINE = InnoDB,
PARTITION before20180122213718 VALUES LESS THAN (63683876238) ENGINE = InnoDB,
PARTITION before20180124112518 VALUES LESS THAN (63684012318) ENGINE = InnoDB,
PARTITION before20180126011318 VALUES LESS THAN (63684148398) ENGINE = InnoDB,
PARTITION before20180127150118 VALUES LESS THAN (63684284478) ENGINE = InnoDB,
PARTITION before20180129044918 VALUES LESS THAN (63684420558) ENGINE = InnoDB,
PARTITION before20180130183718 VALUES LESS THAN (63684556638) ENGINE = InnoDB,
PARTITION before20180201082518 VALUES LESS THAN (63684692718) ENGINE = InnoDB,
PARTITION before20180202221318 VALUES LESS THAN (63684828798) ENGINE = InnoDB,
PARTITION before20180204120118 VALUES LESS THAN (63684964878) ENGINE = InnoDB,
PARTITION before20180206014918 VALUES LESS THAN (63685100958) ENGINE = InnoDB,
PARTITION before20180207153718 VALUES LESS THAN (63685237038) ENGINE = InnoDB,
PARTITION before20180209052518 VALUES LESS THAN (63685373118) ENGINE = InnoDB,
PARTITION before20180210191318 VALUES LESS THAN (63685509198) ENGINE = InnoDB,
PARTITION before20180212090118 VALUES LESS THAN (63685645278) ENGINE = InnoDB,
PARTITION before20180213224918 VALUES LESS THAN (63685781358) ENGINE = InnoDB,
PARTITION before20180215123718 VALUES LESS THAN (63685917438) ENGINE = InnoDB,
PARTITION before20180217022518 VALUES LESS THAN (63686053518) ENGINE = InnoDB,
PARTITION before20180218161318 VALUES LESS THAN (63686189598) ENGINE = InnoDB,
PARTITION before20180220060118 VALUES LESS THAN (63686325678) ENGINE = InnoDB,
PARTITION before20180221194918 VALUES LESS THAN (63686461758) ENGINE = InnoDB,
PARTITION before20180223093718 VALUES LESS THAN (63686597838) ENGINE = InnoDB,
PARTITION before20180224232518 VALUES LESS THAN (63686733918) ENGINE = InnoDB,
PARTITION before20180226131318 VALUES LESS THAN (63686869998) ENGINE = InnoDB,
PARTITION before20180228030118 VALUES LESS THAN (63687006078) ENGINE = InnoDB,
PARTITION before20180301164918 VALUES LESS THAN (63687142158) ENGINE = InnoDB,
PARTITION before20180303063718 VALUES LESS THAN (63687278238) ENGINE = InnoDB,
PARTITION before20180304202518 VALUES LESS THAN (63687414318) ENGINE = InnoDB,
PARTITION before20180306101318 VALUES LESS THAN (63687550398) ENGINE = InnoDB,
PARTITION before20180308000118 VALUES LESS THAN (63687686478) ENGINE = InnoDB,
PARTITION before20180309134918 VALUES LESS THAN (63687822558) ENGINE = InnoDB,
PARTITION before20180311033718 VALUES LESS THAN (63687958638) ENGINE = InnoDB,
PARTITION before20180312172518 VALUES LESS THAN (63688094718) ENGINE = InnoDB,
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
PARTITION before20180306192519 VALUES LESS THAN (63687583519) ENGINE = InnoDB,
PARTITION before20180306212519 VALUES LESS THAN (63687590719) ENGINE = InnoDB,
PARTITION before20180306232519 VALUES LESS THAN (63687597919) ENGINE = InnoDB,
PARTITION before20180307012519 VALUES LESS THAN (63687605119) ENGINE = InnoDB,
PARTITION before20180307032519 VALUES LESS THAN (63687612319) ENGINE = InnoDB,
PARTITION before20180307052519 VALUES LESS THAN (63687619519) ENGINE = InnoDB,
PARTITION before20180307072519 VALUES LESS THAN (63687626719) ENGINE = InnoDB,
PARTITION before20180307092519 VALUES LESS THAN (63687633919) ENGINE = InnoDB,
PARTITION before20180307112519 VALUES LESS THAN (63687641119) ENGINE = InnoDB,
PARTITION before20180307132519 VALUES LESS THAN (63687648319) ENGINE = InnoDB,
PARTITION before20180307152519 VALUES LESS THAN (63687655519) ENGINE = InnoDB,
PARTITION before20180307172519 VALUES LESS THAN (63687662719) ENGINE = InnoDB,
PARTITION before20180307192519 VALUES LESS THAN (63687669919) ENGINE = InnoDB,
PARTITION before20180307212519 VALUES LESS THAN (63687677119) ENGINE = InnoDB,
PARTITION before20180307232519 VALUES LESS THAN (63687684319) ENGINE = InnoDB,
PARTITION before20180308012519 VALUES LESS THAN (63687691519) ENGINE = InnoDB,
PARTITION before20180308032519 VALUES LESS THAN (63687698719) ENGINE = InnoDB,
PARTITION before20180308052519 VALUES LESS THAN (63687705919) ENGINE = InnoDB,
PARTITION before20180308072519 VALUES LESS THAN (63687713119) ENGINE = InnoDB,
PARTITION before20180308092519 VALUES LESS THAN (63687720319) ENGINE = InnoDB,
PARTITION before20180308112519 VALUES LESS THAN (63687727519) ENGINE = InnoDB,
PARTITION before20180308132519 VALUES LESS THAN (63687734719) ENGINE = InnoDB,
PARTITION before20180308152519 VALUES LESS THAN (63687741919) ENGINE = InnoDB,
PARTITION before20180308172519 VALUES LESS THAN (63687749119) ENGINE = InnoDB,
PARTITION before20180308192519 VALUES LESS THAN (63687756319) ENGINE = InnoDB,
PARTITION before20180308212519 VALUES LESS THAN (63687763519) ENGINE = InnoDB,
PARTITION before20180308232519 VALUES LESS THAN (63687770719) ENGINE = InnoDB,
PARTITION before20180309012519 VALUES LESS THAN (63687777919) ENGINE = InnoDB,
PARTITION before20180309032519 VALUES LESS THAN (63687785119) ENGINE = InnoDB,
PARTITION before20180309052519 VALUES LESS THAN (63687792319) ENGINE = InnoDB,
PARTITION before20180309072519 VALUES LESS THAN (63687799519) ENGINE = InnoDB,
PARTITION before20180309092519 VALUES LESS THAN (63687806719) ENGINE = InnoDB,
PARTITION before20180309112519 VALUES LESS THAN (63687813919) ENGINE = InnoDB,
PARTITION before20180309132519 VALUES LESS THAN (63687821119) ENGINE = InnoDB,
PARTITION before20180309152519 VALUES LESS THAN (63687828319) ENGINE = InnoDB,
PARTITION before20180309172519 VALUES LESS THAN (63687835519) ENGINE = InnoDB,
PARTITION before20180309192519 VALUES LESS THAN (63687842719) ENGINE = InnoDB,
PARTITION before20180309212519 VALUES LESS THAN (63687849919) ENGINE = InnoDB,
PARTITION before20180309232519 VALUES LESS THAN (63687857119) ENGINE = InnoDB,
PARTITION before20180310012519 VALUES LESS THAN (63687864319) ENGINE = InnoDB,
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
PARTITION before20160314093719 VALUES LESS THAN (63625167439) ENGINE = InnoDB,
PARTITION before20160402014919 VALUES LESS THAN (63626780959) ENGINE = InnoDB,
PARTITION before20160420180119 VALUES LESS THAN (63628394479) ENGINE = InnoDB,
PARTITION before20160509101319 VALUES LESS THAN (63630007999) ENGINE = InnoDB,
PARTITION before20160528022519 VALUES LESS THAN (63631621519) ENGINE = InnoDB,
PARTITION before20160615183719 VALUES LESS THAN (63633235039) ENGINE = InnoDB,
PARTITION before20160704104919 VALUES LESS THAN (63634848559) ENGINE = InnoDB,
PARTITION before20160723030119 VALUES LESS THAN (63636462079) ENGINE = InnoDB,
PARTITION before20160810191319 VALUES LESS THAN (63638075599) ENGINE = InnoDB,
PARTITION before20160829112519 VALUES LESS THAN (63639689119) ENGINE = InnoDB,
PARTITION before20160917033719 VALUES LESS THAN (63641302639) ENGINE = InnoDB,
PARTITION before20161005194919 VALUES LESS THAN (63642916159) ENGINE = InnoDB,
PARTITION before20161024120119 VALUES LESS THAN (63644529679) ENGINE = InnoDB,
PARTITION before20161112041319 VALUES LESS THAN (63646143199) ENGINE = InnoDB,
PARTITION before20161130202519 VALUES LESS THAN (63647756719) ENGINE = InnoDB,
PARTITION before20161219123719 VALUES LESS THAN (63649370239) ENGINE = InnoDB,
PARTITION before20170107044919 VALUES LESS THAN (63650983759) ENGINE = InnoDB,
PARTITION before20170125210119 VALUES LESS THAN (63652597279) ENGINE = InnoDB,
PARTITION before20170213131319 VALUES LESS THAN (63654210799) ENGINE = InnoDB,
PARTITION before20170304052519 VALUES LESS THAN (63655824319) ENGINE = InnoDB,
PARTITION before20170322213719 VALUES LESS THAN (63657437839) ENGINE = InnoDB,
PARTITION before20170410134919 VALUES LESS THAN (63659051359) ENGINE = InnoDB,
PARTITION before20170429060119 VALUES LESS THAN (63660664879) ENGINE = InnoDB,
PARTITION before20170517221319 VALUES LESS THAN (63662278399) ENGINE = InnoDB,
PARTITION before20170605142519 VALUES LESS THAN (63663891919) ENGINE = InnoDB,
PARTITION before20170624063719 VALUES LESS THAN (63665505439) ENGINE = InnoDB,
PARTITION before20170712224919 VALUES LESS THAN (63667118959) ENGINE = InnoDB,
PARTITION before20170731150119 VALUES LESS THAN (63668732479) ENGINE = InnoDB,
PARTITION before20170819071319 VALUES LESS THAN (63670345999) ENGINE = InnoDB,
PARTITION before20170906232519 VALUES LESS THAN (63671959519) ENGINE = InnoDB,
PARTITION before20170925153719 VALUES LESS THAN (63673573039) ENGINE = InnoDB,
PARTITION before20171014074919 VALUES LESS THAN (63675186559) ENGINE = InnoDB,
PARTITION before20171102000119 VALUES LESS THAN (63676800079) ENGINE = InnoDB,
PARTITION before20171120161319 VALUES LESS THAN (63678413599) ENGINE = InnoDB,
PARTITION before20171209082519 VALUES LESS THAN (63680027119) ENGINE = InnoDB,
PARTITION before20171228003719 VALUES LESS THAN (63681640639) ENGINE = InnoDB,
PARTITION before20180115164919 VALUES LESS THAN (63683254159) ENGINE = InnoDB,
PARTITION before20180203090119 VALUES LESS THAN (63684867679) ENGINE = InnoDB,
PARTITION before20180222011319 VALUES LESS THAN (63686481199) ENGINE = InnoDB,
PARTITION before20180312172519 VALUES LESS THAN (63688094719) ENGINE = InnoDB,
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
PARTITION before20180309153717 VALUES LESS THAN (63687829037) ENGINE = InnoDB,
PARTITION before20180309154317 VALUES LESS THAN (63687829397) ENGINE = InnoDB,
PARTITION before20180309154917 VALUES LESS THAN (63687829757) ENGINE = InnoDB,
PARTITION before20180309155517 VALUES LESS THAN (63687830117) ENGINE = InnoDB,
PARTITION before20180309160117 VALUES LESS THAN (63687830477) ENGINE = InnoDB,
PARTITION before20180309160717 VALUES LESS THAN (63687830837) ENGINE = InnoDB,
PARTITION before20180309161317 VALUES LESS THAN (63687831197) ENGINE = InnoDB,
PARTITION before20180309161917 VALUES LESS THAN (63687831557) ENGINE = InnoDB,
PARTITION before20180309162517 VALUES LESS THAN (63687831917) ENGINE = InnoDB,
PARTITION before20180309163117 VALUES LESS THAN (63687832277) ENGINE = InnoDB,
PARTITION before20180309163717 VALUES LESS THAN (63687832637) ENGINE = InnoDB,
PARTITION before20180309164317 VALUES LESS THAN (63687832997) ENGINE = InnoDB,
PARTITION before20180309164917 VALUES LESS THAN (63687833357) ENGINE = InnoDB,
PARTITION before20180309165517 VALUES LESS THAN (63687833717) ENGINE = InnoDB,
PARTITION before20180309170117 VALUES LESS THAN (63687834077) ENGINE = InnoDB,
PARTITION before20180309170717 VALUES LESS THAN (63687834437) ENGINE = InnoDB,
PARTITION before20180309171317 VALUES LESS THAN (63687834797) ENGINE = InnoDB,
PARTITION before20180309171917 VALUES LESS THAN (63687835157) ENGINE = InnoDB,
PARTITION before20180309172517 VALUES LESS THAN (63687835517) ENGINE = InnoDB,
PARTITION before20180309173117 VALUES LESS THAN (63687835877) ENGINE = InnoDB,
PARTITION before20180309173717 VALUES LESS THAN (63687836237) ENGINE = InnoDB,
PARTITION before20180309174317 VALUES LESS THAN (63687836597) ENGINE = InnoDB,
PARTITION before20180309174917 VALUES LESS THAN (63687836957) ENGINE = InnoDB,
PARTITION before20180309175517 VALUES LESS THAN (63687837317) ENGINE = InnoDB,
PARTITION before20180309180117 VALUES LESS THAN (63687837677) ENGINE = InnoDB,
PARTITION before20180309180717 VALUES LESS THAN (63687838037) ENGINE = InnoDB,
PARTITION before20180309181317 VALUES LESS THAN (63687838397) ENGINE = InnoDB,
PARTITION before20180309181917 VALUES LESS THAN (63687838757) ENGINE = InnoDB,
PARTITION before20180309182517 VALUES LESS THAN (63687839117) ENGINE = InnoDB,
PARTITION before20180309183117 VALUES LESS THAN (63687839477) ENGINE = InnoDB,
PARTITION before20180309183717 VALUES LESS THAN (63687839837) ENGINE = InnoDB,
PARTITION before20180309184317 VALUES LESS THAN (63687840197) ENGINE = InnoDB,
PARTITION before20180309184917 VALUES LESS THAN (63687840557) ENGINE = InnoDB,
PARTITION before20180309185517 VALUES LESS THAN (63687840917) ENGINE = InnoDB,
PARTITION before20180309190117 VALUES LESS THAN (63687841277) ENGINE = InnoDB,
PARTITION before20180309190717 VALUES LESS THAN (63687841637) ENGINE = InnoDB,
PARTITION before20180309191317 VALUES LESS THAN (63687841997) ENGINE = InnoDB,
PARTITION before20180309191917 VALUES LESS THAN (63687842357) ENGINE = InnoDB,
PARTITION before20180309192517 VALUES LESS THAN (63687842717) ENGINE = InnoDB,
PARTITION before20180309193117 VALUES LESS THAN (63687843077) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_dpod_primary_keys BEFORE INSERT ON dpod_stats_latest
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
PARTITION before20180110071321 VALUES LESS THAN (63682787601) ENGINE = InnoDB,
PARTITION before20180111210121 VALUES LESS THAN (63682923681) ENGINE = InnoDB,
PARTITION before20180113104921 VALUES LESS THAN (63683059761) ENGINE = InnoDB,
PARTITION before20180115003721 VALUES LESS THAN (63683195841) ENGINE = InnoDB,
PARTITION before20180116142521 VALUES LESS THAN (63683331921) ENGINE = InnoDB,
PARTITION before20180118041321 VALUES LESS THAN (63683468001) ENGINE = InnoDB,
PARTITION before20180119180121 VALUES LESS THAN (63683604081) ENGINE = InnoDB,
PARTITION before20180121074921 VALUES LESS THAN (63683740161) ENGINE = InnoDB,
PARTITION before20180122213721 VALUES LESS THAN (63683876241) ENGINE = InnoDB,
PARTITION before20180124112521 VALUES LESS THAN (63684012321) ENGINE = InnoDB,
PARTITION before20180126011321 VALUES LESS THAN (63684148401) ENGINE = InnoDB,
PARTITION before20180127150121 VALUES LESS THAN (63684284481) ENGINE = InnoDB,
PARTITION before20180129044921 VALUES LESS THAN (63684420561) ENGINE = InnoDB,
PARTITION before20180130183721 VALUES LESS THAN (63684556641) ENGINE = InnoDB,
PARTITION before20180201082521 VALUES LESS THAN (63684692721) ENGINE = InnoDB,
PARTITION before20180202221321 VALUES LESS THAN (63684828801) ENGINE = InnoDB,
PARTITION before20180204120121 VALUES LESS THAN (63684964881) ENGINE = InnoDB,
PARTITION before20180206014921 VALUES LESS THAN (63685100961) ENGINE = InnoDB,
PARTITION before20180207153721 VALUES LESS THAN (63685237041) ENGINE = InnoDB,
PARTITION before20180209052521 VALUES LESS THAN (63685373121) ENGINE = InnoDB,
PARTITION before20180210191321 VALUES LESS THAN (63685509201) ENGINE = InnoDB,
PARTITION before20180212090121 VALUES LESS THAN (63685645281) ENGINE = InnoDB,
PARTITION before20180213224921 VALUES LESS THAN (63685781361) ENGINE = InnoDB,
PARTITION before20180215123721 VALUES LESS THAN (63685917441) ENGINE = InnoDB,
PARTITION before20180217022521 VALUES LESS THAN (63686053521) ENGINE = InnoDB,
PARTITION before20180218161321 VALUES LESS THAN (63686189601) ENGINE = InnoDB,
PARTITION before20180220060121 VALUES LESS THAN (63686325681) ENGINE = InnoDB,
PARTITION before20180221194921 VALUES LESS THAN (63686461761) ENGINE = InnoDB,
PARTITION before20180223093721 VALUES LESS THAN (63686597841) ENGINE = InnoDB,
PARTITION before20180224232521 VALUES LESS THAN (63686733921) ENGINE = InnoDB,
PARTITION before20180226131321 VALUES LESS THAN (63686870001) ENGINE = InnoDB,
PARTITION before20180228030121 VALUES LESS THAN (63687006081) ENGINE = InnoDB,
PARTITION before20180301164921 VALUES LESS THAN (63687142161) ENGINE = InnoDB,
PARTITION before20180303063721 VALUES LESS THAN (63687278241) ENGINE = InnoDB,
PARTITION before20180304202521 VALUES LESS THAN (63687414321) ENGINE = InnoDB,
PARTITION before20180306101321 VALUES LESS THAN (63687550401) ENGINE = InnoDB,
PARTITION before20180308000121 VALUES LESS THAN (63687686481) ENGINE = InnoDB,
PARTITION before20180309134921 VALUES LESS THAN (63687822561) ENGINE = InnoDB,
PARTITION before20180311033721 VALUES LESS THAN (63687958641) ENGINE = InnoDB,
PARTITION before20180312172521 VALUES LESS THAN (63688094721) ENGINE = InnoDB,
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
PARTITION before20180306192522 VALUES LESS THAN (63687583522) ENGINE = InnoDB,
PARTITION before20180306212522 VALUES LESS THAN (63687590722) ENGINE = InnoDB,
PARTITION before20180306232522 VALUES LESS THAN (63687597922) ENGINE = InnoDB,
PARTITION before20180307012522 VALUES LESS THAN (63687605122) ENGINE = InnoDB,
PARTITION before20180307032522 VALUES LESS THAN (63687612322) ENGINE = InnoDB,
PARTITION before20180307052522 VALUES LESS THAN (63687619522) ENGINE = InnoDB,
PARTITION before20180307072522 VALUES LESS THAN (63687626722) ENGINE = InnoDB,
PARTITION before20180307092522 VALUES LESS THAN (63687633922) ENGINE = InnoDB,
PARTITION before20180307112522 VALUES LESS THAN (63687641122) ENGINE = InnoDB,
PARTITION before20180307132522 VALUES LESS THAN (63687648322) ENGINE = InnoDB,
PARTITION before20180307152522 VALUES LESS THAN (63687655522) ENGINE = InnoDB,
PARTITION before20180307172522 VALUES LESS THAN (63687662722) ENGINE = InnoDB,
PARTITION before20180307192522 VALUES LESS THAN (63687669922) ENGINE = InnoDB,
PARTITION before20180307212522 VALUES LESS THAN (63687677122) ENGINE = InnoDB,
PARTITION before20180307232522 VALUES LESS THAN (63687684322) ENGINE = InnoDB,
PARTITION before20180308012522 VALUES LESS THAN (63687691522) ENGINE = InnoDB,
PARTITION before20180308032522 VALUES LESS THAN (63687698722) ENGINE = InnoDB,
PARTITION before20180308052522 VALUES LESS THAN (63687705922) ENGINE = InnoDB,
PARTITION before20180308072522 VALUES LESS THAN (63687713122) ENGINE = InnoDB,
PARTITION before20180308092522 VALUES LESS THAN (63687720322) ENGINE = InnoDB,
PARTITION before20180308112522 VALUES LESS THAN (63687727522) ENGINE = InnoDB,
PARTITION before20180308132522 VALUES LESS THAN (63687734722) ENGINE = InnoDB,
PARTITION before20180308152522 VALUES LESS THAN (63687741922) ENGINE = InnoDB,
PARTITION before20180308172522 VALUES LESS THAN (63687749122) ENGINE = InnoDB,
PARTITION before20180308192522 VALUES LESS THAN (63687756322) ENGINE = InnoDB,
PARTITION before20180308212522 VALUES LESS THAN (63687763522) ENGINE = InnoDB,
PARTITION before20180308232522 VALUES LESS THAN (63687770722) ENGINE = InnoDB,
PARTITION before20180309012522 VALUES LESS THAN (63687777922) ENGINE = InnoDB,
PARTITION before20180309032522 VALUES LESS THAN (63687785122) ENGINE = InnoDB,
PARTITION before20180309052522 VALUES LESS THAN (63687792322) ENGINE = InnoDB,
PARTITION before20180309072522 VALUES LESS THAN (63687799522) ENGINE = InnoDB,
PARTITION before20180309092522 VALUES LESS THAN (63687806722) ENGINE = InnoDB,
PARTITION before20180309112522 VALUES LESS THAN (63687813922) ENGINE = InnoDB,
PARTITION before20180309132522 VALUES LESS THAN (63687821122) ENGINE = InnoDB,
PARTITION before20180309152522 VALUES LESS THAN (63687828322) ENGINE = InnoDB,
PARTITION before20180309172522 VALUES LESS THAN (63687835522) ENGINE = InnoDB,
PARTITION before20180309192522 VALUES LESS THAN (63687842722) ENGINE = InnoDB,
PARTITION before20180309212522 VALUES LESS THAN (63687849922) ENGINE = InnoDB,
PARTITION before20180309232522 VALUES LESS THAN (63687857122) ENGINE = InnoDB,
PARTITION before20180310012522 VALUES LESS THAN (63687864322) ENGINE = InnoDB,
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
PARTITION before20160314093723 VALUES LESS THAN (63625167443) ENGINE = InnoDB,
PARTITION before20160402014923 VALUES LESS THAN (63626780963) ENGINE = InnoDB,
PARTITION before20160420180123 VALUES LESS THAN (63628394483) ENGINE = InnoDB,
PARTITION before20160509101323 VALUES LESS THAN (63630008003) ENGINE = InnoDB,
PARTITION before20160528022523 VALUES LESS THAN (63631621523) ENGINE = InnoDB,
PARTITION before20160615183723 VALUES LESS THAN (63633235043) ENGINE = InnoDB,
PARTITION before20160704104923 VALUES LESS THAN (63634848563) ENGINE = InnoDB,
PARTITION before20160723030123 VALUES LESS THAN (63636462083) ENGINE = InnoDB,
PARTITION before20160810191323 VALUES LESS THAN (63638075603) ENGINE = InnoDB,
PARTITION before20160829112523 VALUES LESS THAN (63639689123) ENGINE = InnoDB,
PARTITION before20160917033723 VALUES LESS THAN (63641302643) ENGINE = InnoDB,
PARTITION before20161005194923 VALUES LESS THAN (63642916163) ENGINE = InnoDB,
PARTITION before20161024120123 VALUES LESS THAN (63644529683) ENGINE = InnoDB,
PARTITION before20161112041323 VALUES LESS THAN (63646143203) ENGINE = InnoDB,
PARTITION before20161130202523 VALUES LESS THAN (63647756723) ENGINE = InnoDB,
PARTITION before20161219123723 VALUES LESS THAN (63649370243) ENGINE = InnoDB,
PARTITION before20170107044923 VALUES LESS THAN (63650983763) ENGINE = InnoDB,
PARTITION before20170125210123 VALUES LESS THAN (63652597283) ENGINE = InnoDB,
PARTITION before20170213131323 VALUES LESS THAN (63654210803) ENGINE = InnoDB,
PARTITION before20170304052523 VALUES LESS THAN (63655824323) ENGINE = InnoDB,
PARTITION before20170322213723 VALUES LESS THAN (63657437843) ENGINE = InnoDB,
PARTITION before20170410134923 VALUES LESS THAN (63659051363) ENGINE = InnoDB,
PARTITION before20170429060123 VALUES LESS THAN (63660664883) ENGINE = InnoDB,
PARTITION before20170517221323 VALUES LESS THAN (63662278403) ENGINE = InnoDB,
PARTITION before20170605142523 VALUES LESS THAN (63663891923) ENGINE = InnoDB,
PARTITION before20170624063723 VALUES LESS THAN (63665505443) ENGINE = InnoDB,
PARTITION before20170712224923 VALUES LESS THAN (63667118963) ENGINE = InnoDB,
PARTITION before20170731150123 VALUES LESS THAN (63668732483) ENGINE = InnoDB,
PARTITION before20170819071323 VALUES LESS THAN (63670346003) ENGINE = InnoDB,
PARTITION before20170906232523 VALUES LESS THAN (63671959523) ENGINE = InnoDB,
PARTITION before20170925153723 VALUES LESS THAN (63673573043) ENGINE = InnoDB,
PARTITION before20171014074923 VALUES LESS THAN (63675186563) ENGINE = InnoDB,
PARTITION before20171102000123 VALUES LESS THAN (63676800083) ENGINE = InnoDB,
PARTITION before20171120161323 VALUES LESS THAN (63678413603) ENGINE = InnoDB,
PARTITION before20171209082523 VALUES LESS THAN (63680027123) ENGINE = InnoDB,
PARTITION before20171228003723 VALUES LESS THAN (63681640643) ENGINE = InnoDB,
PARTITION before20180115164923 VALUES LESS THAN (63683254163) ENGINE = InnoDB,
PARTITION before20180203090123 VALUES LESS THAN (63684867683) ENGINE = InnoDB,
PARTITION before20180222011323 VALUES LESS THAN (63686481203) ENGINE = InnoDB,
PARTITION before20180312172523 VALUES LESS THAN (63688094723) ENGINE = InnoDB,
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
PARTITION before20180309153720 VALUES LESS THAN (63687829040) ENGINE = InnoDB,
PARTITION before20180309154320 VALUES LESS THAN (63687829400) ENGINE = InnoDB,
PARTITION before20180309154920 VALUES LESS THAN (63687829760) ENGINE = InnoDB,
PARTITION before20180309155520 VALUES LESS THAN (63687830120) ENGINE = InnoDB,
PARTITION before20180309160120 VALUES LESS THAN (63687830480) ENGINE = InnoDB,
PARTITION before20180309160720 VALUES LESS THAN (63687830840) ENGINE = InnoDB,
PARTITION before20180309161320 VALUES LESS THAN (63687831200) ENGINE = InnoDB,
PARTITION before20180309161920 VALUES LESS THAN (63687831560) ENGINE = InnoDB,
PARTITION before20180309162520 VALUES LESS THAN (63687831920) ENGINE = InnoDB,
PARTITION before20180309163120 VALUES LESS THAN (63687832280) ENGINE = InnoDB,
PARTITION before20180309163720 VALUES LESS THAN (63687832640) ENGINE = InnoDB,
PARTITION before20180309164320 VALUES LESS THAN (63687833000) ENGINE = InnoDB,
PARTITION before20180309164920 VALUES LESS THAN (63687833360) ENGINE = InnoDB,
PARTITION before20180309165520 VALUES LESS THAN (63687833720) ENGINE = InnoDB,
PARTITION before20180309170120 VALUES LESS THAN (63687834080) ENGINE = InnoDB,
PARTITION before20180309170720 VALUES LESS THAN (63687834440) ENGINE = InnoDB,
PARTITION before20180309171320 VALUES LESS THAN (63687834800) ENGINE = InnoDB,
PARTITION before20180309171920 VALUES LESS THAN (63687835160) ENGINE = InnoDB,
PARTITION before20180309172520 VALUES LESS THAN (63687835520) ENGINE = InnoDB,
PARTITION before20180309173120 VALUES LESS THAN (63687835880) ENGINE = InnoDB,
PARTITION before20180309173720 VALUES LESS THAN (63687836240) ENGINE = InnoDB,
PARTITION before20180309174320 VALUES LESS THAN (63687836600) ENGINE = InnoDB,
PARTITION before20180309174920 VALUES LESS THAN (63687836960) ENGINE = InnoDB,
PARTITION before20180309175520 VALUES LESS THAN (63687837320) ENGINE = InnoDB,
PARTITION before20180309180120 VALUES LESS THAN (63687837680) ENGINE = InnoDB,
PARTITION before20180309180720 VALUES LESS THAN (63687838040) ENGINE = InnoDB,
PARTITION before20180309181320 VALUES LESS THAN (63687838400) ENGINE = InnoDB,
PARTITION before20180309181920 VALUES LESS THAN (63687838760) ENGINE = InnoDB,
PARTITION before20180309182520 VALUES LESS THAN (63687839120) ENGINE = InnoDB,
PARTITION before20180309183120 VALUES LESS THAN (63687839480) ENGINE = InnoDB,
PARTITION before20180309183720 VALUES LESS THAN (63687839840) ENGINE = InnoDB,
PARTITION before20180309184320 VALUES LESS THAN (63687840200) ENGINE = InnoDB,
PARTITION before20180309184920 VALUES LESS THAN (63687840560) ENGINE = InnoDB,
PARTITION before20180309185520 VALUES LESS THAN (63687840920) ENGINE = InnoDB,
PARTITION before20180309190120 VALUES LESS THAN (63687841280) ENGINE = InnoDB,
PARTITION before20180309190720 VALUES LESS THAN (63687841640) ENGINE = InnoDB,
PARTITION before20180309191320 VALUES LESS THAN (63687842000) ENGINE = InnoDB,
PARTITION before20180309191920 VALUES LESS THAN (63687842360) ENGINE = InnoDB,
PARTITION before20180309192520 VALUES LESS THAN (63687842720) ENGINE = InnoDB,
PARTITION before20180309193120 VALUES LESS THAN (63687843080) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_ds_primary_keys BEFORE INSERT ON ds_stats_latest
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
PARTITION before20180110071324 VALUES LESS THAN (63682787604) ENGINE = InnoDB,
PARTITION before20180111210124 VALUES LESS THAN (63682923684) ENGINE = InnoDB,
PARTITION before20180113104924 VALUES LESS THAN (63683059764) ENGINE = InnoDB,
PARTITION before20180115003724 VALUES LESS THAN (63683195844) ENGINE = InnoDB,
PARTITION before20180116142524 VALUES LESS THAN (63683331924) ENGINE = InnoDB,
PARTITION before20180118041324 VALUES LESS THAN (63683468004) ENGINE = InnoDB,
PARTITION before20180119180124 VALUES LESS THAN (63683604084) ENGINE = InnoDB,
PARTITION before20180121074924 VALUES LESS THAN (63683740164) ENGINE = InnoDB,
PARTITION before20180122213724 VALUES LESS THAN (63683876244) ENGINE = InnoDB,
PARTITION before20180124112524 VALUES LESS THAN (63684012324) ENGINE = InnoDB,
PARTITION before20180126011324 VALUES LESS THAN (63684148404) ENGINE = InnoDB,
PARTITION before20180127150124 VALUES LESS THAN (63684284484) ENGINE = InnoDB,
PARTITION before20180129044924 VALUES LESS THAN (63684420564) ENGINE = InnoDB,
PARTITION before20180130183724 VALUES LESS THAN (63684556644) ENGINE = InnoDB,
PARTITION before20180201082524 VALUES LESS THAN (63684692724) ENGINE = InnoDB,
PARTITION before20180202221324 VALUES LESS THAN (63684828804) ENGINE = InnoDB,
PARTITION before20180204120124 VALUES LESS THAN (63684964884) ENGINE = InnoDB,
PARTITION before20180206014924 VALUES LESS THAN (63685100964) ENGINE = InnoDB,
PARTITION before20180207153724 VALUES LESS THAN (63685237044) ENGINE = InnoDB,
PARTITION before20180209052524 VALUES LESS THAN (63685373124) ENGINE = InnoDB,
PARTITION before20180210191324 VALUES LESS THAN (63685509204) ENGINE = InnoDB,
PARTITION before20180212090124 VALUES LESS THAN (63685645284) ENGINE = InnoDB,
PARTITION before20180213224924 VALUES LESS THAN (63685781364) ENGINE = InnoDB,
PARTITION before20180215123724 VALUES LESS THAN (63685917444) ENGINE = InnoDB,
PARTITION before20180217022524 VALUES LESS THAN (63686053524) ENGINE = InnoDB,
PARTITION before20180218161324 VALUES LESS THAN (63686189604) ENGINE = InnoDB,
PARTITION before20180220060124 VALUES LESS THAN (63686325684) ENGINE = InnoDB,
PARTITION before20180221194924 VALUES LESS THAN (63686461764) ENGINE = InnoDB,
PARTITION before20180223093724 VALUES LESS THAN (63686597844) ENGINE = InnoDB,
PARTITION before20180224232524 VALUES LESS THAN (63686733924) ENGINE = InnoDB,
PARTITION before20180226131324 VALUES LESS THAN (63686870004) ENGINE = InnoDB,
PARTITION before20180228030124 VALUES LESS THAN (63687006084) ENGINE = InnoDB,
PARTITION before20180301164924 VALUES LESS THAN (63687142164) ENGINE = InnoDB,
PARTITION before20180303063724 VALUES LESS THAN (63687278244) ENGINE = InnoDB,
PARTITION before20180304202524 VALUES LESS THAN (63687414324) ENGINE = InnoDB,
PARTITION before20180306101324 VALUES LESS THAN (63687550404) ENGINE = InnoDB,
PARTITION before20180308000124 VALUES LESS THAN (63687686484) ENGINE = InnoDB,
PARTITION before20180309134924 VALUES LESS THAN (63687822564) ENGINE = InnoDB,
PARTITION before20180311033724 VALUES LESS THAN (63687958644) ENGINE = InnoDB,
PARTITION before20180312172524 VALUES LESS THAN (63688094724) ENGINE = InnoDB,
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
PARTITION before20180306192525 VALUES LESS THAN (63687583525) ENGINE = InnoDB,
PARTITION before20180306212525 VALUES LESS THAN (63687590725) ENGINE = InnoDB,
PARTITION before20180306232525 VALUES LESS THAN (63687597925) ENGINE = InnoDB,
PARTITION before20180307012525 VALUES LESS THAN (63687605125) ENGINE = InnoDB,
PARTITION before20180307032525 VALUES LESS THAN (63687612325) ENGINE = InnoDB,
PARTITION before20180307052525 VALUES LESS THAN (63687619525) ENGINE = InnoDB,
PARTITION before20180307072525 VALUES LESS THAN (63687626725) ENGINE = InnoDB,
PARTITION before20180307092525 VALUES LESS THAN (63687633925) ENGINE = InnoDB,
PARTITION before20180307112525 VALUES LESS THAN (63687641125) ENGINE = InnoDB,
PARTITION before20180307132525 VALUES LESS THAN (63687648325) ENGINE = InnoDB,
PARTITION before20180307152525 VALUES LESS THAN (63687655525) ENGINE = InnoDB,
PARTITION before20180307172525 VALUES LESS THAN (63687662725) ENGINE = InnoDB,
PARTITION before20180307192525 VALUES LESS THAN (63687669925) ENGINE = InnoDB,
PARTITION before20180307212525 VALUES LESS THAN (63687677125) ENGINE = InnoDB,
PARTITION before20180307232525 VALUES LESS THAN (63687684325) ENGINE = InnoDB,
PARTITION before20180308012525 VALUES LESS THAN (63687691525) ENGINE = InnoDB,
PARTITION before20180308032525 VALUES LESS THAN (63687698725) ENGINE = InnoDB,
PARTITION before20180308052525 VALUES LESS THAN (63687705925) ENGINE = InnoDB,
PARTITION before20180308072525 VALUES LESS THAN (63687713125) ENGINE = InnoDB,
PARTITION before20180308092525 VALUES LESS THAN (63687720325) ENGINE = InnoDB,
PARTITION before20180308112525 VALUES LESS THAN (63687727525) ENGINE = InnoDB,
PARTITION before20180308132525 VALUES LESS THAN (63687734725) ENGINE = InnoDB,
PARTITION before20180308152525 VALUES LESS THAN (63687741925) ENGINE = InnoDB,
PARTITION before20180308172525 VALUES LESS THAN (63687749125) ENGINE = InnoDB,
PARTITION before20180308192525 VALUES LESS THAN (63687756325) ENGINE = InnoDB,
PARTITION before20180308212525 VALUES LESS THAN (63687763525) ENGINE = InnoDB,
PARTITION before20180308232525 VALUES LESS THAN (63687770725) ENGINE = InnoDB,
PARTITION before20180309012525 VALUES LESS THAN (63687777925) ENGINE = InnoDB,
PARTITION before20180309032525 VALUES LESS THAN (63687785125) ENGINE = InnoDB,
PARTITION before20180309052525 VALUES LESS THAN (63687792325) ENGINE = InnoDB,
PARTITION before20180309072525 VALUES LESS THAN (63687799525) ENGINE = InnoDB,
PARTITION before20180309092525 VALUES LESS THAN (63687806725) ENGINE = InnoDB,
PARTITION before20180309112525 VALUES LESS THAN (63687813925) ENGINE = InnoDB,
PARTITION before20180309132525 VALUES LESS THAN (63687821125) ENGINE = InnoDB,
PARTITION before20180309152525 VALUES LESS THAN (63687828325) ENGINE = InnoDB,
PARTITION before20180309172525 VALUES LESS THAN (63687835525) ENGINE = InnoDB,
PARTITION before20180309192525 VALUES LESS THAN (63687842725) ENGINE = InnoDB,
PARTITION before20180309212525 VALUES LESS THAN (63687849925) ENGINE = InnoDB,
PARTITION before20180309232525 VALUES LESS THAN (63687857125) ENGINE = InnoDB,
PARTITION before20180310012525 VALUES LESS THAN (63687864325) ENGINE = InnoDB,
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
PARTITION before20160314093726 VALUES LESS THAN (63625167446) ENGINE = InnoDB,
PARTITION before20160402014926 VALUES LESS THAN (63626780966) ENGINE = InnoDB,
PARTITION before20160420180126 VALUES LESS THAN (63628394486) ENGINE = InnoDB,
PARTITION before20160509101326 VALUES LESS THAN (63630008006) ENGINE = InnoDB,
PARTITION before20160528022526 VALUES LESS THAN (63631621526) ENGINE = InnoDB,
PARTITION before20160615183726 VALUES LESS THAN (63633235046) ENGINE = InnoDB,
PARTITION before20160704104926 VALUES LESS THAN (63634848566) ENGINE = InnoDB,
PARTITION before20160723030126 VALUES LESS THAN (63636462086) ENGINE = InnoDB,
PARTITION before20160810191326 VALUES LESS THAN (63638075606) ENGINE = InnoDB,
PARTITION before20160829112526 VALUES LESS THAN (63639689126) ENGINE = InnoDB,
PARTITION before20160917033726 VALUES LESS THAN (63641302646) ENGINE = InnoDB,
PARTITION before20161005194926 VALUES LESS THAN (63642916166) ENGINE = InnoDB,
PARTITION before20161024120126 VALUES LESS THAN (63644529686) ENGINE = InnoDB,
PARTITION before20161112041326 VALUES LESS THAN (63646143206) ENGINE = InnoDB,
PARTITION before20161130202526 VALUES LESS THAN (63647756726) ENGINE = InnoDB,
PARTITION before20161219123726 VALUES LESS THAN (63649370246) ENGINE = InnoDB,
PARTITION before20170107044926 VALUES LESS THAN (63650983766) ENGINE = InnoDB,
PARTITION before20170125210126 VALUES LESS THAN (63652597286) ENGINE = InnoDB,
PARTITION before20170213131326 VALUES LESS THAN (63654210806) ENGINE = InnoDB,
PARTITION before20170304052526 VALUES LESS THAN (63655824326) ENGINE = InnoDB,
PARTITION before20170322213726 VALUES LESS THAN (63657437846) ENGINE = InnoDB,
PARTITION before20170410134926 VALUES LESS THAN (63659051366) ENGINE = InnoDB,
PARTITION before20170429060126 VALUES LESS THAN (63660664886) ENGINE = InnoDB,
PARTITION before20170517221326 VALUES LESS THAN (63662278406) ENGINE = InnoDB,
PARTITION before20170605142526 VALUES LESS THAN (63663891926) ENGINE = InnoDB,
PARTITION before20170624063726 VALUES LESS THAN (63665505446) ENGINE = InnoDB,
PARTITION before20170712224926 VALUES LESS THAN (63667118966) ENGINE = InnoDB,
PARTITION before20170731150126 VALUES LESS THAN (63668732486) ENGINE = InnoDB,
PARTITION before20170819071326 VALUES LESS THAN (63670346006) ENGINE = InnoDB,
PARTITION before20170906232526 VALUES LESS THAN (63671959526) ENGINE = InnoDB,
PARTITION before20170925153726 VALUES LESS THAN (63673573046) ENGINE = InnoDB,
PARTITION before20171014074926 VALUES LESS THAN (63675186566) ENGINE = InnoDB,
PARTITION before20171102000126 VALUES LESS THAN (63676800086) ENGINE = InnoDB,
PARTITION before20171120161326 VALUES LESS THAN (63678413606) ENGINE = InnoDB,
PARTITION before20171209082526 VALUES LESS THAN (63680027126) ENGINE = InnoDB,
PARTITION before20171228003726 VALUES LESS THAN (63681640646) ENGINE = InnoDB,
PARTITION before20180115164926 VALUES LESS THAN (63683254166) ENGINE = InnoDB,
PARTITION before20180203090126 VALUES LESS THAN (63684867686) ENGINE = InnoDB,
PARTITION before20180222011326 VALUES LESS THAN (63686481206) ENGINE = InnoDB,
PARTITION before20180312172526 VALUES LESS THAN (63688094726) ENGINE = InnoDB,
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
PARTITION before20180309153724 VALUES LESS THAN (63687829044) ENGINE = InnoDB,
PARTITION before20180309154324 VALUES LESS THAN (63687829404) ENGINE = InnoDB,
PARTITION before20180309154924 VALUES LESS THAN (63687829764) ENGINE = InnoDB,
PARTITION before20180309155524 VALUES LESS THAN (63687830124) ENGINE = InnoDB,
PARTITION before20180309160124 VALUES LESS THAN (63687830484) ENGINE = InnoDB,
PARTITION before20180309160724 VALUES LESS THAN (63687830844) ENGINE = InnoDB,
PARTITION before20180309161324 VALUES LESS THAN (63687831204) ENGINE = InnoDB,
PARTITION before20180309161924 VALUES LESS THAN (63687831564) ENGINE = InnoDB,
PARTITION before20180309162524 VALUES LESS THAN (63687831924) ENGINE = InnoDB,
PARTITION before20180309163124 VALUES LESS THAN (63687832284) ENGINE = InnoDB,
PARTITION before20180309163724 VALUES LESS THAN (63687832644) ENGINE = InnoDB,
PARTITION before20180309164324 VALUES LESS THAN (63687833004) ENGINE = InnoDB,
PARTITION before20180309164924 VALUES LESS THAN (63687833364) ENGINE = InnoDB,
PARTITION before20180309165524 VALUES LESS THAN (63687833724) ENGINE = InnoDB,
PARTITION before20180309170124 VALUES LESS THAN (63687834084) ENGINE = InnoDB,
PARTITION before20180309170724 VALUES LESS THAN (63687834444) ENGINE = InnoDB,
PARTITION before20180309171324 VALUES LESS THAN (63687834804) ENGINE = InnoDB,
PARTITION before20180309171924 VALUES LESS THAN (63687835164) ENGINE = InnoDB,
PARTITION before20180309172524 VALUES LESS THAN (63687835524) ENGINE = InnoDB,
PARTITION before20180309173124 VALUES LESS THAN (63687835884) ENGINE = InnoDB,
PARTITION before20180309173724 VALUES LESS THAN (63687836244) ENGINE = InnoDB,
PARTITION before20180309174324 VALUES LESS THAN (63687836604) ENGINE = InnoDB,
PARTITION before20180309174924 VALUES LESS THAN (63687836964) ENGINE = InnoDB,
PARTITION before20180309175524 VALUES LESS THAN (63687837324) ENGINE = InnoDB,
PARTITION before20180309180124 VALUES LESS THAN (63687837684) ENGINE = InnoDB,
PARTITION before20180309180724 VALUES LESS THAN (63687838044) ENGINE = InnoDB,
PARTITION before20180309181324 VALUES LESS THAN (63687838404) ENGINE = InnoDB,
PARTITION before20180309181924 VALUES LESS THAN (63687838764) ENGINE = InnoDB,
PARTITION before20180309182524 VALUES LESS THAN (63687839124) ENGINE = InnoDB,
PARTITION before20180309183124 VALUES LESS THAN (63687839484) ENGINE = InnoDB,
PARTITION before20180309183724 VALUES LESS THAN (63687839844) ENGINE = InnoDB,
PARTITION before20180309184324 VALUES LESS THAN (63687840204) ENGINE = InnoDB,
PARTITION before20180309184924 VALUES LESS THAN (63687840564) ENGINE = InnoDB,
PARTITION before20180309185524 VALUES LESS THAN (63687840924) ENGINE = InnoDB,
PARTITION before20180309190124 VALUES LESS THAN (63687841284) ENGINE = InnoDB,
PARTITION before20180309190724 VALUES LESS THAN (63687841644) ENGINE = InnoDB,
PARTITION before20180309191324 VALUES LESS THAN (63687842004) ENGINE = InnoDB,
PARTITION before20180309191924 VALUES LESS THAN (63687842364) ENGINE = InnoDB,
PARTITION before20180309192524 VALUES LESS THAN (63687842724) ENGINE = InnoDB,
PARTITION before20180309193124 VALUES LESS THAN (63687843084) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_iom_primary_keys BEFORE INSERT ON iom_stats_latest
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
PARTITION before20180110072059 VALUES LESS THAN (63682788059) ENGINE = InnoDB,
PARTITION before20180111210859 VALUES LESS THAN (63682924139) ENGINE = InnoDB,
PARTITION before20180113105659 VALUES LESS THAN (63683060219) ENGINE = InnoDB,
PARTITION before20180115004459 VALUES LESS THAN (63683196299) ENGINE = InnoDB,
PARTITION before20180116143259 VALUES LESS THAN (63683332379) ENGINE = InnoDB,
PARTITION before20180118042059 VALUES LESS THAN (63683468459) ENGINE = InnoDB,
PARTITION before20180119180859 VALUES LESS THAN (63683604539) ENGINE = InnoDB,
PARTITION before20180121075659 VALUES LESS THAN (63683740619) ENGINE = InnoDB,
PARTITION before20180122214459 VALUES LESS THAN (63683876699) ENGINE = InnoDB,
PARTITION before20180124113259 VALUES LESS THAN (63684012779) ENGINE = InnoDB,
PARTITION before20180126012059 VALUES LESS THAN (63684148859) ENGINE = InnoDB,
PARTITION before20180127150859 VALUES LESS THAN (63684284939) ENGINE = InnoDB,
PARTITION before20180129045659 VALUES LESS THAN (63684421019) ENGINE = InnoDB,
PARTITION before20180130184459 VALUES LESS THAN (63684557099) ENGINE = InnoDB,
PARTITION before20180201083259 VALUES LESS THAN (63684693179) ENGINE = InnoDB,
PARTITION before20180202222059 VALUES LESS THAN (63684829259) ENGINE = InnoDB,
PARTITION before20180204120859 VALUES LESS THAN (63684965339) ENGINE = InnoDB,
PARTITION before20180206015659 VALUES LESS THAN (63685101419) ENGINE = InnoDB,
PARTITION before20180207154459 VALUES LESS THAN (63685237499) ENGINE = InnoDB,
PARTITION before20180209053259 VALUES LESS THAN (63685373579) ENGINE = InnoDB,
PARTITION before20180210192059 VALUES LESS THAN (63685509659) ENGINE = InnoDB,
PARTITION before20180212090859 VALUES LESS THAN (63685645739) ENGINE = InnoDB,
PARTITION before20180213225659 VALUES LESS THAN (63685781819) ENGINE = InnoDB,
PARTITION before20180215124459 VALUES LESS THAN (63685917899) ENGINE = InnoDB,
PARTITION before20180217023259 VALUES LESS THAN (63686053979) ENGINE = InnoDB,
PARTITION before20180218162059 VALUES LESS THAN (63686190059) ENGINE = InnoDB,
PARTITION before20180220060859 VALUES LESS THAN (63686326139) ENGINE = InnoDB,
PARTITION before20180221195659 VALUES LESS THAN (63686462219) ENGINE = InnoDB,
PARTITION before20180223094459 VALUES LESS THAN (63686598299) ENGINE = InnoDB,
PARTITION before20180224233259 VALUES LESS THAN (63686734379) ENGINE = InnoDB,
PARTITION before20180226132059 VALUES LESS THAN (63686870459) ENGINE = InnoDB,
PARTITION before20180228030859 VALUES LESS THAN (63687006539) ENGINE = InnoDB,
PARTITION before20180301165659 VALUES LESS THAN (63687142619) ENGINE = InnoDB,
PARTITION before20180303064459 VALUES LESS THAN (63687278699) ENGINE = InnoDB,
PARTITION before20180304203259 VALUES LESS THAN (63687414779) ENGINE = InnoDB,
PARTITION before20180306102059 VALUES LESS THAN (63687550859) ENGINE = InnoDB,
PARTITION before20180308000859 VALUES LESS THAN (63687686939) ENGINE = InnoDB,
PARTITION before20180309135659 VALUES LESS THAN (63687823019) ENGINE = InnoDB,
PARTITION before20180311034459 VALUES LESS THAN (63687959099) ENGINE = InnoDB,
PARTITION before20180312173259 VALUES LESS THAN (63688095179) ENGINE = InnoDB,
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
PARTITION before20180306193300 VALUES LESS THAN (63687583980) ENGINE = InnoDB,
PARTITION before20180306213300 VALUES LESS THAN (63687591180) ENGINE = InnoDB,
PARTITION before20180306233300 VALUES LESS THAN (63687598380) ENGINE = InnoDB,
PARTITION before20180307013300 VALUES LESS THAN (63687605580) ENGINE = InnoDB,
PARTITION before20180307033300 VALUES LESS THAN (63687612780) ENGINE = InnoDB,
PARTITION before20180307053300 VALUES LESS THAN (63687619980) ENGINE = InnoDB,
PARTITION before20180307073300 VALUES LESS THAN (63687627180) ENGINE = InnoDB,
PARTITION before20180307093300 VALUES LESS THAN (63687634380) ENGINE = InnoDB,
PARTITION before20180307113300 VALUES LESS THAN (63687641580) ENGINE = InnoDB,
PARTITION before20180307133300 VALUES LESS THAN (63687648780) ENGINE = InnoDB,
PARTITION before20180307153300 VALUES LESS THAN (63687655980) ENGINE = InnoDB,
PARTITION before20180307173300 VALUES LESS THAN (63687663180) ENGINE = InnoDB,
PARTITION before20180307193300 VALUES LESS THAN (63687670380) ENGINE = InnoDB,
PARTITION before20180307213300 VALUES LESS THAN (63687677580) ENGINE = InnoDB,
PARTITION before20180307233300 VALUES LESS THAN (63687684780) ENGINE = InnoDB,
PARTITION before20180308013300 VALUES LESS THAN (63687691980) ENGINE = InnoDB,
PARTITION before20180308033300 VALUES LESS THAN (63687699180) ENGINE = InnoDB,
PARTITION before20180308053300 VALUES LESS THAN (63687706380) ENGINE = InnoDB,
PARTITION before20180308073300 VALUES LESS THAN (63687713580) ENGINE = InnoDB,
PARTITION before20180308093300 VALUES LESS THAN (63687720780) ENGINE = InnoDB,
PARTITION before20180308113300 VALUES LESS THAN (63687727980) ENGINE = InnoDB,
PARTITION before20180308133300 VALUES LESS THAN (63687735180) ENGINE = InnoDB,
PARTITION before20180308153300 VALUES LESS THAN (63687742380) ENGINE = InnoDB,
PARTITION before20180308173300 VALUES LESS THAN (63687749580) ENGINE = InnoDB,
PARTITION before20180308193300 VALUES LESS THAN (63687756780) ENGINE = InnoDB,
PARTITION before20180308213300 VALUES LESS THAN (63687763980) ENGINE = InnoDB,
PARTITION before20180308233300 VALUES LESS THAN (63687771180) ENGINE = InnoDB,
PARTITION before20180309013300 VALUES LESS THAN (63687778380) ENGINE = InnoDB,
PARTITION before20180309033300 VALUES LESS THAN (63687785580) ENGINE = InnoDB,
PARTITION before20180309053300 VALUES LESS THAN (63687792780) ENGINE = InnoDB,
PARTITION before20180309073300 VALUES LESS THAN (63687799980) ENGINE = InnoDB,
PARTITION before20180309093300 VALUES LESS THAN (63687807180) ENGINE = InnoDB,
PARTITION before20180309113300 VALUES LESS THAN (63687814380) ENGINE = InnoDB,
PARTITION before20180309133300 VALUES LESS THAN (63687821580) ENGINE = InnoDB,
PARTITION before20180309153300 VALUES LESS THAN (63687828780) ENGINE = InnoDB,
PARTITION before20180309173300 VALUES LESS THAN (63687835980) ENGINE = InnoDB,
PARTITION before20180309193300 VALUES LESS THAN (63687843180) ENGINE = InnoDB,
PARTITION before20180309213300 VALUES LESS THAN (63687850380) ENGINE = InnoDB,
PARTITION before20180309233300 VALUES LESS THAN (63687857580) ENGINE = InnoDB,
PARTITION before20180310013300 VALUES LESS THAN (63687864780) ENGINE = InnoDB,
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
PARTITION before20160314094501 VALUES LESS THAN (63625167901) ENGINE = InnoDB,
PARTITION before20160402015701 VALUES LESS THAN (63626781421) ENGINE = InnoDB,
PARTITION before20160420180901 VALUES LESS THAN (63628394941) ENGINE = InnoDB,
PARTITION before20160509102101 VALUES LESS THAN (63630008461) ENGINE = InnoDB,
PARTITION before20160528023301 VALUES LESS THAN (63631621981) ENGINE = InnoDB,
PARTITION before20160615184501 VALUES LESS THAN (63633235501) ENGINE = InnoDB,
PARTITION before20160704105701 VALUES LESS THAN (63634849021) ENGINE = InnoDB,
PARTITION before20160723030901 VALUES LESS THAN (63636462541) ENGINE = InnoDB,
PARTITION before20160810192101 VALUES LESS THAN (63638076061) ENGINE = InnoDB,
PARTITION before20160829113301 VALUES LESS THAN (63639689581) ENGINE = InnoDB,
PARTITION before20160917034501 VALUES LESS THAN (63641303101) ENGINE = InnoDB,
PARTITION before20161005195701 VALUES LESS THAN (63642916621) ENGINE = InnoDB,
PARTITION before20161024120901 VALUES LESS THAN (63644530141) ENGINE = InnoDB,
PARTITION before20161112042101 VALUES LESS THAN (63646143661) ENGINE = InnoDB,
PARTITION before20161130203301 VALUES LESS THAN (63647757181) ENGINE = InnoDB,
PARTITION before20161219124501 VALUES LESS THAN (63649370701) ENGINE = InnoDB,
PARTITION before20170107045701 VALUES LESS THAN (63650984221) ENGINE = InnoDB,
PARTITION before20170125210901 VALUES LESS THAN (63652597741) ENGINE = InnoDB,
PARTITION before20170213132101 VALUES LESS THAN (63654211261) ENGINE = InnoDB,
PARTITION before20170304053301 VALUES LESS THAN (63655824781) ENGINE = InnoDB,
PARTITION before20170322214501 VALUES LESS THAN (63657438301) ENGINE = InnoDB,
PARTITION before20170410135701 VALUES LESS THAN (63659051821) ENGINE = InnoDB,
PARTITION before20170429060901 VALUES LESS THAN (63660665341) ENGINE = InnoDB,
PARTITION before20170517222101 VALUES LESS THAN (63662278861) ENGINE = InnoDB,
PARTITION before20170605143301 VALUES LESS THAN (63663892381) ENGINE = InnoDB,
PARTITION before20170624064501 VALUES LESS THAN (63665505901) ENGINE = InnoDB,
PARTITION before20170712225701 VALUES LESS THAN (63667119421) ENGINE = InnoDB,
PARTITION before20170731150901 VALUES LESS THAN (63668732941) ENGINE = InnoDB,
PARTITION before20170819072101 VALUES LESS THAN (63670346461) ENGINE = InnoDB,
PARTITION before20170906233301 VALUES LESS THAN (63671959981) ENGINE = InnoDB,
PARTITION before20170925154501 VALUES LESS THAN (63673573501) ENGINE = InnoDB,
PARTITION before20171014075701 VALUES LESS THAN (63675187021) ENGINE = InnoDB,
PARTITION before20171102000901 VALUES LESS THAN (63676800541) ENGINE = InnoDB,
PARTITION before20171120162101 VALUES LESS THAN (63678414061) ENGINE = InnoDB,
PARTITION before20171209083301 VALUES LESS THAN (63680027581) ENGINE = InnoDB,
PARTITION before20171228004501 VALUES LESS THAN (63681641101) ENGINE = InnoDB,
PARTITION before20180115165701 VALUES LESS THAN (63683254621) ENGINE = InnoDB,
PARTITION before20180203090901 VALUES LESS THAN (63684868141) ENGINE = InnoDB,
PARTITION before20180222012101 VALUES LESS THAN (63686481661) ENGINE = InnoDB,
PARTITION before20180312173301 VALUES LESS THAN (63688095181) ENGINE = InnoDB,
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
PARTITION before20180309153856 VALUES LESS THAN (63687829136) ENGINE = InnoDB,
PARTITION before20180309154456 VALUES LESS THAN (63687829496) ENGINE = InnoDB,
PARTITION before20180309155056 VALUES LESS THAN (63687829856) ENGINE = InnoDB,
PARTITION before20180309155656 VALUES LESS THAN (63687830216) ENGINE = InnoDB,
PARTITION before20180309160256 VALUES LESS THAN (63687830576) ENGINE = InnoDB,
PARTITION before20180309160856 VALUES LESS THAN (63687830936) ENGINE = InnoDB,
PARTITION before20180309161456 VALUES LESS THAN (63687831296) ENGINE = InnoDB,
PARTITION before20180309162056 VALUES LESS THAN (63687831656) ENGINE = InnoDB,
PARTITION before20180309162656 VALUES LESS THAN (63687832016) ENGINE = InnoDB,
PARTITION before20180309163256 VALUES LESS THAN (63687832376) ENGINE = InnoDB,
PARTITION before20180309163856 VALUES LESS THAN (63687832736) ENGINE = InnoDB,
PARTITION before20180309164456 VALUES LESS THAN (63687833096) ENGINE = InnoDB,
PARTITION before20180309165056 VALUES LESS THAN (63687833456) ENGINE = InnoDB,
PARTITION before20180309165656 VALUES LESS THAN (63687833816) ENGINE = InnoDB,
PARTITION before20180309170256 VALUES LESS THAN (63687834176) ENGINE = InnoDB,
PARTITION before20180309170856 VALUES LESS THAN (63687834536) ENGINE = InnoDB,
PARTITION before20180309171456 VALUES LESS THAN (63687834896) ENGINE = InnoDB,
PARTITION before20180309172056 VALUES LESS THAN (63687835256) ENGINE = InnoDB,
PARTITION before20180309172656 VALUES LESS THAN (63687835616) ENGINE = InnoDB,
PARTITION before20180309173256 VALUES LESS THAN (63687835976) ENGINE = InnoDB,
PARTITION before20180309173856 VALUES LESS THAN (63687836336) ENGINE = InnoDB,
PARTITION before20180309174456 VALUES LESS THAN (63687836696) ENGINE = InnoDB,
PARTITION before20180309175056 VALUES LESS THAN (63687837056) ENGINE = InnoDB,
PARTITION before20180309175656 VALUES LESS THAN (63687837416) ENGINE = InnoDB,
PARTITION before20180309180256 VALUES LESS THAN (63687837776) ENGINE = InnoDB,
PARTITION before20180309180856 VALUES LESS THAN (63687838136) ENGINE = InnoDB,
PARTITION before20180309181456 VALUES LESS THAN (63687838496) ENGINE = InnoDB,
PARTITION before20180309182056 VALUES LESS THAN (63687838856) ENGINE = InnoDB,
PARTITION before20180309182656 VALUES LESS THAN (63687839216) ENGINE = InnoDB,
PARTITION before20180309183256 VALUES LESS THAN (63687839576) ENGINE = InnoDB,
PARTITION before20180309183856 VALUES LESS THAN (63687839936) ENGINE = InnoDB,
PARTITION before20180309184456 VALUES LESS THAN (63687840296) ENGINE = InnoDB,
PARTITION before20180309185056 VALUES LESS THAN (63687840656) ENGINE = InnoDB,
PARTITION before20180309185656 VALUES LESS THAN (63687841016) ENGINE = InnoDB,
PARTITION before20180309190256 VALUES LESS THAN (63687841376) ENGINE = InnoDB,
PARTITION before20180309190856 VALUES LESS THAN (63687841736) ENGINE = InnoDB,
PARTITION before20180309191456 VALUES LESS THAN (63687842096) ENGINE = InnoDB,
PARTITION before20180309192056 VALUES LESS THAN (63687842456) ENGINE = InnoDB,
PARTITION before20180309192656 VALUES LESS THAN (63687842816) ENGINE = InnoDB,
PARTITION before20180309193256 VALUES LESS THAN (63687843176) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_lp_primary_keys BEFORE INSERT ON lp_stats_latest
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
  `snapshot_time` date DEFAULT NULL,
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
PARTITION before20180110071304 VALUES LESS THAN (63682787584) ENGINE = InnoDB,
PARTITION before20180111210104 VALUES LESS THAN (63682923664) ENGINE = InnoDB,
PARTITION before20180113104904 VALUES LESS THAN (63683059744) ENGINE = InnoDB,
PARTITION before20180115003704 VALUES LESS THAN (63683195824) ENGINE = InnoDB,
PARTITION before20180116142504 VALUES LESS THAN (63683331904) ENGINE = InnoDB,
PARTITION before20180118041304 VALUES LESS THAN (63683467984) ENGINE = InnoDB,
PARTITION before20180119180104 VALUES LESS THAN (63683604064) ENGINE = InnoDB,
PARTITION before20180121074904 VALUES LESS THAN (63683740144) ENGINE = InnoDB,
PARTITION before20180122213704 VALUES LESS THAN (63683876224) ENGINE = InnoDB,
PARTITION before20180124112504 VALUES LESS THAN (63684012304) ENGINE = InnoDB,
PARTITION before20180126011304 VALUES LESS THAN (63684148384) ENGINE = InnoDB,
PARTITION before20180127150104 VALUES LESS THAN (63684284464) ENGINE = InnoDB,
PARTITION before20180129044904 VALUES LESS THAN (63684420544) ENGINE = InnoDB,
PARTITION before20180130183704 VALUES LESS THAN (63684556624) ENGINE = InnoDB,
PARTITION before20180201082504 VALUES LESS THAN (63684692704) ENGINE = InnoDB,
PARTITION before20180202221304 VALUES LESS THAN (63684828784) ENGINE = InnoDB,
PARTITION before20180204120104 VALUES LESS THAN (63684964864) ENGINE = InnoDB,
PARTITION before20180206014904 VALUES LESS THAN (63685100944) ENGINE = InnoDB,
PARTITION before20180207153704 VALUES LESS THAN (63685237024) ENGINE = InnoDB,
PARTITION before20180209052504 VALUES LESS THAN (63685373104) ENGINE = InnoDB,
PARTITION before20180210191304 VALUES LESS THAN (63685509184) ENGINE = InnoDB,
PARTITION before20180212090104 VALUES LESS THAN (63685645264) ENGINE = InnoDB,
PARTITION before20180213224904 VALUES LESS THAN (63685781344) ENGINE = InnoDB,
PARTITION before20180215123704 VALUES LESS THAN (63685917424) ENGINE = InnoDB,
PARTITION before20180217022504 VALUES LESS THAN (63686053504) ENGINE = InnoDB,
PARTITION before20180218161304 VALUES LESS THAN (63686189584) ENGINE = InnoDB,
PARTITION before20180220060104 VALUES LESS THAN (63686325664) ENGINE = InnoDB,
PARTITION before20180221194904 VALUES LESS THAN (63686461744) ENGINE = InnoDB,
PARTITION before20180223093704 VALUES LESS THAN (63686597824) ENGINE = InnoDB,
PARTITION before20180224232504 VALUES LESS THAN (63686733904) ENGINE = InnoDB,
PARTITION before20180226131304 VALUES LESS THAN (63686869984) ENGINE = InnoDB,
PARTITION before20180228030104 VALUES LESS THAN (63687006064) ENGINE = InnoDB,
PARTITION before20180301164904 VALUES LESS THAN (63687142144) ENGINE = InnoDB,
PARTITION before20180303063704 VALUES LESS THAN (63687278224) ENGINE = InnoDB,
PARTITION before20180304202504 VALUES LESS THAN (63687414304) ENGINE = InnoDB,
PARTITION before20180306101304 VALUES LESS THAN (63687550384) ENGINE = InnoDB,
PARTITION before20180308000104 VALUES LESS THAN (63687686464) ENGINE = InnoDB,
PARTITION before20180309134904 VALUES LESS THAN (63687822544) ENGINE = InnoDB,
PARTITION before20180311033704 VALUES LESS THAN (63687958624) ENGINE = InnoDB,
PARTITION before20180312172504 VALUES LESS THAN (63688094704) ENGINE = InnoDB,
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
PARTITION before20180306192505 VALUES LESS THAN (63687583505) ENGINE = InnoDB,
PARTITION before20180306212505 VALUES LESS THAN (63687590705) ENGINE = InnoDB,
PARTITION before20180306232505 VALUES LESS THAN (63687597905) ENGINE = InnoDB,
PARTITION before20180307012505 VALUES LESS THAN (63687605105) ENGINE = InnoDB,
PARTITION before20180307032505 VALUES LESS THAN (63687612305) ENGINE = InnoDB,
PARTITION before20180307052505 VALUES LESS THAN (63687619505) ENGINE = InnoDB,
PARTITION before20180307072505 VALUES LESS THAN (63687626705) ENGINE = InnoDB,
PARTITION before20180307092505 VALUES LESS THAN (63687633905) ENGINE = InnoDB,
PARTITION before20180307112505 VALUES LESS THAN (63687641105) ENGINE = InnoDB,
PARTITION before20180307132505 VALUES LESS THAN (63687648305) ENGINE = InnoDB,
PARTITION before20180307152505 VALUES LESS THAN (63687655505) ENGINE = InnoDB,
PARTITION before20180307172505 VALUES LESS THAN (63687662705) ENGINE = InnoDB,
PARTITION before20180307192505 VALUES LESS THAN (63687669905) ENGINE = InnoDB,
PARTITION before20180307212505 VALUES LESS THAN (63687677105) ENGINE = InnoDB,
PARTITION before20180307232505 VALUES LESS THAN (63687684305) ENGINE = InnoDB,
PARTITION before20180308012505 VALUES LESS THAN (63687691505) ENGINE = InnoDB,
PARTITION before20180308032505 VALUES LESS THAN (63687698705) ENGINE = InnoDB,
PARTITION before20180308052505 VALUES LESS THAN (63687705905) ENGINE = InnoDB,
PARTITION before20180308072505 VALUES LESS THAN (63687713105) ENGINE = InnoDB,
PARTITION before20180308092505 VALUES LESS THAN (63687720305) ENGINE = InnoDB,
PARTITION before20180308112505 VALUES LESS THAN (63687727505) ENGINE = InnoDB,
PARTITION before20180308132505 VALUES LESS THAN (63687734705) ENGINE = InnoDB,
PARTITION before20180308152505 VALUES LESS THAN (63687741905) ENGINE = InnoDB,
PARTITION before20180308172505 VALUES LESS THAN (63687749105) ENGINE = InnoDB,
PARTITION before20180308192505 VALUES LESS THAN (63687756305) ENGINE = InnoDB,
PARTITION before20180308212505 VALUES LESS THAN (63687763505) ENGINE = InnoDB,
PARTITION before20180308232505 VALUES LESS THAN (63687770705) ENGINE = InnoDB,
PARTITION before20180309012505 VALUES LESS THAN (63687777905) ENGINE = InnoDB,
PARTITION before20180309032505 VALUES LESS THAN (63687785105) ENGINE = InnoDB,
PARTITION before20180309052505 VALUES LESS THAN (63687792305) ENGINE = InnoDB,
PARTITION before20180309072505 VALUES LESS THAN (63687799505) ENGINE = InnoDB,
PARTITION before20180309092505 VALUES LESS THAN (63687806705) ENGINE = InnoDB,
PARTITION before20180309112505 VALUES LESS THAN (63687813905) ENGINE = InnoDB,
PARTITION before20180309132505 VALUES LESS THAN (63687821105) ENGINE = InnoDB,
PARTITION before20180309152505 VALUES LESS THAN (63687828305) ENGINE = InnoDB,
PARTITION before20180309172505 VALUES LESS THAN (63687835505) ENGINE = InnoDB,
PARTITION before20180309192505 VALUES LESS THAN (63687842705) ENGINE = InnoDB,
PARTITION before20180309212505 VALUES LESS THAN (63687849905) ENGINE = InnoDB,
PARTITION before20180309232505 VALUES LESS THAN (63687857105) ENGINE = InnoDB,
PARTITION before20180310012505 VALUES LESS THAN (63687864305) ENGINE = InnoDB,
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
  `snapshot_time` date DEFAULT NULL,
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
PARTITION before20160314093706 VALUES LESS THAN (63625167426) ENGINE = InnoDB,
PARTITION before20160402014906 VALUES LESS THAN (63626780946) ENGINE = InnoDB,
PARTITION before20160420180106 VALUES LESS THAN (63628394466) ENGINE = InnoDB,
PARTITION before20160509101306 VALUES LESS THAN (63630007986) ENGINE = InnoDB,
PARTITION before20160528022506 VALUES LESS THAN (63631621506) ENGINE = InnoDB,
PARTITION before20160615183706 VALUES LESS THAN (63633235026) ENGINE = InnoDB,
PARTITION before20160704104906 VALUES LESS THAN (63634848546) ENGINE = InnoDB,
PARTITION before20160723030106 VALUES LESS THAN (63636462066) ENGINE = InnoDB,
PARTITION before20160810191306 VALUES LESS THAN (63638075586) ENGINE = InnoDB,
PARTITION before20160829112506 VALUES LESS THAN (63639689106) ENGINE = InnoDB,
PARTITION before20160917033706 VALUES LESS THAN (63641302626) ENGINE = InnoDB,
PARTITION before20161005194906 VALUES LESS THAN (63642916146) ENGINE = InnoDB,
PARTITION before20161024120106 VALUES LESS THAN (63644529666) ENGINE = InnoDB,
PARTITION before20161112041306 VALUES LESS THAN (63646143186) ENGINE = InnoDB,
PARTITION before20161130202506 VALUES LESS THAN (63647756706) ENGINE = InnoDB,
PARTITION before20161219123706 VALUES LESS THAN (63649370226) ENGINE = InnoDB,
PARTITION before20170107044906 VALUES LESS THAN (63650983746) ENGINE = InnoDB,
PARTITION before20170125210106 VALUES LESS THAN (63652597266) ENGINE = InnoDB,
PARTITION before20170213131306 VALUES LESS THAN (63654210786) ENGINE = InnoDB,
PARTITION before20170304052506 VALUES LESS THAN (63655824306) ENGINE = InnoDB,
PARTITION before20170322213706 VALUES LESS THAN (63657437826) ENGINE = InnoDB,
PARTITION before20170410134906 VALUES LESS THAN (63659051346) ENGINE = InnoDB,
PARTITION before20170429060106 VALUES LESS THAN (63660664866) ENGINE = InnoDB,
PARTITION before20170517221306 VALUES LESS THAN (63662278386) ENGINE = InnoDB,
PARTITION before20170605142506 VALUES LESS THAN (63663891906) ENGINE = InnoDB,
PARTITION before20170624063706 VALUES LESS THAN (63665505426) ENGINE = InnoDB,
PARTITION before20170712224906 VALUES LESS THAN (63667118946) ENGINE = InnoDB,
PARTITION before20170731150106 VALUES LESS THAN (63668732466) ENGINE = InnoDB,
PARTITION before20170819071306 VALUES LESS THAN (63670345986) ENGINE = InnoDB,
PARTITION before20170906232506 VALUES LESS THAN (63671959506) ENGINE = InnoDB,
PARTITION before20170925153706 VALUES LESS THAN (63673573026) ENGINE = InnoDB,
PARTITION before20171014074906 VALUES LESS THAN (63675186546) ENGINE = InnoDB,
PARTITION before20171102000106 VALUES LESS THAN (63676800066) ENGINE = InnoDB,
PARTITION before20171120161306 VALUES LESS THAN (63678413586) ENGINE = InnoDB,
PARTITION before20171209082506 VALUES LESS THAN (63680027106) ENGINE = InnoDB,
PARTITION before20171228003706 VALUES LESS THAN (63681640626) ENGINE = InnoDB,
PARTITION before20180115164906 VALUES LESS THAN (63683254146) ENGINE = InnoDB,
PARTITION before20180203090106 VALUES LESS THAN (63684867666) ENGINE = InnoDB,
PARTITION before20180222011306 VALUES LESS THAN (63686481186) ENGINE = InnoDB,
PARTITION before20180312172506 VALUES LESS THAN (63688094706) ENGINE = InnoDB,
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
PARTITION before20180309153702 VALUES LESS THAN (63687829022) ENGINE = InnoDB,
PARTITION before20180309154302 VALUES LESS THAN (63687829382) ENGINE = InnoDB,
PARTITION before20180309154902 VALUES LESS THAN (63687829742) ENGINE = InnoDB,
PARTITION before20180309155502 VALUES LESS THAN (63687830102) ENGINE = InnoDB,
PARTITION before20180309160102 VALUES LESS THAN (63687830462) ENGINE = InnoDB,
PARTITION before20180309160702 VALUES LESS THAN (63687830822) ENGINE = InnoDB,
PARTITION before20180309161302 VALUES LESS THAN (63687831182) ENGINE = InnoDB,
PARTITION before20180309161902 VALUES LESS THAN (63687831542) ENGINE = InnoDB,
PARTITION before20180309162502 VALUES LESS THAN (63687831902) ENGINE = InnoDB,
PARTITION before20180309163102 VALUES LESS THAN (63687832262) ENGINE = InnoDB,
PARTITION before20180309163702 VALUES LESS THAN (63687832622) ENGINE = InnoDB,
PARTITION before20180309164302 VALUES LESS THAN (63687832982) ENGINE = InnoDB,
PARTITION before20180309164902 VALUES LESS THAN (63687833342) ENGINE = InnoDB,
PARTITION before20180309165502 VALUES LESS THAN (63687833702) ENGINE = InnoDB,
PARTITION before20180309170102 VALUES LESS THAN (63687834062) ENGINE = InnoDB,
PARTITION before20180309170702 VALUES LESS THAN (63687834422) ENGINE = InnoDB,
PARTITION before20180309171302 VALUES LESS THAN (63687834782) ENGINE = InnoDB,
PARTITION before20180309171902 VALUES LESS THAN (63687835142) ENGINE = InnoDB,
PARTITION before20180309172502 VALUES LESS THAN (63687835502) ENGINE = InnoDB,
PARTITION before20180309173102 VALUES LESS THAN (63687835862) ENGINE = InnoDB,
PARTITION before20180309173702 VALUES LESS THAN (63687836222) ENGINE = InnoDB,
PARTITION before20180309174302 VALUES LESS THAN (63687836582) ENGINE = InnoDB,
PARTITION before20180309174902 VALUES LESS THAN (63687836942) ENGINE = InnoDB,
PARTITION before20180309175502 VALUES LESS THAN (63687837302) ENGINE = InnoDB,
PARTITION before20180309180102 VALUES LESS THAN (63687837662) ENGINE = InnoDB,
PARTITION before20180309180702 VALUES LESS THAN (63687838022) ENGINE = InnoDB,
PARTITION before20180309181302 VALUES LESS THAN (63687838382) ENGINE = InnoDB,
PARTITION before20180309181902 VALUES LESS THAN (63687838742) ENGINE = InnoDB,
PARTITION before20180309182502 VALUES LESS THAN (63687839102) ENGINE = InnoDB,
PARTITION before20180309183102 VALUES LESS THAN (63687839462) ENGINE = InnoDB,
PARTITION before20180309183702 VALUES LESS THAN (63687839822) ENGINE = InnoDB,
PARTITION before20180309184302 VALUES LESS THAN (63687840182) ENGINE = InnoDB,
PARTITION before20180309184902 VALUES LESS THAN (63687840542) ENGINE = InnoDB,
PARTITION before20180309185502 VALUES LESS THAN (63687840902) ENGINE = InnoDB,
PARTITION before20180309190102 VALUES LESS THAN (63687841262) ENGINE = InnoDB,
PARTITION before20180309190702 VALUES LESS THAN (63687841622) ENGINE = InnoDB,
PARTITION before20180309191302 VALUES LESS THAN (63687841982) ENGINE = InnoDB,
PARTITION before20180309191902 VALUES LESS THAN (63687842342) ENGINE = InnoDB,
PARTITION before20180309192502 VALUES LESS THAN (63687842702) ENGINE = InnoDB,
PARTITION before20180309193102 VALUES LESS THAN (63687843062) ENGINE = InnoDB,
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
PARTITION before20180110071328 VALUES LESS THAN (63682787608) ENGINE = InnoDB,
PARTITION before20180111210128 VALUES LESS THAN (63682923688) ENGINE = InnoDB,
PARTITION before20180113104928 VALUES LESS THAN (63683059768) ENGINE = InnoDB,
PARTITION before20180115003728 VALUES LESS THAN (63683195848) ENGINE = InnoDB,
PARTITION before20180116142528 VALUES LESS THAN (63683331928) ENGINE = InnoDB,
PARTITION before20180118041328 VALUES LESS THAN (63683468008) ENGINE = InnoDB,
PARTITION before20180119180128 VALUES LESS THAN (63683604088) ENGINE = InnoDB,
PARTITION before20180121074928 VALUES LESS THAN (63683740168) ENGINE = InnoDB,
PARTITION before20180122213728 VALUES LESS THAN (63683876248) ENGINE = InnoDB,
PARTITION before20180124112528 VALUES LESS THAN (63684012328) ENGINE = InnoDB,
PARTITION before20180126011328 VALUES LESS THAN (63684148408) ENGINE = InnoDB,
PARTITION before20180127150128 VALUES LESS THAN (63684284488) ENGINE = InnoDB,
PARTITION before20180129044928 VALUES LESS THAN (63684420568) ENGINE = InnoDB,
PARTITION before20180130183728 VALUES LESS THAN (63684556648) ENGINE = InnoDB,
PARTITION before20180201082528 VALUES LESS THAN (63684692728) ENGINE = InnoDB,
PARTITION before20180202221328 VALUES LESS THAN (63684828808) ENGINE = InnoDB,
PARTITION before20180204120128 VALUES LESS THAN (63684964888) ENGINE = InnoDB,
PARTITION before20180206014928 VALUES LESS THAN (63685100968) ENGINE = InnoDB,
PARTITION before20180207153728 VALUES LESS THAN (63685237048) ENGINE = InnoDB,
PARTITION before20180209052528 VALUES LESS THAN (63685373128) ENGINE = InnoDB,
PARTITION before20180210191328 VALUES LESS THAN (63685509208) ENGINE = InnoDB,
PARTITION before20180212090128 VALUES LESS THAN (63685645288) ENGINE = InnoDB,
PARTITION before20180213224928 VALUES LESS THAN (63685781368) ENGINE = InnoDB,
PARTITION before20180215123728 VALUES LESS THAN (63685917448) ENGINE = InnoDB,
PARTITION before20180217022528 VALUES LESS THAN (63686053528) ENGINE = InnoDB,
PARTITION before20180218161328 VALUES LESS THAN (63686189608) ENGINE = InnoDB,
PARTITION before20180220060128 VALUES LESS THAN (63686325688) ENGINE = InnoDB,
PARTITION before20180221194928 VALUES LESS THAN (63686461768) ENGINE = InnoDB,
PARTITION before20180223093728 VALUES LESS THAN (63686597848) ENGINE = InnoDB,
PARTITION before20180224232528 VALUES LESS THAN (63686733928) ENGINE = InnoDB,
PARTITION before20180226131328 VALUES LESS THAN (63686870008) ENGINE = InnoDB,
PARTITION before20180228030128 VALUES LESS THAN (63687006088) ENGINE = InnoDB,
PARTITION before20180301164928 VALUES LESS THAN (63687142168) ENGINE = InnoDB,
PARTITION before20180303063728 VALUES LESS THAN (63687278248) ENGINE = InnoDB,
PARTITION before20180304202528 VALUES LESS THAN (63687414328) ENGINE = InnoDB,
PARTITION before20180306101328 VALUES LESS THAN (63687550408) ENGINE = InnoDB,
PARTITION before20180308000128 VALUES LESS THAN (63687686488) ENGINE = InnoDB,
PARTITION before20180309134928 VALUES LESS THAN (63687822568) ENGINE = InnoDB,
PARTITION before20180311033728 VALUES LESS THAN (63687958648) ENGINE = InnoDB,
PARTITION before20180312172528 VALUES LESS THAN (63688094728) ENGINE = InnoDB,
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
PARTITION before20180306192529 VALUES LESS THAN (63687583529) ENGINE = InnoDB,
PARTITION before20180306212529 VALUES LESS THAN (63687590729) ENGINE = InnoDB,
PARTITION before20180306232529 VALUES LESS THAN (63687597929) ENGINE = InnoDB,
PARTITION before20180307012529 VALUES LESS THAN (63687605129) ENGINE = InnoDB,
PARTITION before20180307032529 VALUES LESS THAN (63687612329) ENGINE = InnoDB,
PARTITION before20180307052529 VALUES LESS THAN (63687619529) ENGINE = InnoDB,
PARTITION before20180307072529 VALUES LESS THAN (63687626729) ENGINE = InnoDB,
PARTITION before20180307092529 VALUES LESS THAN (63687633929) ENGINE = InnoDB,
PARTITION before20180307112529 VALUES LESS THAN (63687641129) ENGINE = InnoDB,
PARTITION before20180307132529 VALUES LESS THAN (63687648329) ENGINE = InnoDB,
PARTITION before20180307152529 VALUES LESS THAN (63687655529) ENGINE = InnoDB,
PARTITION before20180307172529 VALUES LESS THAN (63687662729) ENGINE = InnoDB,
PARTITION before20180307192529 VALUES LESS THAN (63687669929) ENGINE = InnoDB,
PARTITION before20180307212529 VALUES LESS THAN (63687677129) ENGINE = InnoDB,
PARTITION before20180307232529 VALUES LESS THAN (63687684329) ENGINE = InnoDB,
PARTITION before20180308012529 VALUES LESS THAN (63687691529) ENGINE = InnoDB,
PARTITION before20180308032529 VALUES LESS THAN (63687698729) ENGINE = InnoDB,
PARTITION before20180308052529 VALUES LESS THAN (63687705929) ENGINE = InnoDB,
PARTITION before20180308072529 VALUES LESS THAN (63687713129) ENGINE = InnoDB,
PARTITION before20180308092529 VALUES LESS THAN (63687720329) ENGINE = InnoDB,
PARTITION before20180308112529 VALUES LESS THAN (63687727529) ENGINE = InnoDB,
PARTITION before20180308132529 VALUES LESS THAN (63687734729) ENGINE = InnoDB,
PARTITION before20180308152529 VALUES LESS THAN (63687741929) ENGINE = InnoDB,
PARTITION before20180308172529 VALUES LESS THAN (63687749129) ENGINE = InnoDB,
PARTITION before20180308192529 VALUES LESS THAN (63687756329) ENGINE = InnoDB,
PARTITION before20180308212529 VALUES LESS THAN (63687763529) ENGINE = InnoDB,
PARTITION before20180308232529 VALUES LESS THAN (63687770729) ENGINE = InnoDB,
PARTITION before20180309012529 VALUES LESS THAN (63687777929) ENGINE = InnoDB,
PARTITION before20180309032529 VALUES LESS THAN (63687785129) ENGINE = InnoDB,
PARTITION before20180309052529 VALUES LESS THAN (63687792329) ENGINE = InnoDB,
PARTITION before20180309072529 VALUES LESS THAN (63687799529) ENGINE = InnoDB,
PARTITION before20180309092529 VALUES LESS THAN (63687806729) ENGINE = InnoDB,
PARTITION before20180309112529 VALUES LESS THAN (63687813929) ENGINE = InnoDB,
PARTITION before20180309132529 VALUES LESS THAN (63687821129) ENGINE = InnoDB,
PARTITION before20180309152529 VALUES LESS THAN (63687828329) ENGINE = InnoDB,
PARTITION before20180309172529 VALUES LESS THAN (63687835529) ENGINE = InnoDB,
PARTITION before20180309192529 VALUES LESS THAN (63687842729) ENGINE = InnoDB,
PARTITION before20180309212529 VALUES LESS THAN (63687849929) ENGINE = InnoDB,
PARTITION before20180309232529 VALUES LESS THAN (63687857129) ENGINE = InnoDB,
PARTITION before20180310012529 VALUES LESS THAN (63687864329) ENGINE = InnoDB,
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
PARTITION before20160314093730 VALUES LESS THAN (63625167450) ENGINE = InnoDB,
PARTITION before20160402014930 VALUES LESS THAN (63626780970) ENGINE = InnoDB,
PARTITION before20160420180130 VALUES LESS THAN (63628394490) ENGINE = InnoDB,
PARTITION before20160509101330 VALUES LESS THAN (63630008010) ENGINE = InnoDB,
PARTITION before20160528022530 VALUES LESS THAN (63631621530) ENGINE = InnoDB,
PARTITION before20160615183730 VALUES LESS THAN (63633235050) ENGINE = InnoDB,
PARTITION before20160704104930 VALUES LESS THAN (63634848570) ENGINE = InnoDB,
PARTITION before20160723030130 VALUES LESS THAN (63636462090) ENGINE = InnoDB,
PARTITION before20160810191330 VALUES LESS THAN (63638075610) ENGINE = InnoDB,
PARTITION before20160829112530 VALUES LESS THAN (63639689130) ENGINE = InnoDB,
PARTITION before20160917033730 VALUES LESS THAN (63641302650) ENGINE = InnoDB,
PARTITION before20161005194930 VALUES LESS THAN (63642916170) ENGINE = InnoDB,
PARTITION before20161024120130 VALUES LESS THAN (63644529690) ENGINE = InnoDB,
PARTITION before20161112041330 VALUES LESS THAN (63646143210) ENGINE = InnoDB,
PARTITION before20161130202530 VALUES LESS THAN (63647756730) ENGINE = InnoDB,
PARTITION before20161219123730 VALUES LESS THAN (63649370250) ENGINE = InnoDB,
PARTITION before20170107044930 VALUES LESS THAN (63650983770) ENGINE = InnoDB,
PARTITION before20170125210130 VALUES LESS THAN (63652597290) ENGINE = InnoDB,
PARTITION before20170213131330 VALUES LESS THAN (63654210810) ENGINE = InnoDB,
PARTITION before20170304052530 VALUES LESS THAN (63655824330) ENGINE = InnoDB,
PARTITION before20170322213730 VALUES LESS THAN (63657437850) ENGINE = InnoDB,
PARTITION before20170410134930 VALUES LESS THAN (63659051370) ENGINE = InnoDB,
PARTITION before20170429060130 VALUES LESS THAN (63660664890) ENGINE = InnoDB,
PARTITION before20170517221330 VALUES LESS THAN (63662278410) ENGINE = InnoDB,
PARTITION before20170605142530 VALUES LESS THAN (63663891930) ENGINE = InnoDB,
PARTITION before20170624063730 VALUES LESS THAN (63665505450) ENGINE = InnoDB,
PARTITION before20170712224930 VALUES LESS THAN (63667118970) ENGINE = InnoDB,
PARTITION before20170731150130 VALUES LESS THAN (63668732490) ENGINE = InnoDB,
PARTITION before20170819071330 VALUES LESS THAN (63670346010) ENGINE = InnoDB,
PARTITION before20170906232530 VALUES LESS THAN (63671959530) ENGINE = InnoDB,
PARTITION before20170925153730 VALUES LESS THAN (63673573050) ENGINE = InnoDB,
PARTITION before20171014074930 VALUES LESS THAN (63675186570) ENGINE = InnoDB,
PARTITION before20171102000130 VALUES LESS THAN (63676800090) ENGINE = InnoDB,
PARTITION before20171120161330 VALUES LESS THAN (63678413610) ENGINE = InnoDB,
PARTITION before20171209082530 VALUES LESS THAN (63680027130) ENGINE = InnoDB,
PARTITION before20171228003730 VALUES LESS THAN (63681640650) ENGINE = InnoDB,
PARTITION before20180115164930 VALUES LESS THAN (63683254170) ENGINE = InnoDB,
PARTITION before20180203090130 VALUES LESS THAN (63684867690) ENGINE = InnoDB,
PARTITION before20180222011330 VALUES LESS THAN (63686481210) ENGINE = InnoDB,
PARTITION before20180312172530 VALUES LESS THAN (63688094730) ENGINE = InnoDB,
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
PARTITION before20180309153727 VALUES LESS THAN (63687829047) ENGINE = InnoDB,
PARTITION before20180309154327 VALUES LESS THAN (63687829407) ENGINE = InnoDB,
PARTITION before20180309154927 VALUES LESS THAN (63687829767) ENGINE = InnoDB,
PARTITION before20180309155527 VALUES LESS THAN (63687830127) ENGINE = InnoDB,
PARTITION before20180309160127 VALUES LESS THAN (63687830487) ENGINE = InnoDB,
PARTITION before20180309160727 VALUES LESS THAN (63687830847) ENGINE = InnoDB,
PARTITION before20180309161327 VALUES LESS THAN (63687831207) ENGINE = InnoDB,
PARTITION before20180309161927 VALUES LESS THAN (63687831567) ENGINE = InnoDB,
PARTITION before20180309162527 VALUES LESS THAN (63687831927) ENGINE = InnoDB,
PARTITION before20180309163127 VALUES LESS THAN (63687832287) ENGINE = InnoDB,
PARTITION before20180309163727 VALUES LESS THAN (63687832647) ENGINE = InnoDB,
PARTITION before20180309164327 VALUES LESS THAN (63687833007) ENGINE = InnoDB,
PARTITION before20180309164927 VALUES LESS THAN (63687833367) ENGINE = InnoDB,
PARTITION before20180309165527 VALUES LESS THAN (63687833727) ENGINE = InnoDB,
PARTITION before20180309170127 VALUES LESS THAN (63687834087) ENGINE = InnoDB,
PARTITION before20180309170727 VALUES LESS THAN (63687834447) ENGINE = InnoDB,
PARTITION before20180309171327 VALUES LESS THAN (63687834807) ENGINE = InnoDB,
PARTITION before20180309171927 VALUES LESS THAN (63687835167) ENGINE = InnoDB,
PARTITION before20180309172527 VALUES LESS THAN (63687835527) ENGINE = InnoDB,
PARTITION before20180309173127 VALUES LESS THAN (63687835887) ENGINE = InnoDB,
PARTITION before20180309173727 VALUES LESS THAN (63687836247) ENGINE = InnoDB,
PARTITION before20180309174327 VALUES LESS THAN (63687836607) ENGINE = InnoDB,
PARTITION before20180309174927 VALUES LESS THAN (63687836967) ENGINE = InnoDB,
PARTITION before20180309175527 VALUES LESS THAN (63687837327) ENGINE = InnoDB,
PARTITION before20180309180127 VALUES LESS THAN (63687837687) ENGINE = InnoDB,
PARTITION before20180309180727 VALUES LESS THAN (63687838047) ENGINE = InnoDB,
PARTITION before20180309181327 VALUES LESS THAN (63687838407) ENGINE = InnoDB,
PARTITION before20180309181927 VALUES LESS THAN (63687838767) ENGINE = InnoDB,
PARTITION before20180309182527 VALUES LESS THAN (63687839127) ENGINE = InnoDB,
PARTITION before20180309183127 VALUES LESS THAN (63687839487) ENGINE = InnoDB,
PARTITION before20180309183727 VALUES LESS THAN (63687839847) ENGINE = InnoDB,
PARTITION before20180309184327 VALUES LESS THAN (63687840207) ENGINE = InnoDB,
PARTITION before20180309184927 VALUES LESS THAN (63687840567) ENGINE = InnoDB,
PARTITION before20180309185527 VALUES LESS THAN (63687840927) ENGINE = InnoDB,
PARTITION before20180309190127 VALUES LESS THAN (63687841287) ENGINE = InnoDB,
PARTITION before20180309190727 VALUES LESS THAN (63687841647) ENGINE = InnoDB,
PARTITION before20180309191327 VALUES LESS THAN (63687842007) ENGINE = InnoDB,
PARTITION before20180309191927 VALUES LESS THAN (63687842367) ENGINE = InnoDB,
PARTITION before20180309192527 VALUES LESS THAN (63687842727) ENGINE = InnoDB,
PARTITION before20180309193127 VALUES LESS THAN (63687843087) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_pm_primary_keys BEFORE INSERT ON pm_stats_latest
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
PARTITION before20180110071332 VALUES LESS THAN (63682787612) ENGINE = InnoDB,
PARTITION before20180111210132 VALUES LESS THAN (63682923692) ENGINE = InnoDB,
PARTITION before20180113104932 VALUES LESS THAN (63683059772) ENGINE = InnoDB,
PARTITION before20180115003732 VALUES LESS THAN (63683195852) ENGINE = InnoDB,
PARTITION before20180116142532 VALUES LESS THAN (63683331932) ENGINE = InnoDB,
PARTITION before20180118041332 VALUES LESS THAN (63683468012) ENGINE = InnoDB,
PARTITION before20180119180132 VALUES LESS THAN (63683604092) ENGINE = InnoDB,
PARTITION before20180121074932 VALUES LESS THAN (63683740172) ENGINE = InnoDB,
PARTITION before20180122213732 VALUES LESS THAN (63683876252) ENGINE = InnoDB,
PARTITION before20180124112532 VALUES LESS THAN (63684012332) ENGINE = InnoDB,
PARTITION before20180126011332 VALUES LESS THAN (63684148412) ENGINE = InnoDB,
PARTITION before20180127150132 VALUES LESS THAN (63684284492) ENGINE = InnoDB,
PARTITION before20180129044932 VALUES LESS THAN (63684420572) ENGINE = InnoDB,
PARTITION before20180130183732 VALUES LESS THAN (63684556652) ENGINE = InnoDB,
PARTITION before20180201082532 VALUES LESS THAN (63684692732) ENGINE = InnoDB,
PARTITION before20180202221332 VALUES LESS THAN (63684828812) ENGINE = InnoDB,
PARTITION before20180204120132 VALUES LESS THAN (63684964892) ENGINE = InnoDB,
PARTITION before20180206014932 VALUES LESS THAN (63685100972) ENGINE = InnoDB,
PARTITION before20180207153732 VALUES LESS THAN (63685237052) ENGINE = InnoDB,
PARTITION before20180209052532 VALUES LESS THAN (63685373132) ENGINE = InnoDB,
PARTITION before20180210191332 VALUES LESS THAN (63685509212) ENGINE = InnoDB,
PARTITION before20180212090132 VALUES LESS THAN (63685645292) ENGINE = InnoDB,
PARTITION before20180213224932 VALUES LESS THAN (63685781372) ENGINE = InnoDB,
PARTITION before20180215123732 VALUES LESS THAN (63685917452) ENGINE = InnoDB,
PARTITION before20180217022532 VALUES LESS THAN (63686053532) ENGINE = InnoDB,
PARTITION before20180218161332 VALUES LESS THAN (63686189612) ENGINE = InnoDB,
PARTITION before20180220060132 VALUES LESS THAN (63686325692) ENGINE = InnoDB,
PARTITION before20180221194932 VALUES LESS THAN (63686461772) ENGINE = InnoDB,
PARTITION before20180223093732 VALUES LESS THAN (63686597852) ENGINE = InnoDB,
PARTITION before20180224232532 VALUES LESS THAN (63686733932) ENGINE = InnoDB,
PARTITION before20180226131332 VALUES LESS THAN (63686870012) ENGINE = InnoDB,
PARTITION before20180228030132 VALUES LESS THAN (63687006092) ENGINE = InnoDB,
PARTITION before20180301164932 VALUES LESS THAN (63687142172) ENGINE = InnoDB,
PARTITION before20180303063732 VALUES LESS THAN (63687278252) ENGINE = InnoDB,
PARTITION before20180304202532 VALUES LESS THAN (63687414332) ENGINE = InnoDB,
PARTITION before20180306101332 VALUES LESS THAN (63687550412) ENGINE = InnoDB,
PARTITION before20180308000132 VALUES LESS THAN (63687686492) ENGINE = InnoDB,
PARTITION before20180309134932 VALUES LESS THAN (63687822572) ENGINE = InnoDB,
PARTITION before20180311033732 VALUES LESS THAN (63687958652) ENGINE = InnoDB,
PARTITION before20180312172532 VALUES LESS THAN (63688094732) ENGINE = InnoDB,
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
PARTITION before20180306192533 VALUES LESS THAN (63687583533) ENGINE = InnoDB,
PARTITION before20180306212533 VALUES LESS THAN (63687590733) ENGINE = InnoDB,
PARTITION before20180306232533 VALUES LESS THAN (63687597933) ENGINE = InnoDB,
PARTITION before20180307012533 VALUES LESS THAN (63687605133) ENGINE = InnoDB,
PARTITION before20180307032533 VALUES LESS THAN (63687612333) ENGINE = InnoDB,
PARTITION before20180307052533 VALUES LESS THAN (63687619533) ENGINE = InnoDB,
PARTITION before20180307072533 VALUES LESS THAN (63687626733) ENGINE = InnoDB,
PARTITION before20180307092533 VALUES LESS THAN (63687633933) ENGINE = InnoDB,
PARTITION before20180307112533 VALUES LESS THAN (63687641133) ENGINE = InnoDB,
PARTITION before20180307132533 VALUES LESS THAN (63687648333) ENGINE = InnoDB,
PARTITION before20180307152533 VALUES LESS THAN (63687655533) ENGINE = InnoDB,
PARTITION before20180307172533 VALUES LESS THAN (63687662733) ENGINE = InnoDB,
PARTITION before20180307192533 VALUES LESS THAN (63687669933) ENGINE = InnoDB,
PARTITION before20180307212533 VALUES LESS THAN (63687677133) ENGINE = InnoDB,
PARTITION before20180307232533 VALUES LESS THAN (63687684333) ENGINE = InnoDB,
PARTITION before20180308012533 VALUES LESS THAN (63687691533) ENGINE = InnoDB,
PARTITION before20180308032533 VALUES LESS THAN (63687698733) ENGINE = InnoDB,
PARTITION before20180308052533 VALUES LESS THAN (63687705933) ENGINE = InnoDB,
PARTITION before20180308072533 VALUES LESS THAN (63687713133) ENGINE = InnoDB,
PARTITION before20180308092533 VALUES LESS THAN (63687720333) ENGINE = InnoDB,
PARTITION before20180308112533 VALUES LESS THAN (63687727533) ENGINE = InnoDB,
PARTITION before20180308132533 VALUES LESS THAN (63687734733) ENGINE = InnoDB,
PARTITION before20180308152533 VALUES LESS THAN (63687741933) ENGINE = InnoDB,
PARTITION before20180308172533 VALUES LESS THAN (63687749133) ENGINE = InnoDB,
PARTITION before20180308192533 VALUES LESS THAN (63687756333) ENGINE = InnoDB,
PARTITION before20180308212533 VALUES LESS THAN (63687763533) ENGINE = InnoDB,
PARTITION before20180308232533 VALUES LESS THAN (63687770733) ENGINE = InnoDB,
PARTITION before20180309012533 VALUES LESS THAN (63687777933) ENGINE = InnoDB,
PARTITION before20180309032533 VALUES LESS THAN (63687785133) ENGINE = InnoDB,
PARTITION before20180309052533 VALUES LESS THAN (63687792333) ENGINE = InnoDB,
PARTITION before20180309072533 VALUES LESS THAN (63687799533) ENGINE = InnoDB,
PARTITION before20180309092533 VALUES LESS THAN (63687806733) ENGINE = InnoDB,
PARTITION before20180309112533 VALUES LESS THAN (63687813933) ENGINE = InnoDB,
PARTITION before20180309132533 VALUES LESS THAN (63687821133) ENGINE = InnoDB,
PARTITION before20180309152533 VALUES LESS THAN (63687828333) ENGINE = InnoDB,
PARTITION before20180309172533 VALUES LESS THAN (63687835533) ENGINE = InnoDB,
PARTITION before20180309192533 VALUES LESS THAN (63687842733) ENGINE = InnoDB,
PARTITION before20180309212533 VALUES LESS THAN (63687849933) ENGINE = InnoDB,
PARTITION before20180309232533 VALUES LESS THAN (63687857133) ENGINE = InnoDB,
PARTITION before20180310012533 VALUES LESS THAN (63687864333) ENGINE = InnoDB,
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
PARTITION before20160314093733 VALUES LESS THAN (63625167453) ENGINE = InnoDB,
PARTITION before20160402014933 VALUES LESS THAN (63626780973) ENGINE = InnoDB,
PARTITION before20160420180133 VALUES LESS THAN (63628394493) ENGINE = InnoDB,
PARTITION before20160509101333 VALUES LESS THAN (63630008013) ENGINE = InnoDB,
PARTITION before20160528022533 VALUES LESS THAN (63631621533) ENGINE = InnoDB,
PARTITION before20160615183733 VALUES LESS THAN (63633235053) ENGINE = InnoDB,
PARTITION before20160704104933 VALUES LESS THAN (63634848573) ENGINE = InnoDB,
PARTITION before20160723030133 VALUES LESS THAN (63636462093) ENGINE = InnoDB,
PARTITION before20160810191333 VALUES LESS THAN (63638075613) ENGINE = InnoDB,
PARTITION before20160829112533 VALUES LESS THAN (63639689133) ENGINE = InnoDB,
PARTITION before20160917033733 VALUES LESS THAN (63641302653) ENGINE = InnoDB,
PARTITION before20161005194933 VALUES LESS THAN (63642916173) ENGINE = InnoDB,
PARTITION before20161024120133 VALUES LESS THAN (63644529693) ENGINE = InnoDB,
PARTITION before20161112041333 VALUES LESS THAN (63646143213) ENGINE = InnoDB,
PARTITION before20161130202533 VALUES LESS THAN (63647756733) ENGINE = InnoDB,
PARTITION before20161219123733 VALUES LESS THAN (63649370253) ENGINE = InnoDB,
PARTITION before20170107044933 VALUES LESS THAN (63650983773) ENGINE = InnoDB,
PARTITION before20170125210133 VALUES LESS THAN (63652597293) ENGINE = InnoDB,
PARTITION before20170213131333 VALUES LESS THAN (63654210813) ENGINE = InnoDB,
PARTITION before20170304052533 VALUES LESS THAN (63655824333) ENGINE = InnoDB,
PARTITION before20170322213733 VALUES LESS THAN (63657437853) ENGINE = InnoDB,
PARTITION before20170410134933 VALUES LESS THAN (63659051373) ENGINE = InnoDB,
PARTITION before20170429060133 VALUES LESS THAN (63660664893) ENGINE = InnoDB,
PARTITION before20170517221333 VALUES LESS THAN (63662278413) ENGINE = InnoDB,
PARTITION before20170605142533 VALUES LESS THAN (63663891933) ENGINE = InnoDB,
PARTITION before20170624063733 VALUES LESS THAN (63665505453) ENGINE = InnoDB,
PARTITION before20170712224933 VALUES LESS THAN (63667118973) ENGINE = InnoDB,
PARTITION before20170731150133 VALUES LESS THAN (63668732493) ENGINE = InnoDB,
PARTITION before20170819071333 VALUES LESS THAN (63670346013) ENGINE = InnoDB,
PARTITION before20170906232533 VALUES LESS THAN (63671959533) ENGINE = InnoDB,
PARTITION before20170925153733 VALUES LESS THAN (63673573053) ENGINE = InnoDB,
PARTITION before20171014074933 VALUES LESS THAN (63675186573) ENGINE = InnoDB,
PARTITION before20171102000133 VALUES LESS THAN (63676800093) ENGINE = InnoDB,
PARTITION before20171120161333 VALUES LESS THAN (63678413613) ENGINE = InnoDB,
PARTITION before20171209082533 VALUES LESS THAN (63680027133) ENGINE = InnoDB,
PARTITION before20171228003733 VALUES LESS THAN (63681640653) ENGINE = InnoDB,
PARTITION before20180115164933 VALUES LESS THAN (63683254173) ENGINE = InnoDB,
PARTITION before20180203090133 VALUES LESS THAN (63684867693) ENGINE = InnoDB,
PARTITION before20180222011333 VALUES LESS THAN (63686481213) ENGINE = InnoDB,
PARTITION before20180312172533 VALUES LESS THAN (63688094733) ENGINE = InnoDB,
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
PARTITION before20180309153731 VALUES LESS THAN (63687829051) ENGINE = InnoDB,
PARTITION before20180309154331 VALUES LESS THAN (63687829411) ENGINE = InnoDB,
PARTITION before20180309154931 VALUES LESS THAN (63687829771) ENGINE = InnoDB,
PARTITION before20180309155531 VALUES LESS THAN (63687830131) ENGINE = InnoDB,
PARTITION before20180309160131 VALUES LESS THAN (63687830491) ENGINE = InnoDB,
PARTITION before20180309160731 VALUES LESS THAN (63687830851) ENGINE = InnoDB,
PARTITION before20180309161331 VALUES LESS THAN (63687831211) ENGINE = InnoDB,
PARTITION before20180309161931 VALUES LESS THAN (63687831571) ENGINE = InnoDB,
PARTITION before20180309162531 VALUES LESS THAN (63687831931) ENGINE = InnoDB,
PARTITION before20180309163131 VALUES LESS THAN (63687832291) ENGINE = InnoDB,
PARTITION before20180309163731 VALUES LESS THAN (63687832651) ENGINE = InnoDB,
PARTITION before20180309164331 VALUES LESS THAN (63687833011) ENGINE = InnoDB,
PARTITION before20180309164931 VALUES LESS THAN (63687833371) ENGINE = InnoDB,
PARTITION before20180309165531 VALUES LESS THAN (63687833731) ENGINE = InnoDB,
PARTITION before20180309170131 VALUES LESS THAN (63687834091) ENGINE = InnoDB,
PARTITION before20180309170731 VALUES LESS THAN (63687834451) ENGINE = InnoDB,
PARTITION before20180309171331 VALUES LESS THAN (63687834811) ENGINE = InnoDB,
PARTITION before20180309171931 VALUES LESS THAN (63687835171) ENGINE = InnoDB,
PARTITION before20180309172531 VALUES LESS THAN (63687835531) ENGINE = InnoDB,
PARTITION before20180309173131 VALUES LESS THAN (63687835891) ENGINE = InnoDB,
PARTITION before20180309173731 VALUES LESS THAN (63687836251) ENGINE = InnoDB,
PARTITION before20180309174331 VALUES LESS THAN (63687836611) ENGINE = InnoDB,
PARTITION before20180309174931 VALUES LESS THAN (63687836971) ENGINE = InnoDB,
PARTITION before20180309175531 VALUES LESS THAN (63687837331) ENGINE = InnoDB,
PARTITION before20180309180131 VALUES LESS THAN (63687837691) ENGINE = InnoDB,
PARTITION before20180309180731 VALUES LESS THAN (63687838051) ENGINE = InnoDB,
PARTITION before20180309181331 VALUES LESS THAN (63687838411) ENGINE = InnoDB,
PARTITION before20180309181931 VALUES LESS THAN (63687838771) ENGINE = InnoDB,
PARTITION before20180309182531 VALUES LESS THAN (63687839131) ENGINE = InnoDB,
PARTITION before20180309183131 VALUES LESS THAN (63687839491) ENGINE = InnoDB,
PARTITION before20180309183731 VALUES LESS THAN (63687839851) ENGINE = InnoDB,
PARTITION before20180309184331 VALUES LESS THAN (63687840211) ENGINE = InnoDB,
PARTITION before20180309184931 VALUES LESS THAN (63687840571) ENGINE = InnoDB,
PARTITION before20180309185531 VALUES LESS THAN (63687840931) ENGINE = InnoDB,
PARTITION before20180309190131 VALUES LESS THAN (63687841291) ENGINE = InnoDB,
PARTITION before20180309190731 VALUES LESS THAN (63687841651) ENGINE = InnoDB,
PARTITION before20180309191331 VALUES LESS THAN (63687842011) ENGINE = InnoDB,
PARTITION before20180309191931 VALUES LESS THAN (63687842371) ENGINE = InnoDB,
PARTITION before20180309192531 VALUES LESS THAN (63687842731) ENGINE = InnoDB,
PARTITION before20180309193131 VALUES LESS THAN (63687843091) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_sc_primary_keys BEFORE INSERT ON sc_stats_latest
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
PARTITION before20180110071335 VALUES LESS THAN (63682787615) ENGINE = InnoDB,
PARTITION before20180111210135 VALUES LESS THAN (63682923695) ENGINE = InnoDB,
PARTITION before20180113104935 VALUES LESS THAN (63683059775) ENGINE = InnoDB,
PARTITION before20180115003735 VALUES LESS THAN (63683195855) ENGINE = InnoDB,
PARTITION before20180116142535 VALUES LESS THAN (63683331935) ENGINE = InnoDB,
PARTITION before20180118041335 VALUES LESS THAN (63683468015) ENGINE = InnoDB,
PARTITION before20180119180135 VALUES LESS THAN (63683604095) ENGINE = InnoDB,
PARTITION before20180121074935 VALUES LESS THAN (63683740175) ENGINE = InnoDB,
PARTITION before20180122213735 VALUES LESS THAN (63683876255) ENGINE = InnoDB,
PARTITION before20180124112535 VALUES LESS THAN (63684012335) ENGINE = InnoDB,
PARTITION before20180126011335 VALUES LESS THAN (63684148415) ENGINE = InnoDB,
PARTITION before20180127150135 VALUES LESS THAN (63684284495) ENGINE = InnoDB,
PARTITION before20180129044935 VALUES LESS THAN (63684420575) ENGINE = InnoDB,
PARTITION before20180130183735 VALUES LESS THAN (63684556655) ENGINE = InnoDB,
PARTITION before20180201082535 VALUES LESS THAN (63684692735) ENGINE = InnoDB,
PARTITION before20180202221335 VALUES LESS THAN (63684828815) ENGINE = InnoDB,
PARTITION before20180204120135 VALUES LESS THAN (63684964895) ENGINE = InnoDB,
PARTITION before20180206014935 VALUES LESS THAN (63685100975) ENGINE = InnoDB,
PARTITION before20180207153735 VALUES LESS THAN (63685237055) ENGINE = InnoDB,
PARTITION before20180209052535 VALUES LESS THAN (63685373135) ENGINE = InnoDB,
PARTITION before20180210191335 VALUES LESS THAN (63685509215) ENGINE = InnoDB,
PARTITION before20180212090135 VALUES LESS THAN (63685645295) ENGINE = InnoDB,
PARTITION before20180213224935 VALUES LESS THAN (63685781375) ENGINE = InnoDB,
PARTITION before20180215123735 VALUES LESS THAN (63685917455) ENGINE = InnoDB,
PARTITION before20180217022535 VALUES LESS THAN (63686053535) ENGINE = InnoDB,
PARTITION before20180218161335 VALUES LESS THAN (63686189615) ENGINE = InnoDB,
PARTITION before20180220060135 VALUES LESS THAN (63686325695) ENGINE = InnoDB,
PARTITION before20180221194935 VALUES LESS THAN (63686461775) ENGINE = InnoDB,
PARTITION before20180223093735 VALUES LESS THAN (63686597855) ENGINE = InnoDB,
PARTITION before20180224232535 VALUES LESS THAN (63686733935) ENGINE = InnoDB,
PARTITION before20180226131335 VALUES LESS THAN (63686870015) ENGINE = InnoDB,
PARTITION before20180228030135 VALUES LESS THAN (63687006095) ENGINE = InnoDB,
PARTITION before20180301164935 VALUES LESS THAN (63687142175) ENGINE = InnoDB,
PARTITION before20180303063735 VALUES LESS THAN (63687278255) ENGINE = InnoDB,
PARTITION before20180304202535 VALUES LESS THAN (63687414335) ENGINE = InnoDB,
PARTITION before20180306101335 VALUES LESS THAN (63687550415) ENGINE = InnoDB,
PARTITION before20180308000135 VALUES LESS THAN (63687686495) ENGINE = InnoDB,
PARTITION before20180309134935 VALUES LESS THAN (63687822575) ENGINE = InnoDB,
PARTITION before20180311033735 VALUES LESS THAN (63687958655) ENGINE = InnoDB,
PARTITION before20180312172535 VALUES LESS THAN (63688094735) ENGINE = InnoDB,
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
PARTITION before20180306192536 VALUES LESS THAN (63687583536) ENGINE = InnoDB,
PARTITION before20180306212536 VALUES LESS THAN (63687590736) ENGINE = InnoDB,
PARTITION before20180306232536 VALUES LESS THAN (63687597936) ENGINE = InnoDB,
PARTITION before20180307012536 VALUES LESS THAN (63687605136) ENGINE = InnoDB,
PARTITION before20180307032536 VALUES LESS THAN (63687612336) ENGINE = InnoDB,
PARTITION before20180307052536 VALUES LESS THAN (63687619536) ENGINE = InnoDB,
PARTITION before20180307072536 VALUES LESS THAN (63687626736) ENGINE = InnoDB,
PARTITION before20180307092536 VALUES LESS THAN (63687633936) ENGINE = InnoDB,
PARTITION before20180307112536 VALUES LESS THAN (63687641136) ENGINE = InnoDB,
PARTITION before20180307132536 VALUES LESS THAN (63687648336) ENGINE = InnoDB,
PARTITION before20180307152536 VALUES LESS THAN (63687655536) ENGINE = InnoDB,
PARTITION before20180307172536 VALUES LESS THAN (63687662736) ENGINE = InnoDB,
PARTITION before20180307192536 VALUES LESS THAN (63687669936) ENGINE = InnoDB,
PARTITION before20180307212536 VALUES LESS THAN (63687677136) ENGINE = InnoDB,
PARTITION before20180307232536 VALUES LESS THAN (63687684336) ENGINE = InnoDB,
PARTITION before20180308012536 VALUES LESS THAN (63687691536) ENGINE = InnoDB,
PARTITION before20180308032536 VALUES LESS THAN (63687698736) ENGINE = InnoDB,
PARTITION before20180308052536 VALUES LESS THAN (63687705936) ENGINE = InnoDB,
PARTITION before20180308072536 VALUES LESS THAN (63687713136) ENGINE = InnoDB,
PARTITION before20180308092536 VALUES LESS THAN (63687720336) ENGINE = InnoDB,
PARTITION before20180308112536 VALUES LESS THAN (63687727536) ENGINE = InnoDB,
PARTITION before20180308132536 VALUES LESS THAN (63687734736) ENGINE = InnoDB,
PARTITION before20180308152536 VALUES LESS THAN (63687741936) ENGINE = InnoDB,
PARTITION before20180308172536 VALUES LESS THAN (63687749136) ENGINE = InnoDB,
PARTITION before20180308192536 VALUES LESS THAN (63687756336) ENGINE = InnoDB,
PARTITION before20180308212536 VALUES LESS THAN (63687763536) ENGINE = InnoDB,
PARTITION before20180308232536 VALUES LESS THAN (63687770736) ENGINE = InnoDB,
PARTITION before20180309012536 VALUES LESS THAN (63687777936) ENGINE = InnoDB,
PARTITION before20180309032536 VALUES LESS THAN (63687785136) ENGINE = InnoDB,
PARTITION before20180309052536 VALUES LESS THAN (63687792336) ENGINE = InnoDB,
PARTITION before20180309072536 VALUES LESS THAN (63687799536) ENGINE = InnoDB,
PARTITION before20180309092536 VALUES LESS THAN (63687806736) ENGINE = InnoDB,
PARTITION before20180309112536 VALUES LESS THAN (63687813936) ENGINE = InnoDB,
PARTITION before20180309132536 VALUES LESS THAN (63687821136) ENGINE = InnoDB,
PARTITION before20180309152536 VALUES LESS THAN (63687828336) ENGINE = InnoDB,
PARTITION before20180309172536 VALUES LESS THAN (63687835536) ENGINE = InnoDB,
PARTITION before20180309192536 VALUES LESS THAN (63687842736) ENGINE = InnoDB,
PARTITION before20180309212536 VALUES LESS THAN (63687849936) ENGINE = InnoDB,
PARTITION before20180309232536 VALUES LESS THAN (63687857136) ENGINE = InnoDB,
PARTITION before20180310012536 VALUES LESS THAN (63687864336) ENGINE = InnoDB,
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
PARTITION before20160314093737 VALUES LESS THAN (63625167457) ENGINE = InnoDB,
PARTITION before20160402014937 VALUES LESS THAN (63626780977) ENGINE = InnoDB,
PARTITION before20160420180137 VALUES LESS THAN (63628394497) ENGINE = InnoDB,
PARTITION before20160509101337 VALUES LESS THAN (63630008017) ENGINE = InnoDB,
PARTITION before20160528022537 VALUES LESS THAN (63631621537) ENGINE = InnoDB,
PARTITION before20160615183737 VALUES LESS THAN (63633235057) ENGINE = InnoDB,
PARTITION before20160704104937 VALUES LESS THAN (63634848577) ENGINE = InnoDB,
PARTITION before20160723030137 VALUES LESS THAN (63636462097) ENGINE = InnoDB,
PARTITION before20160810191337 VALUES LESS THAN (63638075617) ENGINE = InnoDB,
PARTITION before20160829112537 VALUES LESS THAN (63639689137) ENGINE = InnoDB,
PARTITION before20160917033737 VALUES LESS THAN (63641302657) ENGINE = InnoDB,
PARTITION before20161005194937 VALUES LESS THAN (63642916177) ENGINE = InnoDB,
PARTITION before20161024120137 VALUES LESS THAN (63644529697) ENGINE = InnoDB,
PARTITION before20161112041337 VALUES LESS THAN (63646143217) ENGINE = InnoDB,
PARTITION before20161130202537 VALUES LESS THAN (63647756737) ENGINE = InnoDB,
PARTITION before20161219123737 VALUES LESS THAN (63649370257) ENGINE = InnoDB,
PARTITION before20170107044937 VALUES LESS THAN (63650983777) ENGINE = InnoDB,
PARTITION before20170125210137 VALUES LESS THAN (63652597297) ENGINE = InnoDB,
PARTITION before20170213131337 VALUES LESS THAN (63654210817) ENGINE = InnoDB,
PARTITION before20170304052537 VALUES LESS THAN (63655824337) ENGINE = InnoDB,
PARTITION before20170322213737 VALUES LESS THAN (63657437857) ENGINE = InnoDB,
PARTITION before20170410134937 VALUES LESS THAN (63659051377) ENGINE = InnoDB,
PARTITION before20170429060137 VALUES LESS THAN (63660664897) ENGINE = InnoDB,
PARTITION before20170517221337 VALUES LESS THAN (63662278417) ENGINE = InnoDB,
PARTITION before20170605142537 VALUES LESS THAN (63663891937) ENGINE = InnoDB,
PARTITION before20170624063737 VALUES LESS THAN (63665505457) ENGINE = InnoDB,
PARTITION before20170712224937 VALUES LESS THAN (63667118977) ENGINE = InnoDB,
PARTITION before20170731150137 VALUES LESS THAN (63668732497) ENGINE = InnoDB,
PARTITION before20170819071337 VALUES LESS THAN (63670346017) ENGINE = InnoDB,
PARTITION before20170906232537 VALUES LESS THAN (63671959537) ENGINE = InnoDB,
PARTITION before20170925153737 VALUES LESS THAN (63673573057) ENGINE = InnoDB,
PARTITION before20171014074937 VALUES LESS THAN (63675186577) ENGINE = InnoDB,
PARTITION before20171102000137 VALUES LESS THAN (63676800097) ENGINE = InnoDB,
PARTITION before20171120161337 VALUES LESS THAN (63678413617) ENGINE = InnoDB,
PARTITION before20171209082537 VALUES LESS THAN (63680027137) ENGINE = InnoDB,
PARTITION before20171228003737 VALUES LESS THAN (63681640657) ENGINE = InnoDB,
PARTITION before20180115164937 VALUES LESS THAN (63683254177) ENGINE = InnoDB,
PARTITION before20180203090137 VALUES LESS THAN (63684867697) ENGINE = InnoDB,
PARTITION before20180222011337 VALUES LESS THAN (63686481217) ENGINE = InnoDB,
PARTITION before20180312172537 VALUES LESS THAN (63688094737) ENGINE = InnoDB,
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
PARTITION before20180309153734 VALUES LESS THAN (63687829054) ENGINE = InnoDB,
PARTITION before20180309154334 VALUES LESS THAN (63687829414) ENGINE = InnoDB,
PARTITION before20180309154934 VALUES LESS THAN (63687829774) ENGINE = InnoDB,
PARTITION before20180309155534 VALUES LESS THAN (63687830134) ENGINE = InnoDB,
PARTITION before20180309160134 VALUES LESS THAN (63687830494) ENGINE = InnoDB,
PARTITION before20180309160734 VALUES LESS THAN (63687830854) ENGINE = InnoDB,
PARTITION before20180309161334 VALUES LESS THAN (63687831214) ENGINE = InnoDB,
PARTITION before20180309161934 VALUES LESS THAN (63687831574) ENGINE = InnoDB,
PARTITION before20180309162534 VALUES LESS THAN (63687831934) ENGINE = InnoDB,
PARTITION before20180309163134 VALUES LESS THAN (63687832294) ENGINE = InnoDB,
PARTITION before20180309163734 VALUES LESS THAN (63687832654) ENGINE = InnoDB,
PARTITION before20180309164334 VALUES LESS THAN (63687833014) ENGINE = InnoDB,
PARTITION before20180309164934 VALUES LESS THAN (63687833374) ENGINE = InnoDB,
PARTITION before20180309165534 VALUES LESS THAN (63687833734) ENGINE = InnoDB,
PARTITION before20180309170134 VALUES LESS THAN (63687834094) ENGINE = InnoDB,
PARTITION before20180309170734 VALUES LESS THAN (63687834454) ENGINE = InnoDB,
PARTITION before20180309171334 VALUES LESS THAN (63687834814) ENGINE = InnoDB,
PARTITION before20180309171934 VALUES LESS THAN (63687835174) ENGINE = InnoDB,
PARTITION before20180309172534 VALUES LESS THAN (63687835534) ENGINE = InnoDB,
PARTITION before20180309173134 VALUES LESS THAN (63687835894) ENGINE = InnoDB,
PARTITION before20180309173734 VALUES LESS THAN (63687836254) ENGINE = InnoDB,
PARTITION before20180309174334 VALUES LESS THAN (63687836614) ENGINE = InnoDB,
PARTITION before20180309174934 VALUES LESS THAN (63687836974) ENGINE = InnoDB,
PARTITION before20180309175534 VALUES LESS THAN (63687837334) ENGINE = InnoDB,
PARTITION before20180309180134 VALUES LESS THAN (63687837694) ENGINE = InnoDB,
PARTITION before20180309180734 VALUES LESS THAN (63687838054) ENGINE = InnoDB,
PARTITION before20180309181334 VALUES LESS THAN (63687838414) ENGINE = InnoDB,
PARTITION before20180309181934 VALUES LESS THAN (63687838774) ENGINE = InnoDB,
PARTITION before20180309182534 VALUES LESS THAN (63687839134) ENGINE = InnoDB,
PARTITION before20180309183134 VALUES LESS THAN (63687839494) ENGINE = InnoDB,
PARTITION before20180309183734 VALUES LESS THAN (63687839854) ENGINE = InnoDB,
PARTITION before20180309184334 VALUES LESS THAN (63687840214) ENGINE = InnoDB,
PARTITION before20180309184934 VALUES LESS THAN (63687840574) ENGINE = InnoDB,
PARTITION before20180309185534 VALUES LESS THAN (63687840934) ENGINE = InnoDB,
PARTITION before20180309190134 VALUES LESS THAN (63687841294) ENGINE = InnoDB,
PARTITION before20180309190734 VALUES LESS THAN (63687841654) ENGINE = InnoDB,
PARTITION before20180309191334 VALUES LESS THAN (63687842014) ENGINE = InnoDB,
PARTITION before20180309191934 VALUES LESS THAN (63687842374) ENGINE = InnoDB,
PARTITION before20180309192534 VALUES LESS THAN (63687842734) ENGINE = InnoDB,
PARTITION before20180309193134 VALUES LESS THAN (63687843094) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_sw_primary_keys BEFORE INSERT ON sw_stats_latest
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
PARTITION before20180110071339 VALUES LESS THAN (63682787619) ENGINE = InnoDB,
PARTITION before20180111210139 VALUES LESS THAN (63682923699) ENGINE = InnoDB,
PARTITION before20180113104939 VALUES LESS THAN (63683059779) ENGINE = InnoDB,
PARTITION before20180115003739 VALUES LESS THAN (63683195859) ENGINE = InnoDB,
PARTITION before20180116142539 VALUES LESS THAN (63683331939) ENGINE = InnoDB,
PARTITION before20180118041339 VALUES LESS THAN (63683468019) ENGINE = InnoDB,
PARTITION before20180119180139 VALUES LESS THAN (63683604099) ENGINE = InnoDB,
PARTITION before20180121074939 VALUES LESS THAN (63683740179) ENGINE = InnoDB,
PARTITION before20180122213739 VALUES LESS THAN (63683876259) ENGINE = InnoDB,
PARTITION before20180124112539 VALUES LESS THAN (63684012339) ENGINE = InnoDB,
PARTITION before20180126011339 VALUES LESS THAN (63684148419) ENGINE = InnoDB,
PARTITION before20180127150139 VALUES LESS THAN (63684284499) ENGINE = InnoDB,
PARTITION before20180129044939 VALUES LESS THAN (63684420579) ENGINE = InnoDB,
PARTITION before20180130183739 VALUES LESS THAN (63684556659) ENGINE = InnoDB,
PARTITION before20180201082539 VALUES LESS THAN (63684692739) ENGINE = InnoDB,
PARTITION before20180202221339 VALUES LESS THAN (63684828819) ENGINE = InnoDB,
PARTITION before20180204120139 VALUES LESS THAN (63684964899) ENGINE = InnoDB,
PARTITION before20180206014939 VALUES LESS THAN (63685100979) ENGINE = InnoDB,
PARTITION before20180207153739 VALUES LESS THAN (63685237059) ENGINE = InnoDB,
PARTITION before20180209052539 VALUES LESS THAN (63685373139) ENGINE = InnoDB,
PARTITION before20180210191339 VALUES LESS THAN (63685509219) ENGINE = InnoDB,
PARTITION before20180212090139 VALUES LESS THAN (63685645299) ENGINE = InnoDB,
PARTITION before20180213224939 VALUES LESS THAN (63685781379) ENGINE = InnoDB,
PARTITION before20180215123739 VALUES LESS THAN (63685917459) ENGINE = InnoDB,
PARTITION before20180217022539 VALUES LESS THAN (63686053539) ENGINE = InnoDB,
PARTITION before20180218161339 VALUES LESS THAN (63686189619) ENGINE = InnoDB,
PARTITION before20180220060139 VALUES LESS THAN (63686325699) ENGINE = InnoDB,
PARTITION before20180221194939 VALUES LESS THAN (63686461779) ENGINE = InnoDB,
PARTITION before20180223093739 VALUES LESS THAN (63686597859) ENGINE = InnoDB,
PARTITION before20180224232539 VALUES LESS THAN (63686733939) ENGINE = InnoDB,
PARTITION before20180226131339 VALUES LESS THAN (63686870019) ENGINE = InnoDB,
PARTITION before20180228030139 VALUES LESS THAN (63687006099) ENGINE = InnoDB,
PARTITION before20180301164939 VALUES LESS THAN (63687142179) ENGINE = InnoDB,
PARTITION before20180303063739 VALUES LESS THAN (63687278259) ENGINE = InnoDB,
PARTITION before20180304202539 VALUES LESS THAN (63687414339) ENGINE = InnoDB,
PARTITION before20180306101339 VALUES LESS THAN (63687550419) ENGINE = InnoDB,
PARTITION before20180308000139 VALUES LESS THAN (63687686499) ENGINE = InnoDB,
PARTITION before20180309134939 VALUES LESS THAN (63687822579) ENGINE = InnoDB,
PARTITION before20180311033739 VALUES LESS THAN (63687958659) ENGINE = InnoDB,
PARTITION before20180312172539 VALUES LESS THAN (63688094739) ENGINE = InnoDB,
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
PARTITION before20180306192540 VALUES LESS THAN (63687583540) ENGINE = InnoDB,
PARTITION before20180306212540 VALUES LESS THAN (63687590740) ENGINE = InnoDB,
PARTITION before20180306232540 VALUES LESS THAN (63687597940) ENGINE = InnoDB,
PARTITION before20180307012540 VALUES LESS THAN (63687605140) ENGINE = InnoDB,
PARTITION before20180307032540 VALUES LESS THAN (63687612340) ENGINE = InnoDB,
PARTITION before20180307052540 VALUES LESS THAN (63687619540) ENGINE = InnoDB,
PARTITION before20180307072540 VALUES LESS THAN (63687626740) ENGINE = InnoDB,
PARTITION before20180307092540 VALUES LESS THAN (63687633940) ENGINE = InnoDB,
PARTITION before20180307112540 VALUES LESS THAN (63687641140) ENGINE = InnoDB,
PARTITION before20180307132540 VALUES LESS THAN (63687648340) ENGINE = InnoDB,
PARTITION before20180307152540 VALUES LESS THAN (63687655540) ENGINE = InnoDB,
PARTITION before20180307172540 VALUES LESS THAN (63687662740) ENGINE = InnoDB,
PARTITION before20180307192540 VALUES LESS THAN (63687669940) ENGINE = InnoDB,
PARTITION before20180307212540 VALUES LESS THAN (63687677140) ENGINE = InnoDB,
PARTITION before20180307232540 VALUES LESS THAN (63687684340) ENGINE = InnoDB,
PARTITION before20180308012540 VALUES LESS THAN (63687691540) ENGINE = InnoDB,
PARTITION before20180308032540 VALUES LESS THAN (63687698740) ENGINE = InnoDB,
PARTITION before20180308052540 VALUES LESS THAN (63687705940) ENGINE = InnoDB,
PARTITION before20180308072540 VALUES LESS THAN (63687713140) ENGINE = InnoDB,
PARTITION before20180308092540 VALUES LESS THAN (63687720340) ENGINE = InnoDB,
PARTITION before20180308112540 VALUES LESS THAN (63687727540) ENGINE = InnoDB,
PARTITION before20180308132540 VALUES LESS THAN (63687734740) ENGINE = InnoDB,
PARTITION before20180308152540 VALUES LESS THAN (63687741940) ENGINE = InnoDB,
PARTITION before20180308172540 VALUES LESS THAN (63687749140) ENGINE = InnoDB,
PARTITION before20180308192540 VALUES LESS THAN (63687756340) ENGINE = InnoDB,
PARTITION before20180308212540 VALUES LESS THAN (63687763540) ENGINE = InnoDB,
PARTITION before20180308232540 VALUES LESS THAN (63687770740) ENGINE = InnoDB,
PARTITION before20180309012540 VALUES LESS THAN (63687777940) ENGINE = InnoDB,
PARTITION before20180309032540 VALUES LESS THAN (63687785140) ENGINE = InnoDB,
PARTITION before20180309052540 VALUES LESS THAN (63687792340) ENGINE = InnoDB,
PARTITION before20180309072540 VALUES LESS THAN (63687799540) ENGINE = InnoDB,
PARTITION before20180309092540 VALUES LESS THAN (63687806740) ENGINE = InnoDB,
PARTITION before20180309112540 VALUES LESS THAN (63687813940) ENGINE = InnoDB,
PARTITION before20180309132540 VALUES LESS THAN (63687821140) ENGINE = InnoDB,
PARTITION before20180309152540 VALUES LESS THAN (63687828340) ENGINE = InnoDB,
PARTITION before20180309172540 VALUES LESS THAN (63687835540) ENGINE = InnoDB,
PARTITION before20180309192540 VALUES LESS THAN (63687842740) ENGINE = InnoDB,
PARTITION before20180309212540 VALUES LESS THAN (63687849940) ENGINE = InnoDB,
PARTITION before20180309232540 VALUES LESS THAN (63687857140) ENGINE = InnoDB,
PARTITION before20180310012540 VALUES LESS THAN (63687864340) ENGINE = InnoDB,
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
PARTITION before20160314093741 VALUES LESS THAN (63625167461) ENGINE = InnoDB,
PARTITION before20160402014941 VALUES LESS THAN (63626780981) ENGINE = InnoDB,
PARTITION before20160420180141 VALUES LESS THAN (63628394501) ENGINE = InnoDB,
PARTITION before20160509101341 VALUES LESS THAN (63630008021) ENGINE = InnoDB,
PARTITION before20160528022541 VALUES LESS THAN (63631621541) ENGINE = InnoDB,
PARTITION before20160615183741 VALUES LESS THAN (63633235061) ENGINE = InnoDB,
PARTITION before20160704104941 VALUES LESS THAN (63634848581) ENGINE = InnoDB,
PARTITION before20160723030141 VALUES LESS THAN (63636462101) ENGINE = InnoDB,
PARTITION before20160810191341 VALUES LESS THAN (63638075621) ENGINE = InnoDB,
PARTITION before20160829112541 VALUES LESS THAN (63639689141) ENGINE = InnoDB,
PARTITION before20160917033741 VALUES LESS THAN (63641302661) ENGINE = InnoDB,
PARTITION before20161005194941 VALUES LESS THAN (63642916181) ENGINE = InnoDB,
PARTITION before20161024120141 VALUES LESS THAN (63644529701) ENGINE = InnoDB,
PARTITION before20161112041341 VALUES LESS THAN (63646143221) ENGINE = InnoDB,
PARTITION before20161130202541 VALUES LESS THAN (63647756741) ENGINE = InnoDB,
PARTITION before20161219123741 VALUES LESS THAN (63649370261) ENGINE = InnoDB,
PARTITION before20170107044941 VALUES LESS THAN (63650983781) ENGINE = InnoDB,
PARTITION before20170125210141 VALUES LESS THAN (63652597301) ENGINE = InnoDB,
PARTITION before20170213131341 VALUES LESS THAN (63654210821) ENGINE = InnoDB,
PARTITION before20170304052541 VALUES LESS THAN (63655824341) ENGINE = InnoDB,
PARTITION before20170322213741 VALUES LESS THAN (63657437861) ENGINE = InnoDB,
PARTITION before20170410134941 VALUES LESS THAN (63659051381) ENGINE = InnoDB,
PARTITION before20170429060141 VALUES LESS THAN (63660664901) ENGINE = InnoDB,
PARTITION before20170517221341 VALUES LESS THAN (63662278421) ENGINE = InnoDB,
PARTITION before20170605142541 VALUES LESS THAN (63663891941) ENGINE = InnoDB,
PARTITION before20170624063741 VALUES LESS THAN (63665505461) ENGINE = InnoDB,
PARTITION before20170712224941 VALUES LESS THAN (63667118981) ENGINE = InnoDB,
PARTITION before20170731150141 VALUES LESS THAN (63668732501) ENGINE = InnoDB,
PARTITION before20170819071341 VALUES LESS THAN (63670346021) ENGINE = InnoDB,
PARTITION before20170906232541 VALUES LESS THAN (63671959541) ENGINE = InnoDB,
PARTITION before20170925153741 VALUES LESS THAN (63673573061) ENGINE = InnoDB,
PARTITION before20171014074941 VALUES LESS THAN (63675186581) ENGINE = InnoDB,
PARTITION before20171102000141 VALUES LESS THAN (63676800101) ENGINE = InnoDB,
PARTITION before20171120161341 VALUES LESS THAN (63678413621) ENGINE = InnoDB,
PARTITION before20171209082541 VALUES LESS THAN (63680027141) ENGINE = InnoDB,
PARTITION before20171228003741 VALUES LESS THAN (63681640661) ENGINE = InnoDB,
PARTITION before20180115164941 VALUES LESS THAN (63683254181) ENGINE = InnoDB,
PARTITION before20180203090141 VALUES LESS THAN (63684867701) ENGINE = InnoDB,
PARTITION before20180222011341 VALUES LESS THAN (63686481221) ENGINE = InnoDB,
PARTITION before20180312172541 VALUES LESS THAN (63688094741) ENGINE = InnoDB,
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
PARTITION before20180309153738 VALUES LESS THAN (63687829058) ENGINE = InnoDB,
PARTITION before20180309154338 VALUES LESS THAN (63687829418) ENGINE = InnoDB,
PARTITION before20180309154938 VALUES LESS THAN (63687829778) ENGINE = InnoDB,
PARTITION before20180309155538 VALUES LESS THAN (63687830138) ENGINE = InnoDB,
PARTITION before20180309160138 VALUES LESS THAN (63687830498) ENGINE = InnoDB,
PARTITION before20180309160738 VALUES LESS THAN (63687830858) ENGINE = InnoDB,
PARTITION before20180309161338 VALUES LESS THAN (63687831218) ENGINE = InnoDB,
PARTITION before20180309161938 VALUES LESS THAN (63687831578) ENGINE = InnoDB,
PARTITION before20180309162538 VALUES LESS THAN (63687831938) ENGINE = InnoDB,
PARTITION before20180309163138 VALUES LESS THAN (63687832298) ENGINE = InnoDB,
PARTITION before20180309163738 VALUES LESS THAN (63687832658) ENGINE = InnoDB,
PARTITION before20180309164338 VALUES LESS THAN (63687833018) ENGINE = InnoDB,
PARTITION before20180309164938 VALUES LESS THAN (63687833378) ENGINE = InnoDB,
PARTITION before20180309165538 VALUES LESS THAN (63687833738) ENGINE = InnoDB,
PARTITION before20180309170138 VALUES LESS THAN (63687834098) ENGINE = InnoDB,
PARTITION before20180309170738 VALUES LESS THAN (63687834458) ENGINE = InnoDB,
PARTITION before20180309171338 VALUES LESS THAN (63687834818) ENGINE = InnoDB,
PARTITION before20180309171938 VALUES LESS THAN (63687835178) ENGINE = InnoDB,
PARTITION before20180309172538 VALUES LESS THAN (63687835538) ENGINE = InnoDB,
PARTITION before20180309173138 VALUES LESS THAN (63687835898) ENGINE = InnoDB,
PARTITION before20180309173738 VALUES LESS THAN (63687836258) ENGINE = InnoDB,
PARTITION before20180309174338 VALUES LESS THAN (63687836618) ENGINE = InnoDB,
PARTITION before20180309174938 VALUES LESS THAN (63687836978) ENGINE = InnoDB,
PARTITION before20180309175538 VALUES LESS THAN (63687837338) ENGINE = InnoDB,
PARTITION before20180309180138 VALUES LESS THAN (63687837698) ENGINE = InnoDB,
PARTITION before20180309180738 VALUES LESS THAN (63687838058) ENGINE = InnoDB,
PARTITION before20180309181338 VALUES LESS THAN (63687838418) ENGINE = InnoDB,
PARTITION before20180309181938 VALUES LESS THAN (63687838778) ENGINE = InnoDB,
PARTITION before20180309182538 VALUES LESS THAN (63687839138) ENGINE = InnoDB,
PARTITION before20180309183138 VALUES LESS THAN (63687839498) ENGINE = InnoDB,
PARTITION before20180309183738 VALUES LESS THAN (63687839858) ENGINE = InnoDB,
PARTITION before20180309184338 VALUES LESS THAN (63687840218) ENGINE = InnoDB,
PARTITION before20180309184938 VALUES LESS THAN (63687840578) ENGINE = InnoDB,
PARTITION before20180309185538 VALUES LESS THAN (63687840938) ENGINE = InnoDB,
PARTITION before20180309190138 VALUES LESS THAN (63687841298) ENGINE = InnoDB,
PARTITION before20180309190738 VALUES LESS THAN (63687841658) ENGINE = InnoDB,
PARTITION before20180309191338 VALUES LESS THAN (63687842018) ENGINE = InnoDB,
PARTITION before20180309191938 VALUES LESS THAN (63687842378) ENGINE = InnoDB,
PARTITION before20180309192538 VALUES LESS THAN (63687842738) ENGINE = InnoDB,
PARTITION before20180309193138 VALUES LESS THAN (63687843098) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_vdc_primary_keys BEFORE INSERT ON vdc_stats_latest
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
PARTITION before20180110071343 VALUES LESS THAN (63682787623) ENGINE = InnoDB,
PARTITION before20180111210143 VALUES LESS THAN (63682923703) ENGINE = InnoDB,
PARTITION before20180113104943 VALUES LESS THAN (63683059783) ENGINE = InnoDB,
PARTITION before20180115003743 VALUES LESS THAN (63683195863) ENGINE = InnoDB,
PARTITION before20180116142543 VALUES LESS THAN (63683331943) ENGINE = InnoDB,
PARTITION before20180118041343 VALUES LESS THAN (63683468023) ENGINE = InnoDB,
PARTITION before20180119180143 VALUES LESS THAN (63683604103) ENGINE = InnoDB,
PARTITION before20180121074943 VALUES LESS THAN (63683740183) ENGINE = InnoDB,
PARTITION before20180122213743 VALUES LESS THAN (63683876263) ENGINE = InnoDB,
PARTITION before20180124112543 VALUES LESS THAN (63684012343) ENGINE = InnoDB,
PARTITION before20180126011343 VALUES LESS THAN (63684148423) ENGINE = InnoDB,
PARTITION before20180127150143 VALUES LESS THAN (63684284503) ENGINE = InnoDB,
PARTITION before20180129044943 VALUES LESS THAN (63684420583) ENGINE = InnoDB,
PARTITION before20180130183743 VALUES LESS THAN (63684556663) ENGINE = InnoDB,
PARTITION before20180201082543 VALUES LESS THAN (63684692743) ENGINE = InnoDB,
PARTITION before20180202221343 VALUES LESS THAN (63684828823) ENGINE = InnoDB,
PARTITION before20180204120143 VALUES LESS THAN (63684964903) ENGINE = InnoDB,
PARTITION before20180206014943 VALUES LESS THAN (63685100983) ENGINE = InnoDB,
PARTITION before20180207153743 VALUES LESS THAN (63685237063) ENGINE = InnoDB,
PARTITION before20180209052543 VALUES LESS THAN (63685373143) ENGINE = InnoDB,
PARTITION before20180210191343 VALUES LESS THAN (63685509223) ENGINE = InnoDB,
PARTITION before20180212090143 VALUES LESS THAN (63685645303) ENGINE = InnoDB,
PARTITION before20180213224943 VALUES LESS THAN (63685781383) ENGINE = InnoDB,
PARTITION before20180215123743 VALUES LESS THAN (63685917463) ENGINE = InnoDB,
PARTITION before20180217022543 VALUES LESS THAN (63686053543) ENGINE = InnoDB,
PARTITION before20180218161343 VALUES LESS THAN (63686189623) ENGINE = InnoDB,
PARTITION before20180220060143 VALUES LESS THAN (63686325703) ENGINE = InnoDB,
PARTITION before20180221194943 VALUES LESS THAN (63686461783) ENGINE = InnoDB,
PARTITION before20180223093743 VALUES LESS THAN (63686597863) ENGINE = InnoDB,
PARTITION before20180224232543 VALUES LESS THAN (63686733943) ENGINE = InnoDB,
PARTITION before20180226131343 VALUES LESS THAN (63686870023) ENGINE = InnoDB,
PARTITION before20180228030143 VALUES LESS THAN (63687006103) ENGINE = InnoDB,
PARTITION before20180301164943 VALUES LESS THAN (63687142183) ENGINE = InnoDB,
PARTITION before20180303063743 VALUES LESS THAN (63687278263) ENGINE = InnoDB,
PARTITION before20180304202543 VALUES LESS THAN (63687414343) ENGINE = InnoDB,
PARTITION before20180306101343 VALUES LESS THAN (63687550423) ENGINE = InnoDB,
PARTITION before20180308000143 VALUES LESS THAN (63687686503) ENGINE = InnoDB,
PARTITION before20180309134943 VALUES LESS THAN (63687822583) ENGINE = InnoDB,
PARTITION before20180311033743 VALUES LESS THAN (63687958663) ENGINE = InnoDB,
PARTITION before20180312172543 VALUES LESS THAN (63688094743) ENGINE = InnoDB,
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
PARTITION before20180306192545 VALUES LESS THAN (63687583545) ENGINE = InnoDB,
PARTITION before20180306212545 VALUES LESS THAN (63687590745) ENGINE = InnoDB,
PARTITION before20180306232545 VALUES LESS THAN (63687597945) ENGINE = InnoDB,
PARTITION before20180307012545 VALUES LESS THAN (63687605145) ENGINE = InnoDB,
PARTITION before20180307032545 VALUES LESS THAN (63687612345) ENGINE = InnoDB,
PARTITION before20180307052545 VALUES LESS THAN (63687619545) ENGINE = InnoDB,
PARTITION before20180307072545 VALUES LESS THAN (63687626745) ENGINE = InnoDB,
PARTITION before20180307092545 VALUES LESS THAN (63687633945) ENGINE = InnoDB,
PARTITION before20180307112545 VALUES LESS THAN (63687641145) ENGINE = InnoDB,
PARTITION before20180307132545 VALUES LESS THAN (63687648345) ENGINE = InnoDB,
PARTITION before20180307152545 VALUES LESS THAN (63687655545) ENGINE = InnoDB,
PARTITION before20180307172545 VALUES LESS THAN (63687662745) ENGINE = InnoDB,
PARTITION before20180307192545 VALUES LESS THAN (63687669945) ENGINE = InnoDB,
PARTITION before20180307212545 VALUES LESS THAN (63687677145) ENGINE = InnoDB,
PARTITION before20180307232545 VALUES LESS THAN (63687684345) ENGINE = InnoDB,
PARTITION before20180308012545 VALUES LESS THAN (63687691545) ENGINE = InnoDB,
PARTITION before20180308032545 VALUES LESS THAN (63687698745) ENGINE = InnoDB,
PARTITION before20180308052545 VALUES LESS THAN (63687705945) ENGINE = InnoDB,
PARTITION before20180308072545 VALUES LESS THAN (63687713145) ENGINE = InnoDB,
PARTITION before20180308092545 VALUES LESS THAN (63687720345) ENGINE = InnoDB,
PARTITION before20180308112545 VALUES LESS THAN (63687727545) ENGINE = InnoDB,
PARTITION before20180308132545 VALUES LESS THAN (63687734745) ENGINE = InnoDB,
PARTITION before20180308152545 VALUES LESS THAN (63687741945) ENGINE = InnoDB,
PARTITION before20180308172545 VALUES LESS THAN (63687749145) ENGINE = InnoDB,
PARTITION before20180308192545 VALUES LESS THAN (63687756345) ENGINE = InnoDB,
PARTITION before20180308212545 VALUES LESS THAN (63687763545) ENGINE = InnoDB,
PARTITION before20180308232545 VALUES LESS THAN (63687770745) ENGINE = InnoDB,
PARTITION before20180309012545 VALUES LESS THAN (63687777945) ENGINE = InnoDB,
PARTITION before20180309032545 VALUES LESS THAN (63687785145) ENGINE = InnoDB,
PARTITION before20180309052545 VALUES LESS THAN (63687792345) ENGINE = InnoDB,
PARTITION before20180309072545 VALUES LESS THAN (63687799545) ENGINE = InnoDB,
PARTITION before20180309092545 VALUES LESS THAN (63687806745) ENGINE = InnoDB,
PARTITION before20180309112545 VALUES LESS THAN (63687813945) ENGINE = InnoDB,
PARTITION before20180309132545 VALUES LESS THAN (63687821145) ENGINE = InnoDB,
PARTITION before20180309152545 VALUES LESS THAN (63687828345) ENGINE = InnoDB,
PARTITION before20180309172545 VALUES LESS THAN (63687835545) ENGINE = InnoDB,
PARTITION before20180309192545 VALUES LESS THAN (63687842745) ENGINE = InnoDB,
PARTITION before20180309212545 VALUES LESS THAN (63687849945) ENGINE = InnoDB,
PARTITION before20180309232545 VALUES LESS THAN (63687857145) ENGINE = InnoDB,
PARTITION before20180310012545 VALUES LESS THAN (63687864345) ENGINE = InnoDB,
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
PARTITION before20160314093746 VALUES LESS THAN (63625167466) ENGINE = InnoDB,
PARTITION before20160402014946 VALUES LESS THAN (63626780986) ENGINE = InnoDB,
PARTITION before20160420180146 VALUES LESS THAN (63628394506) ENGINE = InnoDB,
PARTITION before20160509101346 VALUES LESS THAN (63630008026) ENGINE = InnoDB,
PARTITION before20160528022546 VALUES LESS THAN (63631621546) ENGINE = InnoDB,
PARTITION before20160615183746 VALUES LESS THAN (63633235066) ENGINE = InnoDB,
PARTITION before20160704104946 VALUES LESS THAN (63634848586) ENGINE = InnoDB,
PARTITION before20160723030146 VALUES LESS THAN (63636462106) ENGINE = InnoDB,
PARTITION before20160810191346 VALUES LESS THAN (63638075626) ENGINE = InnoDB,
PARTITION before20160829112546 VALUES LESS THAN (63639689146) ENGINE = InnoDB,
PARTITION before20160917033746 VALUES LESS THAN (63641302666) ENGINE = InnoDB,
PARTITION before20161005194946 VALUES LESS THAN (63642916186) ENGINE = InnoDB,
PARTITION before20161024120146 VALUES LESS THAN (63644529706) ENGINE = InnoDB,
PARTITION before20161112041346 VALUES LESS THAN (63646143226) ENGINE = InnoDB,
PARTITION before20161130202546 VALUES LESS THAN (63647756746) ENGINE = InnoDB,
PARTITION before20161219123746 VALUES LESS THAN (63649370266) ENGINE = InnoDB,
PARTITION before20170107044946 VALUES LESS THAN (63650983786) ENGINE = InnoDB,
PARTITION before20170125210146 VALUES LESS THAN (63652597306) ENGINE = InnoDB,
PARTITION before20170213131346 VALUES LESS THAN (63654210826) ENGINE = InnoDB,
PARTITION before20170304052546 VALUES LESS THAN (63655824346) ENGINE = InnoDB,
PARTITION before20170322213746 VALUES LESS THAN (63657437866) ENGINE = InnoDB,
PARTITION before20170410134946 VALUES LESS THAN (63659051386) ENGINE = InnoDB,
PARTITION before20170429060146 VALUES LESS THAN (63660664906) ENGINE = InnoDB,
PARTITION before20170517221346 VALUES LESS THAN (63662278426) ENGINE = InnoDB,
PARTITION before20170605142546 VALUES LESS THAN (63663891946) ENGINE = InnoDB,
PARTITION before20170624063746 VALUES LESS THAN (63665505466) ENGINE = InnoDB,
PARTITION before20170712224946 VALUES LESS THAN (63667118986) ENGINE = InnoDB,
PARTITION before20170731150146 VALUES LESS THAN (63668732506) ENGINE = InnoDB,
PARTITION before20170819071346 VALUES LESS THAN (63670346026) ENGINE = InnoDB,
PARTITION before20170906232546 VALUES LESS THAN (63671959546) ENGINE = InnoDB,
PARTITION before20170925153746 VALUES LESS THAN (63673573066) ENGINE = InnoDB,
PARTITION before20171014074946 VALUES LESS THAN (63675186586) ENGINE = InnoDB,
PARTITION before20171102000146 VALUES LESS THAN (63676800106) ENGINE = InnoDB,
PARTITION before20171120161346 VALUES LESS THAN (63678413626) ENGINE = InnoDB,
PARTITION before20171209082546 VALUES LESS THAN (63680027146) ENGINE = InnoDB,
PARTITION before20171228003746 VALUES LESS THAN (63681640666) ENGINE = InnoDB,
PARTITION before20180115164946 VALUES LESS THAN (63683254186) ENGINE = InnoDB,
PARTITION before20180203090146 VALUES LESS THAN (63684867706) ENGINE = InnoDB,
PARTITION before20180222011346 VALUES LESS THAN (63686481226) ENGINE = InnoDB,
PARTITION before20180312172546 VALUES LESS THAN (63688094746) ENGINE = InnoDB,
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
PARTITION before20180309153742 VALUES LESS THAN (63687829062) ENGINE = InnoDB,
PARTITION before20180309154342 VALUES LESS THAN (63687829422) ENGINE = InnoDB,
PARTITION before20180309154942 VALUES LESS THAN (63687829782) ENGINE = InnoDB,
PARTITION before20180309155542 VALUES LESS THAN (63687830142) ENGINE = InnoDB,
PARTITION before20180309160142 VALUES LESS THAN (63687830502) ENGINE = InnoDB,
PARTITION before20180309160742 VALUES LESS THAN (63687830862) ENGINE = InnoDB,
PARTITION before20180309161342 VALUES LESS THAN (63687831222) ENGINE = InnoDB,
PARTITION before20180309161942 VALUES LESS THAN (63687831582) ENGINE = InnoDB,
PARTITION before20180309162542 VALUES LESS THAN (63687831942) ENGINE = InnoDB,
PARTITION before20180309163142 VALUES LESS THAN (63687832302) ENGINE = InnoDB,
PARTITION before20180309163742 VALUES LESS THAN (63687832662) ENGINE = InnoDB,
PARTITION before20180309164342 VALUES LESS THAN (63687833022) ENGINE = InnoDB,
PARTITION before20180309164942 VALUES LESS THAN (63687833382) ENGINE = InnoDB,
PARTITION before20180309165542 VALUES LESS THAN (63687833742) ENGINE = InnoDB,
PARTITION before20180309170142 VALUES LESS THAN (63687834102) ENGINE = InnoDB,
PARTITION before20180309170742 VALUES LESS THAN (63687834462) ENGINE = InnoDB,
PARTITION before20180309171342 VALUES LESS THAN (63687834822) ENGINE = InnoDB,
PARTITION before20180309171942 VALUES LESS THAN (63687835182) ENGINE = InnoDB,
PARTITION before20180309172542 VALUES LESS THAN (63687835542) ENGINE = InnoDB,
PARTITION before20180309173142 VALUES LESS THAN (63687835902) ENGINE = InnoDB,
PARTITION before20180309173742 VALUES LESS THAN (63687836262) ENGINE = InnoDB,
PARTITION before20180309174342 VALUES LESS THAN (63687836622) ENGINE = InnoDB,
PARTITION before20180309174942 VALUES LESS THAN (63687836982) ENGINE = InnoDB,
PARTITION before20180309175542 VALUES LESS THAN (63687837342) ENGINE = InnoDB,
PARTITION before20180309180142 VALUES LESS THAN (63687837702) ENGINE = InnoDB,
PARTITION before20180309180742 VALUES LESS THAN (63687838062) ENGINE = InnoDB,
PARTITION before20180309181342 VALUES LESS THAN (63687838422) ENGINE = InnoDB,
PARTITION before20180309181942 VALUES LESS THAN (63687838782) ENGINE = InnoDB,
PARTITION before20180309182542 VALUES LESS THAN (63687839142) ENGINE = InnoDB,
PARTITION before20180309183142 VALUES LESS THAN (63687839502) ENGINE = InnoDB,
PARTITION before20180309183742 VALUES LESS THAN (63687839862) ENGINE = InnoDB,
PARTITION before20180309184342 VALUES LESS THAN (63687840222) ENGINE = InnoDB,
PARTITION before20180309184942 VALUES LESS THAN (63687840582) ENGINE = InnoDB,
PARTITION before20180309185542 VALUES LESS THAN (63687840942) ENGINE = InnoDB,
PARTITION before20180309190142 VALUES LESS THAN (63687841302) ENGINE = InnoDB,
PARTITION before20180309190742 VALUES LESS THAN (63687841662) ENGINE = InnoDB,
PARTITION before20180309191342 VALUES LESS THAN (63687842022) ENGINE = InnoDB,
PARTITION before20180309191942 VALUES LESS THAN (63687842382) ENGINE = InnoDB,
PARTITION before20180309192542 VALUES LESS THAN (63687842742) ENGINE = InnoDB,
PARTITION before20180309193142 VALUES LESS THAN (63687843102) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_vm_primary_keys BEFORE INSERT ON vm_stats_latest
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
PARTITION before20180110071349 VALUES LESS THAN (63682787629) ENGINE = InnoDB,
PARTITION before20180111210149 VALUES LESS THAN (63682923709) ENGINE = InnoDB,
PARTITION before20180113104949 VALUES LESS THAN (63683059789) ENGINE = InnoDB,
PARTITION before20180115003749 VALUES LESS THAN (63683195869) ENGINE = InnoDB,
PARTITION before20180116142549 VALUES LESS THAN (63683331949) ENGINE = InnoDB,
PARTITION before20180118041349 VALUES LESS THAN (63683468029) ENGINE = InnoDB,
PARTITION before20180119180149 VALUES LESS THAN (63683604109) ENGINE = InnoDB,
PARTITION before20180121074949 VALUES LESS THAN (63683740189) ENGINE = InnoDB,
PARTITION before20180122213749 VALUES LESS THAN (63683876269) ENGINE = InnoDB,
PARTITION before20180124112549 VALUES LESS THAN (63684012349) ENGINE = InnoDB,
PARTITION before20180126011349 VALUES LESS THAN (63684148429) ENGINE = InnoDB,
PARTITION before20180127150149 VALUES LESS THAN (63684284509) ENGINE = InnoDB,
PARTITION before20180129044949 VALUES LESS THAN (63684420589) ENGINE = InnoDB,
PARTITION before20180130183749 VALUES LESS THAN (63684556669) ENGINE = InnoDB,
PARTITION before20180201082549 VALUES LESS THAN (63684692749) ENGINE = InnoDB,
PARTITION before20180202221349 VALUES LESS THAN (63684828829) ENGINE = InnoDB,
PARTITION before20180204120149 VALUES LESS THAN (63684964909) ENGINE = InnoDB,
PARTITION before20180206014949 VALUES LESS THAN (63685100989) ENGINE = InnoDB,
PARTITION before20180207153749 VALUES LESS THAN (63685237069) ENGINE = InnoDB,
PARTITION before20180209052549 VALUES LESS THAN (63685373149) ENGINE = InnoDB,
PARTITION before20180210191349 VALUES LESS THAN (63685509229) ENGINE = InnoDB,
PARTITION before20180212090149 VALUES LESS THAN (63685645309) ENGINE = InnoDB,
PARTITION before20180213224949 VALUES LESS THAN (63685781389) ENGINE = InnoDB,
PARTITION before20180215123749 VALUES LESS THAN (63685917469) ENGINE = InnoDB,
PARTITION before20180217022549 VALUES LESS THAN (63686053549) ENGINE = InnoDB,
PARTITION before20180218161349 VALUES LESS THAN (63686189629) ENGINE = InnoDB,
PARTITION before20180220060149 VALUES LESS THAN (63686325709) ENGINE = InnoDB,
PARTITION before20180221194949 VALUES LESS THAN (63686461789) ENGINE = InnoDB,
PARTITION before20180223093749 VALUES LESS THAN (63686597869) ENGINE = InnoDB,
PARTITION before20180224232549 VALUES LESS THAN (63686733949) ENGINE = InnoDB,
PARTITION before20180226131349 VALUES LESS THAN (63686870029) ENGINE = InnoDB,
PARTITION before20180228030149 VALUES LESS THAN (63687006109) ENGINE = InnoDB,
PARTITION before20180301164949 VALUES LESS THAN (63687142189) ENGINE = InnoDB,
PARTITION before20180303063749 VALUES LESS THAN (63687278269) ENGINE = InnoDB,
PARTITION before20180304202549 VALUES LESS THAN (63687414349) ENGINE = InnoDB,
PARTITION before20180306101349 VALUES LESS THAN (63687550429) ENGINE = InnoDB,
PARTITION before20180308000149 VALUES LESS THAN (63687686509) ENGINE = InnoDB,
PARTITION before20180309134949 VALUES LESS THAN (63687822589) ENGINE = InnoDB,
PARTITION before20180311033749 VALUES LESS THAN (63687958669) ENGINE = InnoDB,
PARTITION before20180312172549 VALUES LESS THAN (63688094749) ENGINE = InnoDB,
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
PARTITION before20180306192550 VALUES LESS THAN (63687583550) ENGINE = InnoDB,
PARTITION before20180306212550 VALUES LESS THAN (63687590750) ENGINE = InnoDB,
PARTITION before20180306232550 VALUES LESS THAN (63687597950) ENGINE = InnoDB,
PARTITION before20180307012550 VALUES LESS THAN (63687605150) ENGINE = InnoDB,
PARTITION before20180307032550 VALUES LESS THAN (63687612350) ENGINE = InnoDB,
PARTITION before20180307052550 VALUES LESS THAN (63687619550) ENGINE = InnoDB,
PARTITION before20180307072550 VALUES LESS THAN (63687626750) ENGINE = InnoDB,
PARTITION before20180307092550 VALUES LESS THAN (63687633950) ENGINE = InnoDB,
PARTITION before20180307112550 VALUES LESS THAN (63687641150) ENGINE = InnoDB,
PARTITION before20180307132550 VALUES LESS THAN (63687648350) ENGINE = InnoDB,
PARTITION before20180307152550 VALUES LESS THAN (63687655550) ENGINE = InnoDB,
PARTITION before20180307172550 VALUES LESS THAN (63687662750) ENGINE = InnoDB,
PARTITION before20180307192550 VALUES LESS THAN (63687669950) ENGINE = InnoDB,
PARTITION before20180307212550 VALUES LESS THAN (63687677150) ENGINE = InnoDB,
PARTITION before20180307232550 VALUES LESS THAN (63687684350) ENGINE = InnoDB,
PARTITION before20180308012550 VALUES LESS THAN (63687691550) ENGINE = InnoDB,
PARTITION before20180308032550 VALUES LESS THAN (63687698750) ENGINE = InnoDB,
PARTITION before20180308052550 VALUES LESS THAN (63687705950) ENGINE = InnoDB,
PARTITION before20180308072550 VALUES LESS THAN (63687713150) ENGINE = InnoDB,
PARTITION before20180308092550 VALUES LESS THAN (63687720350) ENGINE = InnoDB,
PARTITION before20180308112550 VALUES LESS THAN (63687727550) ENGINE = InnoDB,
PARTITION before20180308132550 VALUES LESS THAN (63687734750) ENGINE = InnoDB,
PARTITION before20180308152550 VALUES LESS THAN (63687741950) ENGINE = InnoDB,
PARTITION before20180308172550 VALUES LESS THAN (63687749150) ENGINE = InnoDB,
PARTITION before20180308192550 VALUES LESS THAN (63687756350) ENGINE = InnoDB,
PARTITION before20180308212550 VALUES LESS THAN (63687763550) ENGINE = InnoDB,
PARTITION before20180308232550 VALUES LESS THAN (63687770750) ENGINE = InnoDB,
PARTITION before20180309012550 VALUES LESS THAN (63687777950) ENGINE = InnoDB,
PARTITION before20180309032550 VALUES LESS THAN (63687785150) ENGINE = InnoDB,
PARTITION before20180309052550 VALUES LESS THAN (63687792350) ENGINE = InnoDB,
PARTITION before20180309072550 VALUES LESS THAN (63687799550) ENGINE = InnoDB,
PARTITION before20180309092550 VALUES LESS THAN (63687806750) ENGINE = InnoDB,
PARTITION before20180309112550 VALUES LESS THAN (63687813950) ENGINE = InnoDB,
PARTITION before20180309132550 VALUES LESS THAN (63687821150) ENGINE = InnoDB,
PARTITION before20180309152550 VALUES LESS THAN (63687828350) ENGINE = InnoDB,
PARTITION before20180309172550 VALUES LESS THAN (63687835550) ENGINE = InnoDB,
PARTITION before20180309192550 VALUES LESS THAN (63687842750) ENGINE = InnoDB,
PARTITION before20180309212550 VALUES LESS THAN (63687849950) ENGINE = InnoDB,
PARTITION before20180309232550 VALUES LESS THAN (63687857150) ENGINE = InnoDB,
PARTITION before20180310012550 VALUES LESS THAN (63687864350) ENGINE = InnoDB,
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
PARTITION before20160314093752 VALUES LESS THAN (63625167472) ENGINE = InnoDB,
PARTITION before20160402014952 VALUES LESS THAN (63626780992) ENGINE = InnoDB,
PARTITION before20160420180152 VALUES LESS THAN (63628394512) ENGINE = InnoDB,
PARTITION before20160509101352 VALUES LESS THAN (63630008032) ENGINE = InnoDB,
PARTITION before20160528022552 VALUES LESS THAN (63631621552) ENGINE = InnoDB,
PARTITION before20160615183752 VALUES LESS THAN (63633235072) ENGINE = InnoDB,
PARTITION before20160704104952 VALUES LESS THAN (63634848592) ENGINE = InnoDB,
PARTITION before20160723030152 VALUES LESS THAN (63636462112) ENGINE = InnoDB,
PARTITION before20160810191352 VALUES LESS THAN (63638075632) ENGINE = InnoDB,
PARTITION before20160829112552 VALUES LESS THAN (63639689152) ENGINE = InnoDB,
PARTITION before20160917033752 VALUES LESS THAN (63641302672) ENGINE = InnoDB,
PARTITION before20161005194952 VALUES LESS THAN (63642916192) ENGINE = InnoDB,
PARTITION before20161024120152 VALUES LESS THAN (63644529712) ENGINE = InnoDB,
PARTITION before20161112041352 VALUES LESS THAN (63646143232) ENGINE = InnoDB,
PARTITION before20161130202552 VALUES LESS THAN (63647756752) ENGINE = InnoDB,
PARTITION before20161219123752 VALUES LESS THAN (63649370272) ENGINE = InnoDB,
PARTITION before20170107044952 VALUES LESS THAN (63650983792) ENGINE = InnoDB,
PARTITION before20170125210152 VALUES LESS THAN (63652597312) ENGINE = InnoDB,
PARTITION before20170213131352 VALUES LESS THAN (63654210832) ENGINE = InnoDB,
PARTITION before20170304052552 VALUES LESS THAN (63655824352) ENGINE = InnoDB,
PARTITION before20170322213752 VALUES LESS THAN (63657437872) ENGINE = InnoDB,
PARTITION before20170410134952 VALUES LESS THAN (63659051392) ENGINE = InnoDB,
PARTITION before20170429060152 VALUES LESS THAN (63660664912) ENGINE = InnoDB,
PARTITION before20170517221352 VALUES LESS THAN (63662278432) ENGINE = InnoDB,
PARTITION before20170605142552 VALUES LESS THAN (63663891952) ENGINE = InnoDB,
PARTITION before20170624063752 VALUES LESS THAN (63665505472) ENGINE = InnoDB,
PARTITION before20170712224952 VALUES LESS THAN (63667118992) ENGINE = InnoDB,
PARTITION before20170731150152 VALUES LESS THAN (63668732512) ENGINE = InnoDB,
PARTITION before20170819071352 VALUES LESS THAN (63670346032) ENGINE = InnoDB,
PARTITION before20170906232552 VALUES LESS THAN (63671959552) ENGINE = InnoDB,
PARTITION before20170925153752 VALUES LESS THAN (63673573072) ENGINE = InnoDB,
PARTITION before20171014074952 VALUES LESS THAN (63675186592) ENGINE = InnoDB,
PARTITION before20171102000152 VALUES LESS THAN (63676800112) ENGINE = InnoDB,
PARTITION before20171120161352 VALUES LESS THAN (63678413632) ENGINE = InnoDB,
PARTITION before20171209082552 VALUES LESS THAN (63680027152) ENGINE = InnoDB,
PARTITION before20171228003752 VALUES LESS THAN (63681640672) ENGINE = InnoDB,
PARTITION before20180115164952 VALUES LESS THAN (63683254192) ENGINE = InnoDB,
PARTITION before20180203090152 VALUES LESS THAN (63684867712) ENGINE = InnoDB,
PARTITION before20180222011352 VALUES LESS THAN (63686481232) ENGINE = InnoDB,
PARTITION before20180312172552 VALUES LESS THAN (63688094752) ENGINE = InnoDB,
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
PARTITION before20180309153748 VALUES LESS THAN (63687829068) ENGINE = InnoDB,
PARTITION before20180309154348 VALUES LESS THAN (63687829428) ENGINE = InnoDB,
PARTITION before20180309154948 VALUES LESS THAN (63687829788) ENGINE = InnoDB,
PARTITION before20180309155548 VALUES LESS THAN (63687830148) ENGINE = InnoDB,
PARTITION before20180309160148 VALUES LESS THAN (63687830508) ENGINE = InnoDB,
PARTITION before20180309160748 VALUES LESS THAN (63687830868) ENGINE = InnoDB,
PARTITION before20180309161348 VALUES LESS THAN (63687831228) ENGINE = InnoDB,
PARTITION before20180309161948 VALUES LESS THAN (63687831588) ENGINE = InnoDB,
PARTITION before20180309162548 VALUES LESS THAN (63687831948) ENGINE = InnoDB,
PARTITION before20180309163148 VALUES LESS THAN (63687832308) ENGINE = InnoDB,
PARTITION before20180309163748 VALUES LESS THAN (63687832668) ENGINE = InnoDB,
PARTITION before20180309164348 VALUES LESS THAN (63687833028) ENGINE = InnoDB,
PARTITION before20180309164948 VALUES LESS THAN (63687833388) ENGINE = InnoDB,
PARTITION before20180309165548 VALUES LESS THAN (63687833748) ENGINE = InnoDB,
PARTITION before20180309170148 VALUES LESS THAN (63687834108) ENGINE = InnoDB,
PARTITION before20180309170748 VALUES LESS THAN (63687834468) ENGINE = InnoDB,
PARTITION before20180309171348 VALUES LESS THAN (63687834828) ENGINE = InnoDB,
PARTITION before20180309171948 VALUES LESS THAN (63687835188) ENGINE = InnoDB,
PARTITION before20180309172548 VALUES LESS THAN (63687835548) ENGINE = InnoDB,
PARTITION before20180309173148 VALUES LESS THAN (63687835908) ENGINE = InnoDB,
PARTITION before20180309173748 VALUES LESS THAN (63687836268) ENGINE = InnoDB,
PARTITION before20180309174348 VALUES LESS THAN (63687836628) ENGINE = InnoDB,
PARTITION before20180309174948 VALUES LESS THAN (63687836988) ENGINE = InnoDB,
PARTITION before20180309175548 VALUES LESS THAN (63687837348) ENGINE = InnoDB,
PARTITION before20180309180148 VALUES LESS THAN (63687837708) ENGINE = InnoDB,
PARTITION before20180309180748 VALUES LESS THAN (63687838068) ENGINE = InnoDB,
PARTITION before20180309181348 VALUES LESS THAN (63687838428) ENGINE = InnoDB,
PARTITION before20180309181948 VALUES LESS THAN (63687838788) ENGINE = InnoDB,
PARTITION before20180309182548 VALUES LESS THAN (63687839148) ENGINE = InnoDB,
PARTITION before20180309183148 VALUES LESS THAN (63687839508) ENGINE = InnoDB,
PARTITION before20180309183748 VALUES LESS THAN (63687839868) ENGINE = InnoDB,
PARTITION before20180309184348 VALUES LESS THAN (63687840228) ENGINE = InnoDB,
PARTITION before20180309184948 VALUES LESS THAN (63687840588) ENGINE = InnoDB,
PARTITION before20180309185548 VALUES LESS THAN (63687840948) ENGINE = InnoDB,
PARTITION before20180309190148 VALUES LESS THAN (63687841308) ENGINE = InnoDB,
PARTITION before20180309190748 VALUES LESS THAN (63687841668) ENGINE = InnoDB,
PARTITION before20180309191348 VALUES LESS THAN (63687842028) ENGINE = InnoDB,
PARTITION before20180309191948 VALUES LESS THAN (63687842388) ENGINE = InnoDB,
PARTITION before20180309192548 VALUES LESS THAN (63687842748) ENGINE = InnoDB,
PARTITION before20180309193148 VALUES LESS THAN (63687843108) ENGINE = InnoDB,
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
/*!50003 CREATE*/ /*!50017 DEFINER=`vmtplatform`@`%`*/ /*!50003 TRIGGER set_vpod_primary_keys BEFORE INSERT ON vpod_stats_latest
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `aggregate_cluster_event` ON SCHEDULE EVERY 10 MINUTE STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `aggregate_spend_event` ON SCHEDULE EVERY 1 HOUR STARTS '2018-03-09 17:25:55' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `aggregate_stats_event` ON SCHEDULE EVERY 10 MINUTE STARTS '2018-03-09 17:32:36' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `purge_audit_log_expired_days` ON SCHEDULE EVERY 1 DAY STARTS '2018-03-09 17:26:00' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `purge_expired_days_cluster` ON SCHEDULE EVERY 1 DAY STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `purge_expired_days_spend` ON SCHEDULE EVERY 1 DAY STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `purge_expired_hours_spend` ON SCHEDULE EVERY 1 HOUR STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `purge_expired_months_cluster` ON SCHEDULE EVERY 1 MONTH STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
/*!50106 CREATE*/ /*!50117 DEFINER=`vmtplatform`@`%`*/ /*!50106 EVENT `purge_expired_months_spend` ON SCHEDULE EVERY 1 MONTH STARTS '2018-03-09 17:25:56' ON COMPLETION NOT PRESERVE ENABLE DO BEGIN
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
CREATE DEFINER=`vmtplatform`@`%` FUNCTION `CHECKAGGR`() RETURNS int(11)
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `cluster_nm1_factor`(arg_group_name varchar(255)) RETURNS float
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `cluster_nm2_factor`(arg_group_name varchar(255)) RETURNS float
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `datetime_from_ms`(ms_time bigint) RETURNS datetime
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `date_from_ms`(ms_time bigint) RETURNS date
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `days_ago`(ndays int) RETURNS date
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `days_from_now`(ndays int) RETURNS date
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `end_of_day`(ref_date date) RETURNS date
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `end_of_hour`(ref_date timestamp) RETURNS timestamp
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `end_of_month`(ref_date date) RETURNS date
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `end_of_month_ms`(ref_date date) RETURNS bigint(20)
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `ftn_pm_count_for_month`(month_day_1 date) RETURNS int(11)
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `ftn_vm_count_for_month`(month_day_1 date) RETURNS int(11)
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `ms_from_date`(the_date date) RETURNS bigint(20)
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `ms_from_datetime`(the_datetime datetime) RETURNS bigint(20)
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `start_of_day`(ref_date datetime) RETURNS datetime
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `start_of_hour`(ref_date timestamp) RETURNS timestamp
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `start_of_month`(ref_date datetime) RETURNS datetime
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
CREATE DEFINER=`vmtplatform`@`localhost` FUNCTION `start_of_month_ms`(ref_date date) RETURNS bigint(20)
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `aggregate`(IN statspref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `aggregateClusterStats`()
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
  sum(a.value) as samples
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `aggregateSpend`(IN spendpref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`localhost` PROCEDURE `clusterAggPreviousDay`(IN cluster_internal_name varchar(250))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `market_aggregate`(IN statspref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`localhost` PROCEDURE `populate_AllClusters_PreviousDayAggStats`()
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_days`(IN statspref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_days_cluster`()
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_days_spend`(IN spendpref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_hours`(IN statspref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_hours_spend`(IN spendpref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_latest`(IN statspref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_months`(IN statspref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_months_cluster`()
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `purge_expired_months_spend`(IN spendpref CHAR(10))
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `rotate_partition`(IN stats_table CHAR(30))
  BEGIN

    # sql statement to be executed
    DECLARE sql_statement varchar (1000);

    DECLARE done INT DEFAULT FALSE;

    # name of the partition that we are iterating through
    # partitions name follow this pattern: beforeYYYYMMDDHHmmSS
    DECLARE part_name CHAR(22);

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
    set @num_seconds = NULL;
    set @num_seconds_for_future = NULL;
    set @retention_type := (select substring_index(stats_table, '_', -1));
    CASE @retention_type
      WHEN 'latest' then
      set @num_seconds := (select retention_period from retention_policies where policy_name='retention_latest_hours')*60*60;
      # set future to 2 hours
      set @num_seconds_for_future := 2*60*60;

      WHEN 'hour' THEN
      set @num_seconds := (select retention_period from retention_policies where policy_name='retention_hours')*60*60;
      # set future to 8 hours
      set @num_seconds_for_future := 8*60*60;

      when 'day' then
      set @num_seconds := (select retention_period from retention_policies where policy_name='retention_days')*24*60*60;
      # set future to 3 days
      set @num_seconds_for_future := 3*24*60*60;

      when 'month' then
      set @num_seconds := (select retention_period from retention_policies where policy_name='retention_months')*31*24*60*60;
      # set future to 3 days
      set @num_seconds_for_future := 3*24*60*60;
    END CASE;



    #calculate what should be the last partition from the past
    set @last_part := (select date_sub(current_timestamp, INTERVAL @num_seconds SECOND));
    set @last_part_compact := YEAR(@last_part)*10000000000 + MONTH(@last_part)*100000000 + DAY(@last_part)*1000000 + hour(@last_part)*10000 + minute(@last_part)*100 + second(@last_part);

    # create future partitions for next X hours/days
    set @future_part := (select date_add(current_timestamp, INTERVAL @num_seconds_for_future SECOND));
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
    set @add_part := (select date_add(@max_part, INTERVAL @delta SECOND));
    set @add_part_compact := YEAR(@add_part)*10000000000 + MONTH(@add_part)*100000000 + DAY(@add_part)*1000000 + hour(@add_part)*10000 + minute(@add_part)*100 + second(@add_part);

    # continue adding the delta until we reach the future date
    WHILE @add_part_compact <= @future_part_compact DO

      # append another partition
      set @sql_statement = concat(@sql_statement, 'partition before', @add_part_compact, ' VALUES LESS THAN (to_seconds(\'', @add_part, '\')), ');

      # increase the date by another delta
      set @add_part := (select date_add(@add_part, INTERVAL @delta SECOND));
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `supplychain_stats_roll_up`()
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `trigger_purge_expired`()
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
CREATE DEFINER=`vmtplatform`@`%` PROCEDURE `trigger_rotate_partition`()
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`localhost` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
  /*!50013 DEFINER=`vmtplatform`@`%` SQL SECURITY DEFINER */
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
