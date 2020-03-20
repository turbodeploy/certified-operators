/*
  Modify the "vm_capacity_by_day" view to support report like "VM 15 Top Bottom Capacity".
  XL doesn't persist "utilization" property_subtype in vm_stats_by_* table. But this view requires
  this subtype. The changes will replace subtype "utilization" as "used", and related value (e.g. avg_value)
  to round(value/capacity, 3).
*/
-- MySQL dump 10.16  Distrib 10.2.10-MariaDB, for osx10.12 (x86_64)
--
-- Host: 127.0.0.1    Database: vmtdb
-- ------------------------------------------------------
-- Server version	10.1.34-MariaDB-0ubuntu0.18.04.1

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
/*!50001 VIEW `vm_capacity_by_day` AS select 'VirtualMachine' AS `class_name`,`vm_stats_by_day`.`uuid` AS `uuid`,`vm_stats_by_day`.`producer_uuid` AS `producer_uuid`,`vm_stats_by_day`.`property_type` AS `property_type`,round((`vm_stats_by_day`.`avg_value` / `vm_stats_by_day`.`capacity`),3) AS `utilization`,`vm_stats_by_day`.`capacity` AS `capacity`,`vm_stats_by_day`.`avg_value` AS `used_capacity`,round((1.0 - `vm_stats_by_day`.`avg_value`),0) AS `available_capacity`,cast(`vm_stats_by_day`.`snapshot_time` as date) AS `recorded_on` from (`vm_stats_by_day` join `vm_instances`) where ((`vm_stats_by_day`.`uuid` = `vm_instances`.`uuid`) and (`vm_stats_by_day`.`property_subtype` = 'used') and (`vm_stats_by_day`.`capacity` > 0.00)) */;
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

-- Dump completed on 2019-03-28 14:14:45
