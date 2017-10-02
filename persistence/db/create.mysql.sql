--
-- vmtdb
--

SET character_set_client = utf8 ;

DROP DATABASE IF EXISTS vmtdb;
CREATE DATABASE IF NOT EXISTS vmtdb DEFAULT CHARACTER SET = UTF8 DEFAULT COLLATE = utf8_unicode_ci;

USE vmtdb;

CREATE TABLE `is_persistence` (
  `oid`      bigint(8) unsigned NOT NULL,
  `field_1`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_2`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_3`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_4`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_5`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_6`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_7`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_8`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_9`  varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_10` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_11` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_12` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_13` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_14` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_15` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  `field_16` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL,
  PRIMARY KEY (`oid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
