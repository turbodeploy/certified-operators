-- Creates a table for keeping track of the number of days an application service has been empty.
-- Only empty app service plans should be stored in this table.

CREATE TABLE IF NOT EXISTS `application_service_days_empty` (
  `id` bigint(20) NOT NULL,
  `name` varchar(80) COLLATE utf8_unicode_ci NOT NULL DEFAULT '',
  `first_discovered` datetime NOT NULL,
  `last_discovered` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci