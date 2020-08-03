/*
Migration is creating tables to support percentile feature with the same table structure as it was
in classic. For more details, please, look into percentile store data wiki page:
https://vmturbo.atlassian.net/wiki/spaces/PC/pages/623214725/Persistence+of+UtilizationCountStore+data
*/

DROP TABLE IF EXISTS `resource_utilization_counts`;

CREATE TABLE `resource_utilization_counts` (
  `resource_id` varchar(180) COLLATE utf8_unicode_ci,
  `utilization` INT,
  `counts` INT,
  `day_start_timestamp` timestamp,
  `resource_capacity` INT,
  PRIMARY KEY (`resource_id`, `utilization`, `day_start_timestamp`, `resource_capacity`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;

DROP TABLE IF EXISTS `resource_latest_timestamp`;

CREATE TABLE `resource_latest_timestamp` (
  `resource_id` varchar(180) COLLATE utf8_unicode_ci,
  `latest_stored_timestamp` timestamp,
   PRIMARY KEY (`resource_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
