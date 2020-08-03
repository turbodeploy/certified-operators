-- Cleanup for the previous percentile implementation described here
-- https://vmturbo.atlassian.net/wiki/spaces/PC/pages/623214725/Persistence+of+UtilizationCountStore+data

DROP TABLE `resource_utilization_counts`;
DROP TABLE `resource_latest_timestamp`;


-- Creation percentile tables for new design described:
-- https://vmturbo.atlassian.net/wiki/spaces/XD/pages/1006174930/Change%2Bto%2Bpercentiles%2Bbackend%2Bin%2BXL%2Bvs%2Bclassic

DROP TABLE IF EXISTS `percentile_blobs`;

CREATE TABLE `percentile_blobs` (
    `start_timestamp` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
    `aggregation_window_length` bigint(20) NOT NULL DEFAULT '0',
    `data` longblob NULL,
    INDEX(`start_timestamp`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;
