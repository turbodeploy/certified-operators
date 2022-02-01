-- Drop the stored procedures and events which have been converted to java, thus no longer needed
-- this is only done when FF is enabled and dialect is MARIADB

DROP EVENT IF EXISTS `purge_expired_systemload_data`;
DROP EVENT IF EXISTS `purge_expired_percentile_data`;
DROP EVENT IF EXISTS `purge_expired_moving_statistics_data`;
DROP EVENT IF EXISTS `purge_audit_log_expired_days`;

DROP PROCEDURE IF EXISTS `purge_expired_data`;
DROP PROCEDURE IF EXISTS `purge_expired_cluster_stats`;