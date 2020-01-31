-- NOTE: This migration is a copy of the migration V1.24.2, repeated here so that it will be
-- executed by clients upgrading from a prior 7.21 branch release will execute the migration
-- when upgrading to the 7.21.0 release where 7.17 and 7.21 branches of history component were
-- unified.

/**
 * We previously stopped triggering rollup processing in the body of a recurring DB event, but
 * we continued to trigger repartitioning of stats tables from that event. We now perform
 * repartitioning from Java code as well, so this event is no longer needed at all. Nor is the
 * "trigger_rotate_partition" stored proc that was called from the event body.
 */
DROP EVENT IF EXISTS `repartition_stats_event`;
DROP PROCEDURE IF EXISTS `trigger_rotate_partition`;
