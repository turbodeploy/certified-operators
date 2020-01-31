/**
 * We previously stopped triggering rollup processing in the body of a recurring DB event, but
 * we continued to trigger repartitioning of stats tables from that event. We now perform
 * repartitioning from Java code as well, so this event is no longer needed at all. Nor is the
 * "trigger_rotate_partition" stored proc that was called from the event body.
 */
DROP EVENT IF EXISTS `repartition_stats_event`;
DROP PROCEDURE IF EXISTS `trigger_rotate_partition`;
