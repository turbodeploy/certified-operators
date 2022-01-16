/*
 * In Migration V1.24.6 we dropped and recreated the system_load table, and in order
 * to support retention processing that was introduced in migration V1.23 in the same release,
 * we reordered the members of the existing composite index. This ended up badly affecting
 * performance of certain system_load queries. So here we reconstitute the original index
 * and add a separate index that will address the needs of retention processing
 */

-- there's no DROP INDEX IF EXISTS, so...
DROP PROCEDURE IF EXISTS _drop_indexes;
DELIMITER //
CREATE PROCEDURE _drop_indexes()
DROP_INDEXES:BEGIN
    -- unfortunately, MySQL and MariaDB don't agree on either SQLSTATE or error code for
    -- trying to drop an index that doesn't exist, so need to be a little less targeted
    -- in hte handler than would be ideal
    DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
        SELECT CONCAT('Index not found so not dropped: ', @index, ' on system_load');
    SET @index='snapshot_time_slice_property_type';
    DROP INDEX snapshot_time_slice_property_type ON system_load;
    SET @index='slice_property_time';
    DROP INDEX slice_property_time ON system_load;
    SET @index='time';
    DROP INDEX time ON system_load;
END //
CALL _drop_indexes() //
DROP PROCEDURE _drop_indexes //
DELIMITER ;

CREATE INDEX slice_property_time ON system_load(slice, property_type, snapshot_time);
CREATE INDEX time ON system_load(snapshot_time);
