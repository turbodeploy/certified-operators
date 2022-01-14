-- NOTE: This migration is a copy of the migration V1.30, repeated here so that it will be
-- executed by clients upgrading from a prior 7.21 branch release will execute the migration
-- when upgrading to the 7.21.0 release where 7.17 and 7.21 branches of history component were
-- unified.
/*
 * In Migration V1.24.6 we dropped and recreated the system_load table, and in order
 * to support retention processing that was introduced in migration V1.23 in the same release,
 * we reordered the members of the existing composite index. This ended up badly affecting
 * performance of certain system_load queries. So here we reconstitute the original index
 * and add a separate index that will address the needs of retention processing
 */

-- there's no DROP INDEX IF EXISTS, so...
DROP PROCEDURE IF EXISTS _change_indexes;
DELIMITER //
CREATE PROCEDURE _change_indexes()
CHANGE_INDEXES:BEGIN
    -- first thing we'll do is make sure the correct index is not already in place, since
    -- in upgrades from 7.17.9 branch or later 7.17 branches, this will already have been
    -- done as migration V1.30. That conflicted with a different migration in 7.21.0, so
    -- during unification of those branches, this migration was renumered to 7.36, and a
    -- callback was created to remove the V1.30 entry from the migrations table prior to
    -- running migrations. So let's avoid doing all the work of recreating these indexes
    -- if we don't need to.
    SET @unneeded = 0;
    BEGIN
        DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN
            SELECT 'Correct indexes already in place; skipping index changes.';
            set @unneeded = 1;
        END;
        CREATE INDEX slice_property_time ON system_load(slice, property_type, snapshot_time);
    END;
    IF @unneeded THEN LEAVE CHANGE_INDEXES; END IF;

    -- if we make it here we just created slice_property_time index, which means it
    -- didn't previously exist. So we should delete the unwanted index we probably have,
    -- and create the `time` index which also should not exist (and if it does we won't
    -- trust it).
    BEGIN
        DECLARE CONTINUE HANDLER FOR SQLEXCEPTION
            SELECT CONCAT('Index not found so not dropped: ', @index, ' on system_load');
        SET @index='snapshot_time_slice_property_type';
        DROP INDEX snapshot_time_slice_property_type ON system_load;
        SET @index='time';
        DROP INDEX time ON system_load;
    END;

    -- finally, create the `time` index the way we want it
    CREATE INDEX time ON system_load(snapshot_time);
END //
CALL _change_indexes() //
DROP PROCEDURE _change_indexes //
DELIMITER ;

