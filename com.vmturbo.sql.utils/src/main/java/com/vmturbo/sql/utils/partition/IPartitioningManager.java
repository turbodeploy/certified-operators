package com.vmturbo.sql.utils.partition;

import java.sql.Timestamp;

import org.jooq.Table;

/**
 * Ensure that the given partitioned table is ready to receive records with a given timestamp.
 */
public interface IPartitioningManager {
    /**
     * Make sure there's a partition in the given table ready to accept records with the given time
     * value. If not, create one.
     *
     * @param table            table that will receive records
     * @param insertionTimestamp time value in those records
     * @throws PartitionProcessingException if there's a probem creating a new partition
     */
    void prepareForInsertion(Table<?> table, Timestamp insertionTimestamp)
            throws PartitionProcessingException;

    /**
     * Drop partitions that are expired according to the retention rules. And for good measure,
     * refresh those retention rules from the settings service. And this is also a good time to
     * re-scan the database for current partitioning details, in case a change occurred from some
     * other source, or we have a bug in our logic that resulted in our in-memory model no longer
     * matching what's in the database.
     */
    void performRetentionUpdate();
}
