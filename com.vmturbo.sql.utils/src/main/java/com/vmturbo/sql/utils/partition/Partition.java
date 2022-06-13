package com.vmturbo.sql.utils.partition;

/**
 * Class to represent a single partition.
 *
 * @param <T> type used to represent partition boundary values (not necessarily the same as the
 *            representation in the database).
 */
public class Partition<T extends Comparable<T>> {

    private final String tableName;
    private final String schemaName;
    private final String partitionName;
    private final T inclusiveLower;
    private final T exclusiveUpper;

    /**
     * Create a new instance.
     *
     * @param schemaName     schema name of partitioned table
     * @param tableName      table name of partitioned table
     * @param partitionName  partition name
     * @param inclusiveLower inclusive lower bound for partition key
     * @param exclusiveUpper exclusive upper bound for partition key
     */
    public Partition(String schemaName, String tableName, String partitionName, T inclusiveLower,
            T exclusiveUpper) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.partitionName = partitionName;
        this.inclusiveLower = inclusiveLower;
        this.exclusiveUpper = exclusiveUpper;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public T getInclusiveLower() {
        return inclusiveLower;
    }

    public T getExclusiveUpper() {
        return exclusiveUpper;
    }

    @Override
    public String toString() {
        return String.format("%s[%s, %s)", partitionName, inclusiveLower, exclusiveUpper);
    }

    /**
     * Check whether the given value is within the bounds of this {@link Partition} instance.
     *
     * @param value value to be checked
     * @return true if value is within this instance's bounds
     */
    public boolean contains(T value) {
        return value.compareTo(inclusiveLower) >= 0 && value.compareTo(exclusiveUpper) < 0;
    }
}
