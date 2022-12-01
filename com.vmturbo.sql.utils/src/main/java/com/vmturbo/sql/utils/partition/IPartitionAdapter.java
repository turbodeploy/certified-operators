package com.vmturbo.sql.utils.partition;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.annotation.Nullable;

/**
 * Interface for a dialect-specific database partitioning adapter.
 */
public interface IPartitionAdapter {

    /**
     * Retrieve details of existing partitions for tables in the given schema.
     *
     * <p>"Bookend" partitions (e.g. the typical MariaDB "start" and "future" partitions with
     * boundaries set to 0 and MAXINT, respectively) are not retrieved. These are partitions in
     * which no records are ever intended to appear.</p>
     *
     * <p>Tables that are declared as partitioned but for which no partitions (except bookends)
     * exist will not be represented in the results.</p>
     *
     * @param schemaName schema whose partitioning info is desired
     * @return map of table names to lists of table partitions
     * @throws PartitionProcessingException if there's a problem retrieving partition info
     */
    Map<String, List<Partition<Instant>>> getSchemaPartitions(String schemaName)
            throws PartitionProcessingException;

    /**
     * Retrieve details of existing partitions for tables in the given table.
     *
     * <p>This method is used in test classes for simplicity, but due to potential cost of loading
     * this information, direct use of this metho is not recommended. Loading all partition info for
     * the schema using {@link #getSchemaPartitions(String)} will generally be more efficient
     * becuase costs will be amortized across multiple tables.
     *
     * @param schemaName name of schema
     * @param tableName  name of table
     * @return list of partitions currently present on the table
     * @throws PartitionProcessingException if there's an issue
     */
    default List<Partition<Instant>> getSchemaPartitions(String schemaName, String tableName)
            throws PartitionProcessingException {
        return getSchemaPartitions(schemaName).getOrDefault(tableName, Collections.emptyList());
    }

    /**
     * Create a new partition.
     *
     * @param schemaName    name of scheme containing table
     * @param tableName     name of table
     * @param fromInclusive lower bound of partition
     * @param toExclusive   upper bound of partition
     * @param followingPartition the next closest ascending partition, if one exists, or null
     * @return a {@link Partition} instance representing the new partition
     * @throws PartitionProcessingException if there's a problem creating the partition
     */
    Partition<Instant> createPartition(String schemaName, String tableName,
            Instant fromInclusive, Instant toExclusive, @Nullable Partition<Instant> followingPartition)
            throws PartitionProcessingException;

    /**
     * Create a new partition. The next closest ascending partition will be assumed <code>null</code>.
     *
     * @param schemaName    name of schema containing table
     * @param tableName     name of table
     * @param fromInclusive lower bound of partition
     * @param toExclusive   upper bound of partition
     * @return a {@link Partition} instance representing the new partition
     * @throws PartitionProcessingException if there's a problem creating the partition
     */
    default Partition<Instant> createPartition(String schemaName, String tableName,
            Instant fromInclusive, Instant toExclusive) throws PartitionProcessingException {
        return createPartition(schemaName, tableName, fromInclusive, toExclusive, null);
    }

    /**
     * Drop an existing partition.
     *
     * @param partition partition to drop
     * @throws PartitionProcessingException if there's a problem dropping the partition
     */
    void dropPartition(Partition<Instant> partition) throws PartitionProcessingException;
}
