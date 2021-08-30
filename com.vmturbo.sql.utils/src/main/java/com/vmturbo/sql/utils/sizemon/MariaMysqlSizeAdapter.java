package com.vmturbo.sql.utils.sizemon;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Record6;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity;

/**
 * {@link DbSizeAdapter} for MariaDB or MySQL database.
 */
public class MariaMysqlSizeAdapter extends DbSizeAdapter {

    private static final Table<Record> PARTITIONS_TABLE = DSL.table("information_schema.partitions");
    private static final Field<String> PARTITION_NAME_FIELD = DSL.field("partition_name", String.class);
    private static final Field<Long> PARTITION_ORDINAL_POSITION_FIELD = DSL.field("partition_ordinal_position", Long.class);
    private static final Field<Long> DATA_FREE_FIELD = DSL.field("data_free", Long.class);

    private final Map<String, List<SizeInfo>> sizeInfos;

    /**
     * Create a new instance.
     *  @param dsl         {@link DSLContext} for DB access
     * @param schema      schema to be interrogated
     */
    public MariaMysqlSizeAdapter(DSLContext dsl, Schema schema) {
        super(dsl, schema);
        this.sizeInfos = loadPartitionInfo();
    }

    /**
     * Rather than doing a separate query to get table names, we can just get it from the {@link
     * SizeInfo} objects we got from the scan in the constructor.
     *
     * @return tables in schema
     */
    @Override
    public List<Table<?>> getTables() {
        return sizeInfos.keySet().stream()
                .sorted()
                .map(schema::getTable)
                // skip anything not known to jOOQ (like flyway's schema-version)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public List<SizeItem> getSizeItems(Table<?> table) {
        // compute totals across all the partitions associated with this table
        final List<SizeInfo> partitions = sizeInfos.getOrDefault(table.getName(), Collections.emptyList());
        long totData = partitions.stream().mapToLong(pi -> pi.dataSize).sum();
        long totIndex = partitions.stream().mapToLong(pi -> pi.indexSize).sum();
        long totFree = partitions.stream().mapToLong(pi -> pi.freeSize).sum();
        long totRecs = partitions.stream().mapToLong(pi -> pi.recordCount).sum();
        List<SizeItem> items = new ArrayList<>();
        // use that to create a table-level size item
        items.add(formatSizeItem(null, table.getName(), totData, totIndex, totFree, totRecs));
        // then add per-partition info if we have partitions are are logging at partition level
        if (partitions.size() > 1) {
            for (int i = 0; i < partitions.size(); i++) {
                SizeInfo si = partitions.get(i);
                items.add(formatSizeItem(i + 1, si.partitionName,
                        si.dataSize, si.indexSize, si.freeSize, si.recordCount));
            }
        }
        return items;
    }

    /**
     * Get size info on every partition for tables in the schema. This will include a single
     * partition records for any non-partitioned table, so we don't need to scan tables if we're
     * scanning partitions anyway.
     *
     * @return SizeInfo objects organized by table
     */
    private Map<String, List<SizeInfo>> loadPartitionInfo() {
        try (Stream<Record6<String, String, Long, Long, Long, Long>> stream = dsl.select(
                    TABLE_NAME_FIELD, PARTITION_NAME_FIELD, DATA_LENGTH_FIELD, INDEX_LENGTH_FIELD,
                    DATA_FREE_FIELD, TABLE_ROWS_FIELD)
                .from(PARTITIONS_TABLE)
                .where(TABLE_SCHEMA_FIELD.eq(schema.getName()))
                .orderBy(TABLE_NAME_FIELD, PARTITION_ORDINAL_POSITION_FIELD)
                .stream()) {
            return stream
                    .map(r -> new SizeInfo(
                            r.value1(), r.value2(), r.value3(), r.value4(), r.value5(), r.value6()))
                    .collect(Collectors.groupingBy(sizeInfo -> sizeInfo.tableName));
        }
    }

    private SizeItem formatSizeItem(final Integer itemNo, final String itemName,
            final long dataSize, final long indexSize, final long free, final long recordCount) {
        String name = itemNo != null
                ? String.format("Partition [#%d: %s]", itemNo, itemName)
                : String.format("Table %s", itemName);
        return new SizeItem(itemNo != null ? Granularity.PARTITION : Granularity.TABLE, dataSize + indexSize + free,
                String.format("%s: (%d data, %d index, %d free, %d rows)", name, dataSize, indexSize, free, recordCount));
    }

    /**
     * POJO for size information for a table or a partition.
     */
    private static class SizeInfo {

        private final String tableName;
        private final String partitionName;
        private final long dataSize;
        private final long indexSize;
        private final long freeSize;
        private final long recordCount;

        /**
         * Create a new instance.
         *
         * @param tableName     table name
         * @param partitionName partition name, or null if this is an unpartitioned table
         * @param dataSize      size of table data
         * @param indexSize     combined size of all indexes
         * @param freeSize      free space available in table
         * @param recordCount   number of records
         */
        SizeInfo(String tableName, String partitionName,
                long dataSize, long indexSize, long freeSize, long recordCount) {
            this.tableName = tableName;
            this.partitionName = partitionName;
            this.dataSize = dataSize;
            this.indexSize = indexSize;
            this.freeSize = freeSize;
            this.recordCount = recordCount;
        }
    }
}
