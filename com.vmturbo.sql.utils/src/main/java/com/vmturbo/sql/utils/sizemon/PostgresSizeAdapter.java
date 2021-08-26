package com.vmturbo.sql.utils.sizemon;

import static com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity.PARTITION;
import static com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity.TABLE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Schema;
import org.jooq.Table;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.sizemon.DbSizeMonitor.Granularity;

/**
 * {@link DbSizeAdapter} for a PostgreSQL database with TimescaleDB extension.
 */
public class PostgresSizeAdapter extends DbSizeAdapter {
    protected static final Table<Record> HYPERTABLES_TABLE = DSL.table("timescaledb_information.hypertables");
    protected static final Field<String> HYPERTABLE_NAME_FIELD = DSL.field("hypertable_name", String.class);


    /** Names of all the hypertables defined in the schema. */
    private final Set<String> hypertables;
    private static final Field<String> HYPERTABLE_SCHEMA = DSL.field("hypertable_schema", String.class);

    /**
     * Create a new instance.
     *  @param dsl         {@link DSLContext} for access to the database
     * @param schema      name of schema to interrogate
     */
    protected PostgresSizeAdapter(DSLContext dsl, Schema schema) {
        super(dsl, schema);
        this.hypertables = dsl.select(HYPERTABLE_NAME_FIELD)
                .from(HYPERTABLES_TABLE)
                .where(HYPERTABLE_SCHEMA.eq(schema.getName()))
                .fetchSet(HYPERTABLE_NAME_FIELD);
    }

    @Override
    public List<SizeItem> getSizeItems(Table<?> table) {
        final boolean isHypertable = hypertables.contains(table.getName());
        return new TableInfo(table, isHypertable).getSizeItems();
    }

    /**
     * Class to collect size information for a table and package it up as {@link SizeItem}s.
     */
    private class TableInfo {
        private final String tableName;
        private final boolean isHypertable;

        // for a hypertable, these represent info on individual chunks; for a normal table it's
        // a single object for the table as a whole.
        private final List<SizeInfo> sizeInfos;

        /**
         * Create a new instance.
         *
         * @param table        table to interrogate
         * @param isHypertable whether this is a hypertable
         */
        TableInfo(Table<?> table, boolean isHypertable) {
            this.tableName = table.getName();
            this.isHypertable = isHypertable;
            sizeInfos = isHypertable
                    ? populateHypertableData()
                    : populateNormalTableData(table);
        }

        /**
         * Retrieve size data for all the chunks in a hypertable.
         *
         * @return SizeInfo for each chunk, ordered by reverse chronology of time range
         */
        private List<SizeInfo> populateHypertableData() {
            if (isHypertableCompressed()) {
                return getHypertableInfoWithCompression();
            } else {
                return getHypertableInfoWithoutCompression();
            }
        }

        private boolean isHypertableCompressed() {
            String sql = String.format("SELECT DISTINCT hypertable_name "
                            + "FROM timescaledb_information.compression_settings "
                            + "WHERE hypertable_schema='%1$s' and hypertable_name='%2$s'",
                    schema.getName(), tableName);
            return dsl.resultQuery(sql).fetchAny() != null;
        }

        private List<SizeInfo> getHypertableInfoWithCompression() {
            String sql = String.format("SELECT "
                            // chunk identity and compression state
                            + "  c.chunk_name, c.range_start, c.range_end, c.is_compressed, "
                            // position of this chunk in the reverse-chronological sequence
                            + "  row_number() OVER (ORDER BY range_start DESC) AS number, "
                            // before & after sizes for a compressed chunk (null for uncompressed)
                            + "  ccs.before_compression_table_bytes AS data, "
                            + "  ccs.after_compression_table_bytes AS compressed_data, "
                            + "  ccs.before_compression_index_bytes AS index, "
                            + "  ccs.after_compression_index_bytes AS compressed_index, "
                            + "  ccs.before_compression_toast_bytes AS toast, "
                            + "  ccs.after_compression_toast_bytes AS compressed_toast, "
                            // number of rows in chunk
                            + "  pgc.reltuples AS rows ,"
                            // size values for uncompressed chunks
                            + "  pg_relation_size(c.chunk_schema||'.'||c.chunk_name, 'main') AS pgrs_main, "
                            + "  pg_relation_size(c.chunk_schema||'.'||c.chunk_name, 'fsm') as pgrs_fsm, "
                            + "  pg_relation_size(c.chunk_schema||'.'||c.chunk_name) AS pgrs_vm, "
                            + "  pg_table_size(c.chunk_schema||'.'||c.chunk_name) AS pgrs_table, "
                            + "  pg_indexes_size(c.chunk_schema||'.'||c.chunk_name) AS pgrs_indexes "
                            + "FROM timescaledb_information.chunks AS c, "
                            + "  chunk_compression_stats('%1$s.%2$s') AS ccs,"
                            + "  pg_class AS pgc, "
                            + "  pg_namespace AS pgns "
                            + "WHERE c.chunk_schema = ccs.chunk_schema "
                            + "  AND c.chunk_name = ccs.chunk_name "
                            + "  AND c.hypertable_schema = '%1$s' AND c.hypertable_name = '%2$s' "
                            + "  AND pgc.relname = c.chunk_name "
                            + "  AND pgc.relnamespace = pgns.oid "
                            + "  AND pgns.nspname = c.chunk_schema ",
                    schema.getName(), tableName);
            return dsl.resultQuery(sql).fetch().intoMaps().stream()
                    .map(rec -> {
                        final boolean isCompressed = (boolean)rec.get("is_compressed");
                        // extract current size based on compression state
                        long dataSize = isCompressed ? (long)rec.get("data")
                                : (long)rec.get("pgrs_main") + (long)rec.get("pgrs_fsm")
                                + (long)rec.get("pgrs_vm");
                        long indexSize = isCompressed ? (long)rec.get("index")
                                : (long)rec.get("pgrs_indexes");
                        long toastSize = isCompressed ? (long)rec.get("toast")
                                : (long)rec.get("pgrs_table") - dataSize;
                        return new SizeInfo(tableName, getChunkName(rec),
                                dataSize, (Long)rec.get("compressed_data"),
                                indexSize, (Long)rec.get("compressed_index"),
                                toastSize, (Long)rec.get("compressed_toast"),
                                Math.round((Float)rec.get("rows")));
                    })
                    .collect(Collectors.toList());
        }

        private List<SizeInfo> getHypertableInfoWithoutCompression() {
            String sql = String.format("SELECT " +
                    // chunk identity and compression state
                    "  c.chunk_name, c.range_start, c.range_end, " +
                    // position of this chunk in the reverse-chronological sequence
                    "  row_number() OVER (ORDER BY range_start DESC) AS number, " +
                    // size breakdown of each chunk
                    "  cds.table_bytes, cds.index_bytes, cds.toast_bytes, cds.total_bytes," +
                    // number of rows in chunk
                    "  pgc.reltuples AS rows " +
                    "FROM timescaledb_information.chunks AS c, " +
                    "  pg_class AS pgc, " +
                    "  pg_namespace AS pgns," +
                    "  chunks_detailed_size('%1$s.%2$s') AS cds " +
                    "WHERE c.hypertable_schema = '%1$s' AND c.hypertable_name = '%2$s' " +
                    "  AND pgc.relname = c.chunk_name " +
                    "  AND pgc.relnamespace = pgns.oid" +
                    "  AND pgns.nspname = c.chunk_schema" +
                    "  AND cds.chunk_name = c.chunk_name", schema.getName(), tableName);
            return dsl.resultQuery(sql).fetch().intoMaps().stream()
                    .map(rec -> new SizeInfo(tableName, getChunkName(rec),
                            (long)rec.get("table_bytes"), (long)rec.get("index_bytes"),
                            (long)rec.get("toast_bytes"), Math.round((Float)rec.get("rows"))))
                    .collect(Collectors.toList());
        }

        /**
         * Obtain size info for a normal (non-hyper-)table.
         *
         * @param table table to interrogate
         * @return size info
         */
        private List<SizeInfo> populateNormalTableData(Table<?> table) {
            String sql = String.format("SELECT pg_relation_size('%2$s.%1$s', 'main') AS main, "
                            + "  pg_relation_size('%2$s.%1$s', 'fsm') AS fsm, "
                            + "  pg_relation_size('%2$s.%1$s', 'vm') AS vm, "
                            + "  pg_table_size('%2$s.%1$s') AS table, "
                            + "  pg_indexes_size('%2$s.%1$s') AS indexes, "
                            + "  pgc.reltuples AS rows "
                            + "FROM pg_class AS pgc "
                            + "JOIN pg_namespace pgn ON (pgc.relnamespace = pgn.oid) "
                            + "WHERE pgc.relname = '%1$s' AND pgn.nspname = '%2$s'",
                    table.getName(), schema.getName());
            final Map<String, Object> row = dsl.resultQuery(sql).fetchOne().intoMap();
            // three components of table data
            long main = (long)row.get("main");
            long freeSpaceMap = (long)row.get("fsm");
            long visibilityMap = (long)row.get("vm");
            long dataSize = main + freeSpaceMap + visibilityMap;
            // index size
            long indexSize = (long)row.get("indexes");
            // table data + toast
            long tableSizeWithToastNoIndexes = (long)row.get("table");
            // just the toast
            long toastSize = tableSizeWithToastNoIndexes - dataSize;
            long rows = Math.round((float)row.get("rows"));
            return Collections.singletonList(new SizeInfo(table.getName(), dataSize, indexSize, toastSize, rows));
        }

        /**
         * Construct a list of {@link SizeItem} values for this table.
         *
         * @return size items
         */
        public List<SizeItem> getSizeItems() {
            List<SizeItem> items = new ArrayList<>();
            if (isHypertable) {
                // create a table-level item summing the per-chunk values
                final long totalSize = sizeInfos.stream().mapToLong(SizeInfo::size).sum();
                items.add(new SizeItem(TABLE, totalSize, "Hypertable " + tableName));
            }
            // follow with per-chunk items (or just add the single table item for a non-hypertable)
            sizeInfos.stream().map(SizeInfo::toSizeItem).forEach(items::add);
            return items;
        }

        /**
         * Identify a chunk with its position in chunk list, its internal name, and its time range.
         *
         * @param rec record data from chunk query
         * @return formatted chunk name
         */
        private String getChunkName(Map<String, Object> rec) {
            return String.format("%d:%s[%s-%s]",
                    rec.get("number"), rec.get("chunk_name"),
                    rec.get("range_start"), rec.get("range_end"));
        }
    }

    /**
     * Size information for a chunk of a hypertable, or for the whole of a normal table.
     */
    private static class SizeInfo {
        private final String tableName;
        private final String chunkName;
        private final long dataSize;
        private final Long compressedDataSize;
        private final long indexSize;
        private final Long compressedIndexSize;
        private final long toastSize;
        private final Long compressedToastSize;
        private final long rows;

        /**
         * Create a new instance for a normal table.
         *
         * @param tableName name of table
         * @param dataSize  size of table data
         * @param indexSize size of indexes
         * @param toastSize size of toast data
         * @param rows      row count
         */
        SizeInfo(String tableName, long dataSize, long indexSize, long toastSize, long rows) {
            this(tableName, null, dataSize, indexSize, toastSize, rows);
        }

        SizeInfo(String tableName, String chunkname, long dataSize, long indexSize, long toastSize, long rows) {
            this(tableName, chunkname, dataSize, null, indexSize, null, toastSize, null, rows);
        }

        /**
         * Create a new entry for a hypertable chunk or a non-hypertable.
         *
         * @param tableName           name of table
         * @param chunkname           name of chunk (null for normal table)
         * @param dataSize            table data size
         * @param compressedDataSize  data size after compression (null for normal table)
         * @param indexSize           index size
         * @param compressedIndexSize index size after compression (null for normal table)
         * @param toastSize           toast size
         * @param compressedToastSize toast size after compression (null for normal table)
         * @param rows                row count
         */
        SizeInfo(String tableName, String chunkname,
                long dataSize, Long compressedDataSize,
                long indexSize, Long compressedIndexSize,
                long toastSize, Long compressedToastSize,
                long rows) {
            this.tableName = tableName;
            this.chunkName = chunkname;
            this.dataSize = dataSize;
            this.compressedDataSize = compressedDataSize;
            this.indexSize = indexSize;
            this.compressedIndexSize = compressedIndexSize;
            this.toastSize = toastSize;
            this.compressedToastSize = compressedToastSize;
            this.rows = rows;
        }

        /**
         * Create a {@link SizeItem} for this chunk/table.
         *
         * @return size item
         */
        public SizeItem toSizeItem() {
            // if we're a chunk we're at level 1, else we're a top-level item
            final Granularity granularity = chunkName != null ? PARTITION : TABLE;
            // Sometimes total description length is closer to 200 (max DB field size), so as a
            // workaround replace '15:_hyper_8_104_chunk[2021-07-02...' in chunk name with:
            // '15:8_104[2021-07-02...'
            final String shortChunkName = chunkName != null
                    ? chunkName.replace("_hyper_", "").replace("_chunk", "")
                    : null;
            final String desc = (shortChunkName != null ? shortChunkName : "Table " + tableName) + ": " + this;
            return new SizeItem(granularity, size(), desc);
        }

        /**
         * Get size to report for this chunk/table - compressed total if it's a compressed chunk,
         * else uncompressed total.
         *
         * @return current size
         */
        private long size() {
            return compressedDataSize != null
                    ? compressedDataSize + compressedIndexSize + compressedToastSize
                    : dataSize + indexSize + toastSize;
        }

        /**
         * We use this as the bulk of a size-item description for this chunk/table.
         *
         * @return description
         */
        @Override
        public String toString() {
            final long totalSize = dataSize + indexSize + toastSize;
            final Long compressedTotalSize = compressedDataSize != null
                    ? compressedDataSize + compressedIndexSize + compressedToastSize
                    : null;
            // D: data, I: index, T: toast, S: sum total, R: rows.
            return String.format("(D: %d%s, I: %d%s, T: %d%s, S: %d%s, R: %d)",
                    dataSize, compression(dataSize, compressedDataSize),
                    indexSize, compression(indexSize, compressedIndexSize),
                    toastSize, compression(toastSize, compressedToastSize),
                    totalSize, compression(totalSize, compressedTotalSize),
                    rows);
        }

        /**
         * Format the compression data for the given size value.
         *
         * <p>If the compressed size is null, there's no compression data to display,
         * else we show compressed size and compression factor (negative factor means size grew,
         * which is common for toast).</p>
         *
         * @param size           uncompressed size
         * @param compressedSize compressed size, or null if not compressed
         * @return display string for compression result
         */
        private String compression(long size, Long compressedSize) {
            if (compressedSize == null) {
                return "";
            } else {
                double ratio = size > compressedSize ? size * 1.0 / compressedSize
                        : -compressedSize * 1.0 / size;
                return String.format("=>%d(%.1fx)", compressedSize, ratio);
            }
        }
    }
}
