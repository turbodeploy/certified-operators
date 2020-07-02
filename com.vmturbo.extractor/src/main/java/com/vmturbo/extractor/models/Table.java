package com.vmturbo.extractor.models;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import net.jpountz.xxhash.StreamingXXHash64;

import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * This class represents tables populated during the ingestion of topologies.
 *
 * <p>We do not use jOOQ-generated classes because we found through testing that using the jOOQ
 * record representation led to significant performance reduction. This may be partly due to the use
 * of Postgres's COPY statement to stream the records in CSV form to the database.</p>
 *
 * <p>Individual table objects include metadata about the database tables and their columns,
 * and are constructed outside this class, using an inner builder class.</p>
 *
 * <p>A table object also serves as an entry point for record insertion: first, a {@link
 * DslRecordSink} is "attached" to the table, yielding a {@link TableWriter}. Then that writer is
 * used to open {@link Record} objects that are populated with data. Closing a record object causes
 * it to be sent to the record sink, which will take care of sending it to the database in the
 * proper form.</p>
 */
public class Table {

    private final String name;
    private final LinkedHashMap<String, Column<?>> columns = new LinkedHashMap<>();

    /**
     * Private constructor.
     *
     * <p>Clients of this class should use the {@link #named(String)} method to create new
     * instance builders.</p>
     *
     * @param name    name of table
     * @param columns columns in table, in order added to builder
     */
    private Table(String name, LinkedHashMap<String, Column<?>> columns) {
        this.name = name;
        this.columns.putAll(columns);
    }

    /**
     * Get the named column object from this table.
     *
     * @param name column name
     * @return column, or null if no such column appears in this table
     */
    public Column<?> getColumn(String name) {
        return columns.get(name);
    }

    /**
     * Create a builder for table with the given name.
     *
     * @param name table name
     * @return new table builder
     */
    public static Builder named(String name) {
        return new Builder(name);
    }

    /**
     * Attach a record sink to this table.
     *
     * <p>This begins the operation of inserting a stream of records into the database.</p>
     *
     * @param sink the record sink
     * @return TableWriter with the attached sink
     */
    public TableWriter open(Consumer<Record> sink) {
        return new TableWriter(sink);
    }

    /**
     * Get this table's name.
     *
     * @return the table name
     */
    public String getName() {
        return name;
    }

    /**
     * Get this table's columns, in the order they were added ot the builder.
     *
     * @return the table columns
     */
    public Collection<Column<?>> getColumns() {
        return Collections.unmodifiableCollection(columns.values());
    }

    /**
     * Builder class for constructing a table.
     */
    public static class Builder {

        private final String name;
        private final LinkedHashMap<String, Column<?>> columns = new LinkedHashMap<>();

        /**
         * Create a new builder instance, for a table with a given name.
         *
         * @param name name of the table being built
         */
        private Builder(String name) {
            this.name = name;
        }

        /**
         * Add one or more columns to the table under construction.
         *
         * @param columns columns to be added
         * @return this builder
         */
        public Builder withColumns(Column<?>... columns) {
            Arrays.stream(columns).forEach(c -> this.columns.put(c.getName(), c));
            return this;
        }

        /**
         * Build the table.
         *
         * @return the newly built table
         */
        public Table build() {
            return new Table(name, columns);
        }
    }

    private static final DataMetricGauge RECORDS_WRITTEN_GAUGE = DataMetricGauge.builder()
            .withName("xtr_table_write_gauge")
            .withHelp("Gauge of the number of records written to tables during each topology processing cycle. Labelled by name.")
            .withLabelNames("name")
            .build()
            .register();

    /**
     * Class to manage a record sink attached to a table.
     */
    public class TableWriter implements AutoCloseable {

        private final Consumer<Record> sink;
        private boolean closed = false;
        private long recordsWritten = 0;

        /**
         * Create a new instance.
         *
         * @param sink sink to receive records for the table
         */
        public TableWriter(Consumer<Record> sink) {
            this.sink = sink;
        }

        /**
         * Create a new record object, which will be sent to the attached sink when the record is
         * closed.
         *
         * <p>The record is auto-closeable, so opening in a try-with-resource statement is a
         * recommended pattern.</p>
         *
         * @return the new record object
         */
        public Record open() {
            return open(null);
        }

        /**
         * "Open" a previously opened record that was not actually closed.
         *
         * <p>This is not really necessary - the not-yet-closed record can be updated and then
         * closed by the calling code. But using this method as the expression in a
         * try-with-resources statement makes the intention very clear, and allows the t-w-r pattern
         * to be sustained.</p>
         *
         * <p>This can be used when the process of fully populating a record occurs in phases. The
         * caller must arrange to retain partial records for later completion.</p>
         *
         * @param partial record to be "re-opened"
         * @return the record
         */
        public Record open(Record partial) {
            if (!closed) {
                return new Record(this, this::accept, partial);
            } else {
                throw new IllegalStateException(
                        String.format("TableWriter for table %s is closed", name));
            }
        }

        /**
         * Insert a record into current table using attached sink.
         *
         * @param full record with full data
         */
        public void accept(Record full) {
            if (!closed) {
                sink.accept(full);
                recordsWritten++;
            } else {
                throw new IllegalStateException(
                        String.format("TableWriter for table %s is closed", name));
            }
        }

        /**
         * Close this writer and tie off its attached sink.
         *
         * <p>This completes the operation of sending a stream of records to the database.</p>
         */
        @Override
        public void close() {
            sink.accept(null);
            this.closed = true;
            RECORDS_WRITTEN_GAUGE.labels(getName()).setData((double)recordsWritten);
        }

        /**
         * Get a column from the underlying table.
         *
         * @param columnName column name
         * @return column
         */
        public Column<?> getColumn(final String columnName) {
            return Table.this.getColumn(columnName);
        }

        /**
         * Get the name of the underlying table.
         *
         * @return the table name
         */
        public String getName() {
            return Table.this.getName();
        }

        /**
         * Get the list of columns in the underlying table.
         *
         * @return the table columns
         */
        public Collection<Column<?>> getColumns() {
            return Table.this.getColumns();
        }

        public boolean isClosed() {
            return closed;
        }
    }

    /**
     * Class to represent a record to be sent to the database.
     *
     * <p>A newly created record is sent to the database when it is closed, and since this
     * class implements {@link AutoCloseable}, that can be conveniently done in a try-with-resources
     * statement.</p>
     *
     * <p>The associated table needs to have an attached record sink at the time the record is
     * closed.</p>
     */
    public static class Record implements AutoCloseable {

        private final Consumer<Record> sink;
        private final TableWriter tableWriter;
        private final Map<Column<?>, Object> values;

        private Record(TableWriter tableWriter, Consumer<Record> sink, Record partial) {
            this.tableWriter = tableWriter;
            this.sink = sink;
            this.values = partial != null ? partial.values : new HashMap<>();
        }

        /**
         * Create a new record associated with a given table.
         *
         * <p>This method may be used with a table that does not currently have any attached
         * sinks, but it must be reconstituted at the later time, generally by opening it
         * as a partial record with the {@link TableWriter#open(Record)} method.</p>
         *
         * @param table the table
         */
        public Record(Table table) {
            this(table.open(null), null, null);
        }

        /**
         * Set the value of the given column to the given value.
         *
         * @param column {@link Column} to be set
         * @param value  value to set in the column
         * @param <T>    column type
         */
        public <T> void set(Column<T> column, T value) {
            values.put(column, value);
        }

        /**
         * Set a value for the named column.
         *
         * @param columnName column name
         * @param value      value to set
         */
        public void set(String columnName, Object value) {
            Column<?> column = tableWriter.getColumn(columnName);
            if (column == null) {
                throw new IllegalArgumentException(String.format("Column '%s' not found in table '%s'",
                        columnName, tableWriter.getName()));
            } else {
                values.put(column, value);
            }
        }

        /**
         * Set this column if a gating condition is satisfied.
         *
         * <p>The value is supplied in the form of a {@link Supplier} so that if the gate is
         * false, the cost of creating the value can be avoided.</p>
         *
         * @param gate   gating condition
         * @param column column to be set
         * @param value  supplier of value to be set
         * @param <T>    type of column
         */
        public <T> void setIf(boolean gate, Column<T> column, Supplier<T> value) {
            if (gate) {
                values.put(column, value.get());
            }
        }

        /**
         * Merge the given value into the given record column.
         *
         * @param column column to be updated
         * @param value  value to merge into current value
         * @param merger function to merge the existing value (if present) with the new value
         * @param <T>    column type
         */
        public <T> void merge(Column<T> column, T value, BiFunction<Object, Object, T> merger) {
            values.merge(column, value, merger);
        }

        /**
         * Merge the given value into the given table column if a gating condition is satisfied.
         *
         * <p>The value is supplied in the form of a {@link Supplier} so that if the gate is
         * false, the cost of creating the value can be avoided.</p>
         *
         * @param gate   the value of the gating condition
         * @param column column to be updated
         * @param value  supplier of value to be merged
         * @param merger function to merge existing value (if present) with new value
         * @param <T>    type of column
         */
        public <T> void mergeIf(boolean gate, Column<T> column,
                Supplier<T> value, BiFunction<Object, Object, T> merger) {
            if (gate) {
                values.merge(column, value.get(), merger);
            }
        }

        /**
         * Get the value of the given column.
         *
         * @param column column
         * @param <T>    column type
         * @return column value
         */
        public <T> T get(Column<T> column) {
            return (T)values.get(column);
        }

        /**
         * Compute a hash value for this record, using some of its column values.
         *
         * @param includedColumns names of columns to include in the hash calculation
         * @return hash value
         */
        public long getXxHash(Set<String> includedColumns) {
            final StreamingXXHash64 hash64 = HashUtil.XX_HASH_FACTORY.newStreamingHash64(HashUtil.XX_HASH_SEED);
            tableWriter.getColumns().stream()
                    .filter(c -> includedColumns.contains(c.getName()))
                    .map(c -> c.toHashValue(values.get(c)))
                    // empty byte arrays cause problems in at least the unsafe java xxhash impl
                    .filter(ba -> ba.length > 0)
                    .forEach(ba -> hash64.update(ba, 0, ba.length));
            final long hash = hash64.getValue();
            // We use hash values as keys in fastutil maps and sets, so we must exclude using the
            // "no entry here" value used in those structures
            return hash != 0L ? hash : 1L;
        }

        /**
         * Convert a record into a CSV-formatted row, ready to be sent the record sink.
         *
         * @param recordColumns columns to be included in CSV row
         * @return CSV-formatted data row
         */
        public String toCSVRow(final Collection<Column<?>> recordColumns) {
            return recordColumns.stream()
                    .map(c -> c.toCsvValue(values.get(c)))
                    .collect(Collectors.joining(","));
        }

        @Override
        public void close() {
            sink.accept(this);
        }
    }
}
