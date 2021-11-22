package com.vmturbo.extractor.models;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.conf.ParamType;
import org.jooq.impl.DSL;

import com.vmturbo.components.common.utils.ThrowingConsumer;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Class to populate and update a per-cycle table with writes optimized by use of record hashes.
 *
 * <p>Records intended for the current topology are passed to the data manager, which computes
 * a hash value for each. When a record's hash does not correspond to a record that was produced in
 * the prior topology, that record participates in an upsert operation against the target table, so
 * that new or changed records will be inserted/updated in that table. After all records for the
 * current topology have been processed, records corresponding to hash values that were not produced
 * in the current topology are removed from the target table, as part of the same transaction.</p>
 *
 * <p>Upon initial startup, the "prior" hash table is reconstructed by reading current hashes from
 * the target table. Thereafter, the current hashes are retained to serve as the prior hashes for
 * next topology.</p>
 */
public class HashedDataManager {
    private static final Logger logger = LogManager.getLogger();

    private final ExecutorService pool;
    private final Table table;
    private final Column<Long> hashColumn;
    private final List<Column<?>> primaryKey;
    private final Set<Column<?>> hashCols;
    private final List<Column<?>> updateCols;
    private final org.jooq.Table<org.jooq.Record> jooqTable;
    private final Field<Long> jooqHashField;

    LongSet priorHashes = null;
    LongSet currentHashes = new LongOpenHashSet();
    private DslUpsertRecordSink sink;
    private TableWriter writer;

    /**
     * Create a new instance, to be used to process all following topologies until the component
     * exits.
     *
     * @param table      target table
     * @param hashColumn column in table that stores record hashes
     * @param primaryKey columns comprising the table's primary key
     * @param pool       thread pool
     */
    public HashedDataManager(Table table, Column<Long> hashColumn, Set<Column<?>> primaryKey,
            ExecutorService pool) {
        this.table = table;
        this.hashColumn = hashColumn;
        this.primaryKey = new ArrayList<>(primaryKey);
        this.hashCols = Sets.difference(new HashSet<>(table.getColumns()), Collections.singleton(hashColumn));
        this.updateCols = new ArrayList<>(table.getColumns());
        this.updateCols.removeAll(primaryKey);
        this.pool = pool;
        this.jooqTable = DSL.table(table.getName());
        this.jooqHashField = DSL.field(hashColumn.getName(), Long.class);
    }

    /**
     * Open the data manager to receive records for a new topology.
     *
     * @param dsl    {@link DSLContext} for database access
     * @param config writer config data
     * @return a record consumer that is also auto-closeable
     */
    public CloseableConsumer<Record, SQLException> open(DSLContext dsl, WriterConfig config) {
        if (priorHashes == null) {
            priorHashes = loadHashesFromDatabase(dsl);
        }
        this.sink = getUpserter(dsl, config);
        this.writer = table.open(sink, String.format("%s Upserter", table.getName()), logger);
        return new CloseableConsumer<>(new ThrowingConsumer<Record, SQLException>() {
            @Override
            public void accept(Record record) throws SQLException, InterruptedException {
                processRecord(record);
            }
        }, this::finish);
    }

    private DslUpsertRecordSink getUpserter(DSLContext dsl, WriterConfig config) {
        return new DslUpsertRecordSink(dsl, table, config, pool, "_upsert", primaryKey, updateCols) {
            @Override
            protected List<String> getPostCopyHookSql(final Connection transConn) throws SQLException {
                final String deleteSql = dsl.delete(jooqTable)
                        .where(jooqHashField.in(priorHashes))
                        .getSQL(ParamType.INLINED);
                return ImmutableList.<String>builder()
                        .addAll(super.getPostCopyHookSql(transConn))
                        .add(deleteSql)
                        .build();
            }
        };
    }

    private void processRecord(final Record record) throws SQLException, InterruptedException {
        long hash = record.getXxHash(hashCols);
        currentHashes.add(hash);
        if (priorHashes.contains(hash)) {
            // current record is unchanged from prior cycle... remove it from prior hashes
            priorHashes.remove(hash);
        } else {
            // else it's a new or changed record so it's part of the upsert
            record.set(hashColumn, hash);
            writer.accept(record);
        }
    }

    private long finish() {
        try {
            writer.close();
            final long recordsWritten = writer.getRecordsWritten();
            return recordsWritten;
        } finally {
            this.priorHashes = currentHashes;
            this.currentHashes = new LongOpenHashSet();
            this.sink = null;
            this.writer = null;
        }
    }

    private LongSet loadHashesFromDatabase(DSLContext dsl) {
        return dsl.select(jooqHashField)
                .from(jooqTable)
                .fetch()
                .stream()
                .map(org.jooq.Record1::value1)
                .filter(Objects::nonNull)
                .mapToLong(Long::longValue)
                .collect(LongOpenHashSet::new, LongSet::add, LongSet::addAll);
    }


    /**
     * Class that implements both {@link Consumer} and {@link AutoCloseable}. After its' closed,
     * attempts to pass additional values to {@link #accept(Object)} throw an exception.
     *
     * @param <T> type of consumed object
     * @param <E> type of exception thrown
     */
    public static class CloseableConsumer<T, E extends Exception> implements ThrowingConsumer<T, E>, AutoCloseable {

        private boolean isOpen;
        private final ThrowingConsumer<T, E> onAccept;
        private final Runnable onClose;

        CloseableConsumer(ThrowingConsumer<T, E> onAccept, Runnable onClose) {
            this.onAccept = onAccept;
            this.onClose = onClose;
            this.isOpen = true;
        }

        @Override
        public void accept(final T t) throws E, InterruptedException {
            if (isOpen) {
                onAccept.accept(t);
            } else {
                throw new IllegalStateException("Consumer is closed and cannot accept more items");
            }
        }

        @Override
        public void close() {
            try {
                if (isOpen) {
                    onClose.run();
                } else {
                    throw new IllegalStateException("Already closed");
                }
            } finally {
                isOpen = false;
            }
        }
    }
}
