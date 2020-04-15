package com.vmturbo.history.db.bulk;

import static com.vmturbo.history.db.bulk.DbInserters.simpleUpserter;
import static com.vmturbo.history.db.bulk.DbInserters.valuesInserter;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.ENTITIES;
import static com.vmturbo.history.schema.abstraction.Tables.HIST_UTILIZATION;
import static com.vmturbo.history.schema.abstraction.tables.VmStatsLatest.VM_STATS_LATEST;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Table;

import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserterFactory.TableOperation;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;

/**
 * This class uses {@link BulkInserterFactory} to create bulk loaders that are automatically
 * configured for the needs of specific tables.
 *
 * <p>Currently, the following tables get special handling:</p>
 * <dl>
 * <dt>*_stats_latest</dt>
 * <dd>
 * A transformer is configured that computes rollup keys.
 * </dd>
 * <dt>entities</dt>
 * <dd>
 * Uses an inserter based on the Jooq batchStore method, which performs a mix of inserts
 * and updates based on the state of the records.
 * </dd>
 * </dl>
 *
 * <p>For all other tables, no transformer is applied, and the values inserter is used.</p>
 */
public class SimpleBulkLoaderFactory implements AutoCloseable {

    // these column names are common to all the stats_latest tables
    private static final String HOUR_KEY_FIELD_NAME = VM_STATS_LATEST.HOUR_KEY.getName();
    private static final String DAY_KEY_FIELD_NAME = VM_STATS_LATEST.DAY_KEY.getName();
    private static final String MONTH_KEY_FIELD_NAME = VM_STATS_LATEST.MONTH_KEY.getName();

    // cluster stats tables
    private static final Set<Table<?>> CLUSTER_STATS_TABLES = ImmutableSet.of(
            CLUSTER_STATS_LATEST,
            CLUSTER_STATS_BY_HOUR,
            CLUSTER_STATS_BY_DAY,
            CLUSTER_STATS_BY_MONTH);

    // we delegate to this factory for all the writers we create
    private final BulkInserterFactory factory;

    private final BasedbIO basedbIO;

    /**
     * Create a new instance.
     *
     * @param basedbIO      base db utilities
     * @param defaultConfig config to be used by default when creating inserters
     * @param executor      executor service to manage concurrent statement executions
     */
    public SimpleBulkLoaderFactory(final @Nonnull BasedbIO basedbIO,
                                   final @Nonnull BulkInserterConfig defaultConfig,
                                   final @Nonnull ExecutorService executor) {
        this(basedbIO, new BulkInserterFactory(basedbIO, defaultConfig, executor));
    }

    /** Create a new instance, supplying a BulkInserterFactory instance.
     * @param basedbIO      base db utilities
     * @param factory       underlying BulkInserterFactory instance
     */
    public SimpleBulkLoaderFactory(final @Nonnull BasedbIO basedbIO,
                                   final @Nonnull BulkInserterFactory factory) {
        this.basedbIO = basedbIO;
        this.factory = factory;
    }

    /**
     * Get access to the underlying {@link BulkInserterFactory} used by this instance.
     *
     * <p>Clients can use this to create bulk loaders that are configured differently than what
     * this class would create. But keep in mind that only one bulk loader for a given table
     * can be created by any factory, so if a bulk loader has already been created for a given
     * table, this factory will only ever return that loader for that table, regardless of
     * specified configuration options.
     *
     * @return the bulk loader factory underlying this factory
     */
    public BulkInserterFactory getFactory() {
        return factory;
    }


    /**
     * Get a loader for records of the given table, configured appropriately.
     *
     * @param table the table into which records will be inserted
     * @param <R>   the underlying record type
     * @return an appropriately configured writer
     */
    public <R extends Record> BulkLoader<R> getLoader(final @Nonnull Table<R> table) {
        return factory.getInserter(table, table, getRecordTransformer(table), getDbInserter(table));
    }

    /**
     * Get a "transient" loader for the given table.
     *
     * <p>A transient loader is a loader that loads records not into the given table, but into a
     * copy of that table created specifically for this request, and with the same record structure
     * as the given table. This is similar in concept to the "temporary table" facilities ofr MySQL,
     * but the tables are not bound to the connection from which they were created, which makes
     * them far more suitable for use with bulk loaders.</p>
     *
     * <p>The jOOQ {@link Table} object returned can in many cases be used where a primary table
     * object can be used, and will access the transient table. But there are some cases where this
     * does not hold. In particular, when using a transient table in as a FROM table in a SELECT
     * statement, you should use <code>DSL.table(transientTable.getName())</code>.</p>
     *
     * <p>If indexes need to be created on the transient table, or any other post-processing is
     * needed, that can be accomplished by providing a <code>postCreateTableOp</code> function.</p>
     *
     * <p>Unlike non-transient tables, the factory does not give back shared instances to multiple
     * requests for the same table. This method will ALWAYS create a new transient table and return
     * a loader bound to that table.</p>
     *
     * <p>When the loader is closed, the transient table is dropped automatically.</p>
     *
     * @param table             the table whose structure will be copied when creating the transient
     * @param dropExisting      whether to drop existing transient tables for this table first
     *                          (i.e. clean up tables orphaned by a crash)
     * @param postTableCreateOp a function to perform operations on the table after it has been created
     * @param <R>               record type of underlying and transient tables
     * @return the transient table, as a jOOQ {@link Table} instance
     * @throws SQLException if there's a database error creating the table
     * @throws InstantiationException if we can't create the jOOQ table object
     * @throws VmtDbException if there's a problem with DB connection
     * @throws IllegalAccessException if we can't create the jOOQ tabel object
     */
    public <R extends Record> BulkLoader<R> getTransientLoader(
            final @Nonnull Table<R> table, boolean dropExisting, TableOperation<R> postTableCreateOp)
            throws SQLException, InstantiationException, VmtDbException, IllegalAccessException {
        return factory.getTransientInserter(
                table, table, dropExisting, getRecordTransformer(table), getDbInserter(table),
                postTableCreateOp);
    }

    private <R extends Record> RecordTransformer<R, R> getRecordTransformer(Table<R> table) {
        final Optional<EntityType> entityType = EntityType.fromTable(table);
        if (entityType.map(EntityType::rollsUp).orElse(false)) {
            return new RollupKeyTransfomer<>();
        } else {
            return RecordTransformer.identity();
        }
    }

    private <R extends Record> DbInserter<R> getDbInserter(Table<R> table) {
        if (ENTITIES == table || HIST_UTILIZATION == table) {
            // Entities table uses upserts so that previously existing entities get any changes
            // to display name that show up in the topology.
            return simpleUpserter(basedbIO);
        } else {
            // nothing else currently using bulk loader should ever have a primary key collision,
            // so straight inserts are used.
            return valuesInserter(basedbIO);
        }
    }

    /**
     * Get the stats object for the underlying inserter factory.
     *
     * @return inserter factory's stats object
     */
    public BulkInserterFactoryStats getStats() {
        return factory.getStats();
    }

    /**
     * Flush all inserters created by the underlying {@link BulkInserterFactory}.
     *
     * @throws InterruptedException if interrupted
     */
    public void flushAll() throws InterruptedException {
        factory.flushAll();
    }

    /**
     * Close the underlying {@link BulkInserterFactory}.
     *
     * @throws InterruptedException if interrupted
     */
    public void close() throws InterruptedException {
        factory.close();
    }

    /**
     * Close the underlying {@link BulkInserterFactory} with stats logged to the given logger.
     *
     * @param logger where to log stats
     * @throws InterruptedException if interrupted
     */
    public void close(final Logger logger) throws InterruptedException {
        factory.close(logger);
    }

    /** RecordTrnasformer that adds rollup keys to a stats record.
     * @param <R> Type of stats record
     */
    static class RollupKeyTransfomer<R extends Record> implements RecordTransformer<R, R> {
        @Override
        public Optional<R> transform(final R record, final Table<R> inTable, final Table<R> outTable) {
            Map<String, Object> rollupKeyMap = ImmutableMap.<String, Object>builder()
                .put(HOUR_KEY_FIELD_NAME, RollupKey.getHourKey(inTable, record))
                .put(DAY_KEY_FIELD_NAME, RollupKey.getDayKey(inTable, record))
                .put(MONTH_KEY_FIELD_NAME, RollupKey.getMonthKey(inTable, record))
                .build();

            record.fromMap(rollupKeyMap);
            return Optional.of(record);
        }
    }
}
