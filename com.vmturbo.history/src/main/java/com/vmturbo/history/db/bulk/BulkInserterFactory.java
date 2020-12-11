package com.vmturbo.history.db.bulk;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.RecordTransformer;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.db.bulk.BulkInserter.BatchStats;
import com.vmturbo.history.db.bulk.DbInserters.DbInserter;

/**
 * This class hands out {@link BulkInserter} instances on request.
 *
 * <p>When multiple requests are made for an inserter based on the same {@link Table}, the same
 * inserter will be provided to each, but a different instance of this class will hand out different
 * inserters. This makes it easy for an operation that's spread over multiple classes and methods
 * to use a common set of insetters, just by passing the {@link BulkInserterFactory} instance
 * around.</p>
 *
 * <p>The previous paragraph isn't quite true. If you need a single factory to create multiple
 * inserters for the same table, you can do so by supplying an alternate key object. Normally, the
 * input table object is used for this purpose, so normally the first paragraph is true.</p>
 *
 * <p>When an inserter factory object is closed, it closes all the inserter objects it has handed
 * out, which causes all of them to flush any pending operations. This class implements
 * {@link AutoCloseable}, so one can conveniently allocate one in a try-resource block and know
 * that all the inserters that are created during its lifetime will be duly terminated when the
 * block exits.</p>
 *
 * <p>All the inserters are associated with a key object, which is normally the input records
 * table but in fact can be any object. If an inserter is requested with a key for which an
 * inserter already exists, that existing inserter will be returned rather than creating a new
 * one. Any configuration information provided in any such subsequent request will be silently
 * ignored.</p>
 *
 * <p>All the inserters created by an inserter factory perform their database operations using a
 * shared {@link ExecutorService}, which is passed as a constructor argument. This permits a
 * fairly safe, if not terribly aggressive, means of achieving parallel exeuction of database
 * operations. Each participating {@link BulkInserter} is capped in terms of the number of batches
 * it may submit for execution before prior batches have completed and resolved. When the cap
 * is reached, further activity by that inserter will pause awaiting the completion of prior
 * batches.</p>
 *
 * <p>This class is thread-safe, in this sense: The first client to request an inserter for a
 * given table gets a made-to-order inserter for that table. Subsequent requests for that same
 * table are satisfied with a reference to that same inserter, even if such a request is
 * differently configured.
 * </p>
 */
public class BulkInserterFactory implements AutoCloseable {

    /** part of the unique suffix appended to a table name to create a transient table. */
    public static final String TRANSIENT_TABLE_SUFFIX = "__transient__";

    private static Logger logger = LogManager.getLogger();

    // default configuraiton options for new inserters
    private final BulkInserterConfig defaultConfig;

    // all the inserter objects we've allocated
    private final Map<Object, BulkInserter<?, ?>> inserters = new ConcurrentHashMap<>();

    private final BasedbIO basedbIO;
    private final ThrottlingCompletingExecutor<BatchStats> batchCompleter;

    /**
     * Create a new instance.
     *
     * @param basedbIO basic database helpers
     * @param config   default configuration for inserters
     * @param executor threadpool for execution of loader batches
     */
    public BulkInserterFactory(@Nonnull BasedbIO basedbIO,
                               @Nonnull BulkInserterConfig config,
                               @Nonnull ExecutorService executor) {
        this.basedbIO = basedbIO;
        this.defaultConfig = config;
        this.batchCompleter = new ThrottlingCompletingExecutor<>(executor, config.maxPendingBatches());
    }

    /**
     * Create, if needed, a new {@link BulkInserter} instance keyed by input table.
     *
     * <p>If an inserter has previously been created by this factory for the given input table,
     * that inserter is returned instead.</p>
     *
     * @param inTable           table for records that will be fed to this inserter
     * @param outTable          table for records that will be inserted into the database
     * @param recordTransformer function to transform input records to database records
     * @param dbInserter        {@link DbInserter} responsible for saving database records
     * @param <InT>             input record type
     * @param <OutT>            output record type
     * @return the existing or newly created inserter
     */
    public <InT extends Record, OutT extends Record> BulkInserter<InT, OutT> getInserter(
            @Nonnull Table<InT> inTable,
            @Nonnull Table<OutT> outTable,
            @Nonnull RecordTransformer<InT, OutT> recordTransformer,
            @Nonnull DbInserter<OutT> dbInserter) {

        return getInserter(inTable, inTable, outTable, recordTransformer, dbInserter, Optional.empty());
    }

    <InT extends Record, OutT extends Record> BulkInserter<InT, OutT> getInserter(
        @Nonnull Object key,
        @Nonnull Table<InT> inTable,
        @Nonnull Table<OutT> outTable,
        @Nonnull RecordTransformer<InT, OutT> recordTransformer,
        @Nonnull DbInserter<OutT> dbInserter) {

        return getInserter(key, inTable, outTable, recordTransformer, dbInserter, Optional.empty());
    }

    /**
     * Create, if needed, a new {@link BulkInserter} instance keyed by input table.
     *
     * <p>If an inserter has previously been created by this factory for the given input table,
     * that inserter is returned instead.</p>
     *
     * @param key               an object to be used as a key for this inserter's stats object
     * @param inTable           table for records that will be fed to this inserter
     * @param outTable          table for records that will be inserted into the database
     * @param recordTransformer function to transform input records to database records
     * @param dbInserter        {@link DbInserter} responsible for saving database records
     * @param config            optional config object to use instead of default
     * @param <InT>             input record type
     * @param <OutT>            output record type
     * @return the existing or newly created inserter
     */
    public <InT extends Record, OutT extends Record> BulkInserter<InT, OutT> getInserter(
            @Nonnull Object key,
            @Nonnull Table<InT> inTable,
            @Nonnull Table<OutT> outTable,
            @Nonnull RecordTransformer<InT, OutT> recordTransformer,
            @Nonnull DbInserter<OutT> dbInserter,
            @Nonnull Optional<BulkInserterConfig> config) {

        synchronized (inserters) {
            BulkInserter<InT, OutT> inserter = (BulkInserter<InT, OutT>)inserters.get(key);
            if (inserter == null) {
                final BulkInserterConfig configToUse = config.orElse(defaultConfig);
                inserter = new BulkInserter<>(basedbIO, key, inTable, outTable, configToUse,
                        recordTransformer, dbInserter, batchCompleter);
                inserters.put(key, inserter);
            }
            return inserter;
        }
    }

    /**
     * Create a new inserter for a transient table, created for the lifetime of this inserter,
     * with the same column structure as the provided out-table.
     *
     * <p>The created table will have a new name like 'baseTable_transient_xxx', where baseTable is
     * the name of the given out-table, and xxx is a time-based number. The inserter will work
     * like the given out-table, but when the inserter is closed, the table will be dropped.</p>
     *
     * <p>These transient tables are unlike the "temporary" tables supported by MySQL; those would
     * not work with this framework, because they are visible only to the connection from which
     * they were created, and are dropped when that connection is closed.</p>
     *
     * <p>The <code>postTableCreateFunc</code> parameter can be used to create indexes and perform
     * other necessary operations on the newly created table. By default, no indexes will be
     * created for the transient table.</p>
     *
     * @param inTable             table for records that will be fed to this inserter
     * @param outTable            table whose structure will be used to define the transient
     *                            table, where records sent to this instance will actually be inserted
     * @param recordTransformer   function to transform input records to database records
     * @param dbInserter          {@link DbInserter} responsible for saving database records
     * @param postTableCreateFunc function that is invoked after table creation
     * @param <InT>               input record type
     * @param <OutT>              output record type
     * @return the existing or newly created inserter
     * @throws SQLException if we encounter a database exception
     * @throws VmtDbException if we have a DB connection problem
     * @throws InstantiationException if we can't instantiate the transient jOOQ table
     * @throws IllegalAccessException if we can't instantiate the transient jOOQ table
     */
    public <InT extends Record, OutT extends Record> BulkInserter<InT, OutT> getTransientInserter(
            @Nonnull Table<InT> inTable,
            @Nonnull Table<OutT> outTable,
            @Nonnull RecordTransformer<InT, OutT> recordTransformer,
            @Nonnull DbInserter<OutT> dbInserter,
            @Nullable TableOperation<OutT> postTableCreateFunc)
            throws SQLException, InstantiationException, VmtDbException, IllegalAccessException {
        try (Connection conn = basedbIO.connection()) {
            // create a jOOQ table object just like the out table but with a different name
            // and no indexes
            final String tableNamePrefix = outTable.getName() + TRANSIENT_TABLE_SUFFIX;
            String transientTableName = tableNamePrefix + System.nanoTime();
            Table<OutT> transientTable = outTable.getClass().newInstance().as(transientTableName);
            // create that table in the database
            basedbIO.using(conn).createTable(transientTable).columns(outTable.fields())
                    .execute();
            if (postTableCreateFunc != null) {
                postTableCreateFunc.execute(transientTable);
            }
            return getInserter(transientTable, inTable, transientTable, recordTransformer, dbInserter,
                    Optional.of(ImmutableBulkInserterConfig.copyOf(defaultConfig).withDropOnClose(true)));
        } catch (SQLException | VmtDbException | InstantiationException | IllegalAccessException e) {
            logger.error("Failed to create transient table based on {}", outTable.getName(), e);
            throw e;
        }
    }

    /**
     * Called during history component shutdown to check for any transient tables that may have
     * been in use at the time of shutdown and deletes them.
     *
     * <p>The code that would normally drop these tables is unlikely to run due to the shutdown.</p>
     *
     * @param ctx {@link DSLContext} to use for DB operations
     */
    public static void cleanupTransientTables(DSLContext ctx) {
        try {
            final List<String> tables =
                    ctx.fetch("SHOW TABLES LIKE '%" + TRANSIENT_TABLE_SUFFIX + "%'").stream()
                            .map(r -> r.getValue(0, String.class))
                            .collect(Collectors.toList());
            for (final String table : tables) {
                // Double-check - we really don't want to be deleting tables that we're not
                // supposed to!
                if (table.contains(TRANSIENT_TABLE_SUFFIX)) {
                    logger.info("Dropping orphaned transient table {}", table);
                    ctx.dropTable(table).execute();
                } else {
                    logger.error("Narrowly avoided dropping table {} - should not be possible",
                            table);
                }
            }
        } catch (DataAccessException e) {
            logger.warn("Failed to clean up orphaned transient tables", e);
        }
    }

    /**
     * Flush all existing inserter instances and wait until they're all done.
     *
     * <p>This is probably only really meaningful if all clients are inactive while this is
     * executing, though if that's not the case, no harm is really done, as long as that makes
     * sense in the clients.</p>
     *
     * @throws InterruptedException if interrupted
     */
    public void flushAll() throws InterruptedException {
        // first flush all inserters, so they can write pending records in parallel
        for (BulkInserter<?, ?> bulkInserter : inserters.values()) {
            bulkInserter.flush(false);
        }
        batchCompleter.drain();
    }

    /**
     * Obtain {@link BulkInserterStats} objects for all inserters created by this factory.
     *
     * <p>Results are provided in the form of a {@link BulkInserterFactoryStats} object
     * which from which stats for individual inserters can be retrieved, as well as a
     * stats object representing totals across all inserters.</p>
     *
     * @return stats object
     */
    public BulkInserterFactoryStats getStats() {
        return new BulkInserterFactoryStats(inserters.values().stream()
                .map(BulkInserter::getStats)
                .collect(Collectors.toList()));
    }

    /**
     * Obtain a suitable label for the given inserter key, i.e. for the associated inserter.
     *
     * <p>If the key is a {@link Table} object, which is usually the case, that table's name
     * is provided as a label. Otherwise, the keys {@link Object#toString()} method is used</p>
     *
     * @param key the inserter key
     * @return a suitable label for the inserter
     */
    public static String getKeyLabel(Object key) {
        return getKeyLabel(key, Object::toString);
    }

    /**
     * Alternative to {@link #getKeyLabel(Object)} where a labeling function for non-table keys
     * is provided, instead of using the default.
     *
     * @param key             the inserter key
     * @param nonTableLabeler a function to compute labels for non-table keys
     * @return a suitable label for the inserter
     */
    private static String getKeyLabel(Object key, Function<Object, String> nonTableLabeler) {
        return key instanceof Table<?> ? ((Table<?>)key).getName()
                : nonTableLabeler.apply(key);
    }

    /**
     * Close all our inserters.
     *
     * @throws InterruptedException if interrupted
     */
    @Override
    public void close() throws InterruptedException {
        close(null);
    }

    /**
     * Close all our inserters and report inserter stats using given logger.
     *
     * @param logger logger to use for inserter stats if not null, else don't report stats
     * @throws InterruptedException if interrupted
     */
    public void close(Logger logger) throws InterruptedException {
        // flush explicitly rather than as side-effect of close, so that we get any logging
        // produced by all flushes prior to stats logged by close operations
        flushAll();
        for (BulkInserter<?, ?> bulkInserter : inserters.values()) {
            bulkInserter.close(logger);
        }
        batchCompleter.close();
    }

    /**
     * Interface for a function that performs database operations on a jOOQ table, capable of
     * throwing potential exceptions.
     *
     * @param <R> record type on which table is based
     */
    @FunctionalInterface
    public interface TableOperation<R extends Record> {
        /**
         * Perofrm the table operation.
         * @param table the table to operate on
         * @throws SQLException if a database error occurs
         * @throws DataAccessException if a database error occurs in jOOQ-based code
         * @throws VmtDbException if a connection error occurs
         */
        void execute(Table<R> table) throws SQLException, DataAccessException, VmtDbException;
    }
}
