package com.vmturbo.kibitzer.activities.history;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterators;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.springframework.context.ApplicationContext;

import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.history.db.bulk.BulkInserter.BatchStats;
import com.vmturbo.history.db.bulk.BulkInserterConfig;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.db.bulk.BulkInserterStats;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.ImmutableBulkInserterConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.RelationTypeConverter;
import com.vmturbo.history.schema.abstraction.Routines;
import com.vmturbo.history.schema.abstraction.Vmtdb;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.kibitzer.Kibitzer;
import com.vmturbo.kibitzer.KibitzerComponent;
import com.vmturbo.kibitzer.KibitzerDb;
import com.vmturbo.kibitzer.KibitzerDb.DbMode;
import com.vmturbo.kibitzer.activities.ActivityConfig;
import com.vmturbo.kibitzer.activities.KibitzerActivity;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * A {@link KibitzerActivity} that serves as a performance benchmark of ingestion-related DB
 * activity in `history` component. In each repetition, a designated number of records are inserted
 * into a designated entity-stats table, using the same `{@link BulkLoader} as is used in history.
 *
 * <p>If multiple {@link IngestionBenchmark} activities are specified for the same
 * {@link Kibitzer} execution, they share a {@link SimpleBulkLoaderFactory} and so use the same
 * loader instances (if they're configured for the same table), and the same thread pool for
 * performing insertion batches.</p>
 *
 * <p>Each repetition of an activity contributes to an overall `BulkInserterFactoryStats` instance,
 * which is then used to render a report at completion.</p>
 */
public class IngestionBenchmark
        extends KibitzerActivity<BulkInserterStats, BulkInserterFactoryStats> {
    private static final Logger logger = LogManager.getLogger();
    private static final String TABLE_CONFIG_NAME = "table";
    private static final String SEED_TABLE_CONFIG_NAME = "seed_table";
    private static final String BATCH_SIZE_CONFIG_NAME = "batch_size";
    private static final String RECORD_COUNT_CONFIG_NAME = "record_count";
    private static final String INSERT_MODE_CONFIG_NAME = "insert_mode";
    private static final String PARALLEL_INSERTERS_CONFIG_NAME = "parallel_inserters";

    private Table<Record> table;
    private int recordCount;
    private static SimpleBulkLoaderFactory loaders;
    private Result<?> seedRecords;
    private Field<Timestamp> snapshotTimeField;
    private InsertMode insertMode;
    private int batchSize;

    // this is used in JDBC* insert modes to convert between RelationType values appearing in the
    // jOOQ Record objects we use, and the corresponding TINYINT values that get written to the DB
    private final RelationTypeConverter relationTypeConverter = new RelationTypeConverter();

    /**
     * Create a new instance with default configuration.
     *
     * @throws KibitzerActivityException if there's a problem creating the instance
     */
    public IngestionBenchmark() throws KibitzerActivityException {
        super(KibitzerComponent.HISTORY, "ingestion_benchmark");
        defineProperties();
        config.setDefault(DESCRIPTION_CONFIG_KEY,
                "Benchmarks performance of insert operations similar to those "
                        + "performed in topology ingestion");
        config.setDefault(DB_MODE_CONFIG_KEY, DbMode.RETAINED_COPY);
        config.setDefault(RUNS_CONFIG_KEY, 18);
        config.setDefault(SCHEDULE_CONFIG_KEY, Duration.ofMinutes(10));
    }

    @Override
    public KibitzerActivity<BulkInserterStats, BulkInserterFactoryStats> newInstance()
            throws KibitzerActivityException {
        return new IngestionBenchmark();
    }

    private void defineProperties() throws KibitzerActivityException {
        config.add(config.tableProperty(TABLE_CONFIG_NAME, Vmtdb.VMTDB)
                .withValidator(ActivityConfig::isEntityStatsTable,
                        "must be an entity stats table")
                .withDefault(VmStatsLatest.VM_STATS_LATEST)
                .withDescription("table into which records will be inserted"));
        config.add(config.tableProperty(SEED_TABLE_CONFIG_NAME, Vmtdb.VMTDB)
                .withValidator(ActivityConfig::isEntityStatsTable,
                        "must be an entity stats table")
                .withDescription(
                        "prod table from which to obtain records for insertion",
                        "defaults to whatever table the 'table' property resolves to"));
        config.add(config.intProperty(BATCH_SIZE_CONFIG_NAME)
                .withValidator(n -> n > 1, "must be positive")
                .withDefault(1_000)
                .withDescription("number of records that will be inserted in a single operation"));

        config.add(config.intProperty(RECORD_COUNT_CONFIG_NAME)
                .withValidator(n -> n > 1, "must be positive")
                .withDefault(25_000)
                .withDescription("number of records that will be inserted in a single operation"));

        config.add(config.enumProperty(INSERT_MODE_CONFIG_NAME, InsertMode.class)
                .withDefault(InsertMode.JDBC)
                .withDescription("JDBC to perform inserts with JDBC, "
                        + "JDBC_BATCHING for JDBC with batching, or "
                        + "PROD to imitate production code"));
        config.add(config.intProperty(PARALLEL_INSERTERS_CONFIG_NAME)
                .withValidator(n -> n > 0, "must be positive")
                .withDefault(8)
                .withDescription("number of parallel insertion threads to suse (PROD only)"));
    }

    @Override
    public void report(BulkInserterFactoryStats result) {
        logger.info("Results for activity {}", this);
        result.getTotals().logStats(logger);
    }

    @Override
    public Optional<BulkInserterStats> run(int repetition) throws KibitzerActivityException {
        switch (insertMode) {
            case JDBC:
                return writeRecordsWithJDBCNoBatching(repetition);
            case JDBC_BATCHING:
                return writeRecordsWithJDBCBatching(repetition);
            case PROD:
                return writeRecordsWithJooq();
            default:
                throw new KibitzerActivityException(
                        String.format("Unrecognized InsertMode value: %s", insertMode));
        }
    }

    private Optional<BulkInserterStats> writeRecordsWithJDBCBatching(int repetition) {
        String sql = getPrepStmtSQL(table);
        Iterator<Record> infiniteSeeds = Iterators.cycle(seedRecords.toArray(new Record[0]));
        Timestamp now = new Timestamp(System.currentTimeMillis());
        return dsl.connectionResult(conn -> {
            int written = 0;
            int batchCount = 0;
            BulkInserterStats stats = new BulkInserterStats(table + "-" + repetition, table, table);
            BatchStats batchStats;
            Stopwatch watch = Stopwatch.createStarted();
            long start = System.nanoTime();
            long bindingTime = 0;
            while (written < recordCount) {
                batchCount += 1;
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    int recsInBatch = 0;
                    while (recsInBatch < batchSize && written < recordCount) {
                        Record record = infiniteSeeds.next();
                        record.set(snapshotTimeField, now);
                        addBinding(ps, record);
                        written += 1;
                        recsInBatch += 1;
                    }
                    bindingTime = System.nanoTime() - start;
                    int[] batchCounts = ps.executeBatch();
                    int n = Arrays.stream(batchCounts).sum();
                    if (n != recsInBatch) {
                        logger.warn("Batch insertion expected to insert {} records but inserted {}",
                                recsInBatch, n);
                    }
                    batchStats = new BatchStats(false, n, System.nanoTime() - start - bindingTime,
                            0L);
                } catch (DataAccessException e) {
                    logger.error("JDBC batch insertion failed for batch #{}", batchCount, e);
                    batchStats = new BatchStats(true, 0, 0L,
                            System.nanoTime() - start - bindingTime);
                }
                stats.updateForBatch(batchStats);
            }
            logger.info("Wrote {} records to {} in {}", written, table.getName(), watch);
            return Optional.of(stats);
        });
    }

    private Optional<BulkInserterStats> writeRecordsWithJDBCNoBatching(int repetition) {
        Iterator<Record> infiniteSeeds = Iterators.cycle(seedRecords.toArray(new Record[0]));
        Timestamp now = new Timestamp(System.currentTimeMillis());
        Stopwatch watch = Stopwatch.createStarted();
        return dsl.connectionResult(conn -> {
            int written = 0;
            BulkInserterStats stats = new BulkInserterStats(table + "-" + repetition, table, table);
            BatchStats batchStats;
            int batchCount = 0;
            while (written < recordCount) {
                batchCount += 1;
                List<Record> batch = new ArrayList<>();
                long start = System.nanoTime();
                while (batch.size() < batchSize && written + batch.size() < recordCount) {
                    Record record = infiniteSeeds.next();
                    record.set(snapshotTimeField, now);
                    batch.add(record);
                }
                getPrepStmtSQL(table, batch);
                String sql = getPrepStmtSQL(table, batch);
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    addBindings(ps, batch);
                    int n = ps.executeUpdate();
                    if (n != batch.size()) {
                        logger.warn("Expected to insert {} records but inserted {} in batch #{}",
                                batch.size(), n, batchCount);
                    }
                    written += batch.size();
                    batchStats = new BatchStats(false, n, System.nanoTime() - start, 0L);
                } catch (DataAccessException e) {
                    logger.error("JDBC batch insertion failed for batch #{}", batchCount, e);
                    batchStats = new BatchStats(true, 0, 0L,
                            System.nanoTime() - start);
                }
                stats.updateForBatch(batchStats);
            }
            logger.info("Wrote {} records to {} in {}", written, table.getName(), watch);
            return Optional.of(stats);
        });
    }

    private void addBindings(PreparedStatement ps, List<Record> batch) throws SQLException {
        int bindingPos = 1;
        for (Record record : batch) {
            for (Field<?> field : record.fields()) {
                Object value = record.getValue(field);
                Object converted = field.getName().equals(StringConstants.RELATION)
                                   ? relationTypeConverter.to((RelationType)value)
                                   : value;
                ps.setObject(bindingPos++, converted,
                        field.getDataType().getSQLType(dsl.configuration()));
            }
        }
    }

    private void addBinding(PreparedStatement ps, Record record) throws SQLException {
        Field<?>[] fields = record.fields();
        for (int i = 0; i < fields.length; i++) {
            Object value = record.getValue(i);
            if (fields[i].getName().equals(StringConstants.RELATION)) {
                value = relationTypeConverter.to((RelationType)value);
            }
            ps.setObject(i + 1, value, fields[i].getDataType().getSQLType(dsl.configuration()));
        }
        ps.addBatch();
    }

    private String getPrepStmtSQL(Table<Record> table) {
        String cols = Arrays.stream(table.fields()).map(Field::getName).collect(
                Collectors.joining(","));
        String placeholders = Arrays.stream(table.fields()).map(f -> "?").collect(
                Collectors.joining(","));
        return String.format("INSERT INTO %s (%s) VALUES (%s)",
                table.getName(), cols, placeholders);
    }

    private String getPrepStmtSQL(Table<Record> table, List<Record> batch) {
        String cols = Arrays.stream(table.fields()).map(Field::getName).collect(
                Collectors.joining(","));
        String placeholders = "("
                + Arrays.stream(table.fields()).map(f -> "?").collect(Collectors.joining(","))
                + ")";
        return String.format("INSERT INTO %s (%s) VALUES", table.getName(), cols)
                + batch.stream().map(r -> placeholders).collect(Collectors.joining(","));
    }

    private Optional<BulkInserterStats> writeRecordsWithJooq() throws KibitzerActivityException {
        try {
            Routines.rotatePartition(dsl.configuration(), table.getName(), null);
            BulkLoader<Record> loader = loaders.getLoader(table);
            Timestamp now = new Timestamp(System.currentTimeMillis());
            int written = 0;
            boolean interrupted = false;
            Stopwatch watch = Stopwatch.createStarted();
            while (written < recordCount && !interrupted) {
                for (Record record : seedRecords) {
                    record.set(snapshotTimeField, now);
                    try {
                        loader.insert(record);
                    } catch (InterruptedException e) {
                        interrupted = true;
                        Thread.currentThread().interrupt();
                    }
                    if (++written >= recordCount) {
                        break;
                    }
                }
            }
            loader.flush(true);
            logger.info("Wrote {} records to {} in {}", written, table.getName(), watch);
            return Optional.of(loader.getStats());
        } catch (InterruptedException e) {
            logger.warn("Interrupted while flushing loader");
            Thread.currentThread().interrupt();
            return Optional.empty();
        } catch (RuntimeException e) {
            throw new KibitzerActivityException(
                    String.format("Failed to execute activity %s", getTag()), e);
        }
    }

    @Override
    public Optional<BulkInserterFactoryStats> finish(List<BulkInserterStats> runResults) {
        if (runResults.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new BulkInserterFactoryStats(runResults));
        }
    }

    @Override
    public void init(DSLContext dsl, KibitzerDb db, ApplicationContext context)
            throws KibitzerActivityException {
        super.init(dsl, db, context);
        try {
            //noinspection unchecked
            this.table = config.getTable(TABLE_CONFIG_NAME);
            this.snapshotTimeField = table.field(StringConstants.SNAPSHOT_TIME, Timestamp.class);
            this.batchSize = config.getInt(BATCH_SIZE_CONFIG_NAME);
            this.recordCount = config.getInt(RECORD_COUNT_CONFIG_NAME);
            this.insertMode = config.getEnum(INSERT_MODE_CONFIG_NAME, InsertMode.class);
            if (insertMode == InsertMode.PROD) {
                int parallelInserters = config.getInt(PARALLEL_INSERTERS_CONFIG_NAME);
                BulkInserterConfig loaderConfig = ImmutableBulkInserterConfig.builder()
                        .batchSize(batchSize)
                        .maxPendingBatches(parallelInserters)
                        .maxBatchRetries(1)
                        .maxRetryBackoffMsec(0)
                        .flushTimeoutSecs(300)
                        .build();
                synchronized (this) {
                    if (loaders == null) {
                        loaders = new SimpleBulkLoaderFactory(dsl, loaderConfig,
                                null, () -> Executors.newFixedThreadPool(parallelInserters));
                    }
                }
            }
            DbEndpoint componentEndpoint = db.getDatabase(
                            getComponent().info(), getTag(), DbMode.COMPONENT, context)
                    .orElse(null);
            DSLContext componentDsl =
                    componentEndpoint != null ? componentEndpoint.dslContext() : null;
            Table<?> seedTable = config.getTable(SEED_TABLE_CONFIG_NAME);
            if (seedTable == null) {
                seedTable = config.getTable(TABLE_CONFIG_NAME);
            }
            seedRecords = componentDsl.selectFrom(seedTable).limit(batchSize).fetch();
            if (seedRecords.isEmpty()) {
                throw new KibitzerActivityException("No seed records available for ingestion");
            }
        } catch (UnsupportedDialectException | SQLException | InterruptedException e) {
            throw new KibitzerActivityException(
                    String.format("Failed to initialize activity %s", getTag()), e);
        }
    }

    /**
     * This enum values specifies ways in which data insertions may be executed in an instance of
     * this activity.
     */
    public enum InsertMode {
        /** Execute insertions using the bulk inserter as is done by history component. */
        PROD,
        /** Insert records using a JDBC prepared statement without JDBC batching. */
        JDBC,
        /** Insert records using a JDBC prepared statement with JDBC batching. */
        JDBC_BATCHING
    }
}
