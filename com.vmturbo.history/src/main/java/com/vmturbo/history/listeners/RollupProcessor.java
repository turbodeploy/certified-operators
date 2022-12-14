package com.vmturbo.history.listeners;

import static com.vmturbo.history.schema.abstraction.Tables.APPLICATION_SERVICE_DAYS_EMPTY;
import static com.vmturbo.history.schema.abstraction.Tables.AVAILABLE_TIMESTAMPS;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.CLUSTER_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_BY_MONTH;
import static com.vmturbo.history.schema.abstraction.Tables.MARKET_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.VOLUME_ATTACHMENT_HISTORY;
import static org.jooq.impl.DSL.currentDate;
import static org.jooq.impl.DSL.dateDiff;
import static org.jooq.impl.DSL.exists;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.max;
import static org.jooq.impl.DSL.select;
import static org.jooq.impl.DSL.selectFrom;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Functions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.DatePart;
import org.jooq.Field;
import org.jooq.InsertReturningStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.history.db.ClusterStatsRollups;
import com.vmturbo.history.db.EntityStatsRollups;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.MarketStatsRollups;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.RetentionUtil;
import com.vmturbo.history.schema.abstraction.Routines;
import com.vmturbo.history.schema.abstraction.routines.MarketAggregate;
import com.vmturbo.history.schema.abstraction.tables.ApplicationServiceDaysEmpty;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.sql.utils.jooq.PurgeProcedure;
import com.vmturbo.sql.utils.partition.IPartitioningManager;
import com.vmturbo.sql.utils.partition.PartitionProcessingException;

/**
 * This class performs rollups of stats entity and market stats data in the database, and it can
 * also be used to perform retention processing to delete expired data.
 */
public class RollupProcessor {
    private static final Logger logger = LogManager.getLogger(RollupProcessor.class);

    // maximum time we'll wait for a repartitioning task (for a single table) to complete
    private static final long REPARTITION_TIMEOUT_SECS = 60;

    // various strings related to stats related tables and stored procs
    private static final String MARKET_TABLE_PREFIX = "market";
    private static final String MARKET_ROLLUP_PROC = new MarketAggregate().getName();
    /**
     * If the most recent last_discovered_date column value for a Volume OID is older than (current
     * date - VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD), then all records related to this Volume OID
     * will be deleted from the volume_attachment_history table. As this implies that the Volume has
     * not been discovered for a period defined by VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD and may
     * have been deleted from the environment.
     */
    public static final int VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD = 14;
    /**
     * application_service_days_empty
     * If the duration between the last discovered date and the current time for a row in the table
     * is greater than the retention period, the row will be deleted. This condition means that
     * the application service entity has not been discovered during the retention period and
     * most likely has been deleted from the environment.
     */
    public static final int APP_SVC_DAYS_EMPTY_RETENTION_PERIOD = 14;
    private static final int MISSING_RECORD_COUNT = 1_000_000;
    private static final ApplicationServiceDaysEmpty ASDE = APPLICATION_SERVICE_DAYS_EMPTY;

    private final ExecutorService executorService;
    private final DSLContext dsl;
    private final DSLContext unpooledDsl;
    private final IPartitioningManager partitioningManager;
    private final int maxBatchSize;
    private final long timeoutMsecForBatchSize;
    private final long minTimeoutMsec;
    private final int maxBatchRetry;

    /**
     * Create a new instance.
     *
     * @param dsl                     DB access for general use
     * @param unpooledDsl             DB access for long-running operations
     * @param partitioningManager     partition manager
     * @param executorServiceSupplier thread pool to run individual tasks
     * @param maxBatchSize            maximum # of records in a single batch
     * @param timeoutMsecForBatchSize a timeout duration that will be multiplied by the # of batches
     *                                to provide a time limit for the overall rollup operation
     * @param minTimeoutMsec          minimum timeout for overall rollup, so small topologies don't
     *                                end up with an insufficient time to complete reliably
     * @param maxBatchRetry           number of times a failed batch should be retried. Retries do
     */
    public RollupProcessor(DSLContext dsl, DSLContext unpooledDsl,
            IPartitioningManager partitioningManager,
            Supplier<ExecutorService> executorServiceSupplier, int maxBatchSize,
            long timeoutMsecForBatchSize, long minTimeoutMsec, int maxBatchRetry) {
        this.dsl = dsl;
        this.unpooledDsl = unpooledDsl;
        this.partitioningManager = partitioningManager;
        this.executorService = executorServiceSupplier.get();
        this.maxBatchSize = maxBatchSize;
        this.timeoutMsecForBatchSize = timeoutMsecForBatchSize;
        this.minTimeoutMsec = minTimeoutMsec;
        this.maxBatchRetry = maxBatchRetry;
    }

    /**
     * Perform hourly rollup processing for all tables that were active in ingestions that were
     * performed for the given snapshot time.
     *
     * @param tableCounts  tables that were populated by ingestions for the given snapshot, with
     *                     record counts
     * @param msecSnapshot snapshot time of contributing ingestions
     * @return overall outcome of rollup operation
     */
    RollupOutcome performHourRollups(@Nonnull Map<Table<?>, Long> tableCounts,
            @Nonnull Instant msecSnapshot) {
        Timestamp snapshot = dsl.dialect() == SQLDialect.POSTGRES
                             // postgres schema has msec-granularity timestamps
                             ? Timestamp.from(msecSnapshot)
                             : Timestamp.from(msecSnapshot.truncatedTo(ChronoUnit.SECONDS));
        MultiStageTimer timer = new MultiStageTimer(logger);
        RollupOutcome outcome = performRollups(tableCounts, snapshot, RollupType.BY_HOUR, timer);
        addAvailableTimestamps(snapshot, RollupType.BY_HOUR, HistoryVariety.ENTITY_STATS,
                HistoryVariety.PRICE_DATA);
        String incomplete = outcome != RollupOutcome.COMPLETE ? "" : "[INCOMPLETE]";
        timer.stopAll().withElapsedSegment("Total Elapsed").info(
                String.format("Hourly Rollup Processing for %s%s", snapshot, incomplete),
                Detail.STAGE_SUMMARY);
        return outcome;
    }

    /**
     * Perform daily and monthly rollups for all tables that were updated by ingestions for
     * snapshots falling in a single hour.
     *
     * <p>This should be done after ingestions for the hour have all completed.</p>
     *
     * @param tableCounts  tables that participate in ingestions during the hour, with record
     *                     counts
     * @param msecSnapshot instant from within the hour to roll up
     * @return overall outcome of rollup operation
     */
    RollupOutcome performDayMonthRollups(final Map<Table<?>, Long> tableCounts,
            final Instant msecSnapshot) {
        return performDayMonthRollups(tableCounts, msecSnapshot, true);
    }

    /**
     * Perform daily and monthly rollups for all tables that were updated by ingestions for
     * snapshots falling in a single hour, and perhaps do repartioning as well.
     *
     * <p>This should be done after ingestions for the hour have all completed.</p>
     *
     * @param tableCounts  tables that participate in ingestions during the hour, with record
     *                     counts
     * @param msecSnapshot instant from within the hour to roll up
     * @param doRetention  true to do retention processing after rollups
     * @return overall outcome of rollup opsertion
     */
    RollupOutcome performDayMonthRollups(final Map<Table<?>, Long> tableCounts,
            final Instant msecSnapshot,
            boolean doRetention) {
        Timestamp snapshot = Timestamp.from(msecSnapshot.truncatedTo(ChronoUnit.HOURS));
        MultiStageTimer timer = new MultiStageTimer(logger);
        final RollupOutcome dailyOutcome = performRollups(tableCounts, snapshot, RollupType.BY_DAY,
                timer);
        addAvailableTimestamps(snapshot, RollupType.BY_DAY, HistoryVariety.ENTITY_STATS,
                HistoryVariety.PRICE_DATA);
        if (dailyOutcome == RollupOutcome.INTERRUPTED) {
            // don't keep working if we were interrupted
            logger.warn(
                    "Skipping monhtly rollup and retention processing because daily rollup was interrupted");
            return dailyOutcome;
        }
        final RollupOutcome monthlyOutcome = performRollups(tableCounts, snapshot,
                RollupType.BY_MONTH, timer);
        if (monthlyOutcome == RollupOutcome.INTERRUPTED) {
            logger.warn(
                    "Skipping retention processing because monthly rollup was interrupted");
            return monthlyOutcome;
        }
        addAvailableTimestamps(snapshot, RollupType.BY_MONTH, HistoryVariety.ENTITY_STATS,
                HistoryVariety.PRICE_DATA);
        if (doRetention) {
            performRetentionProcessing(timer);
        }
        String incomplete = !(dailyOutcome.isComplete() && monthlyOutcome.isComplete())
                            ? "" : "[INCOMPLETE]";
        timer.stopAll().withElapsedSegment("Total Elapsed").info(
                String.format("Daily/Monthly Rollup Processing for %s%s", snapshot, incomplete),
                Detail.STAGE_SUMMARY);
        return dailyOutcome.isComplete() ? monthlyOutcome : dailyOutcome;
    }

    @VisibleForTesting
    void addAvailableTimestamps(Timestamp snapshot, RollupType rollupType,
            HistoryVariety... historyVarieties) {
        for (HistoryVariety historyVariety : historyVarieties) {
            addAvailableTimestamp(snapshot, rollupType, historyVariety);
        }
    }

    private void addAvailableTimestamp(Timestamp snapshot, RollupType rollupType,
            HistoryVariety historyVariety) {
        Timestamp rollupTime = rollupType.getRollupTime(snapshot);
        Timestamp rollupStart = rollupType.getPeriodStart(snapshot);
        Timestamp rollupEnd = rollupType.getPeriodEnd(snapshot);
        try {
            dsl.insertInto(AVAILABLE_TIMESTAMPS,
                            AVAILABLE_TIMESTAMPS.TIME_STAMP,
                            AVAILABLE_TIMESTAMPS.TIME_FRAME,
                            AVAILABLE_TIMESTAMPS.HISTORY_VARIETY,
                            AVAILABLE_TIMESTAMPS.EXPIRES_AT)
                    .select(
                            select(
                                    inline(rollupTime).as(AVAILABLE_TIMESTAMPS.TIME_STAMP),
                                    inline(rollupType.getTimeFrame().name()).as(
                                            AVAILABLE_TIMESTAMPS.TIME_FRAME),
                                    inline(historyVariety.name()).as(
                                            AVAILABLE_TIMESTAMPS.HISTORY_VARIETY),
                                    inline(Timestamp.from(
                                            rollupType.getRetentionPolicy()
                                                    .getExpiration(rollupTime.toInstant())))
                                            .as(AVAILABLE_TIMESTAMPS.EXPIRES_AT))
                                    .from(AVAILABLE_TIMESTAMPS)
                                    .where(exists(selectFrom(AVAILABLE_TIMESTAMPS)
                                            .where(AVAILABLE_TIMESTAMPS.TIME_STAMP.between(
                                                    inline(rollupStart), inline(rollupEnd)))
                                            .and(AVAILABLE_TIMESTAMPS.HISTORY_VARIETY.eq(
                                                    inline(historyVariety.name()))))
                                    ))

                    .onDuplicateKeyIgnore()
                    .execute();
        } catch (DataAccessException e) {
            logger.error("Failed to rollup available_timestamps", e);
        }
    }

    void performRetentionProcessing(MultiStageTimer timer) {
        performRetentionProcessing(timer, true);
    }

    void performRetentionProcessing(MultiStageTimer timer, boolean doEntityStatsRetentioning) {
        // retention processing for entity stats tables is done by reconfiguring their partitions so
        // that partitions containing expired records are dropped, and currently only works in
        // MARIADB.
        if (doEntityStatsRetentioning) {
            performEntityStatsRetentioning(timer);
        }
        // available_timestamps is small, so we just manually delete records that exceed expiration
        timer.start("Expire available_timestamps records");
        try {
            dsl.deleteFrom(AVAILABLE_TIMESTAMPS)
                    .where(DSL.currentTimestamp().ge(AVAILABLE_TIMESTAMPS.EXPIRES_AT)).execute();
        } catch (DataAccessException e) {
            logger.error("Failed to delete expired available_timestamps records", e);
        } finally {
            timer.stop();
        }
        timer.start("Purge expired cluster_stats records");
        try {
            if (FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()) {
                new PurgeProcedure(dsl, CLUSTER_STATS_LATEST,
                        CLUSTER_STATS_LATEST.RECORDED_ON, RETENTION_POLICIES,
                        RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                        RetentionUtil.LATEST_STATS_RETENTION_POLICY_NAME, DatePart.HOUR).run();
                new PurgeProcedure(dsl, CLUSTER_STATS_BY_HOUR,
                        CLUSTER_STATS_BY_HOUR.RECORDED_ON, RETENTION_POLICIES,
                        RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                        RetentionUtil.HOURLY_STATS_RETENTION_POLICY_NAME, DatePart.HOUR).run();
                new PurgeProcedure(dsl, CLUSTER_STATS_BY_DAY,
                        CLUSTER_STATS_BY_DAY.RECORDED_ON, RETENTION_POLICIES,
                        RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                        RetentionUtil.DAILY_STATS_RETENTION_POLICY_NAME, DatePart.DAY).run();
                new PurgeProcedure(dsl, CLUSTER_STATS_BY_MONTH,
                        CLUSTER_STATS_BY_MONTH.RECORDED_ON, RETENTION_POLICIES,
                        RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD,
                        RetentionUtil.MONTHLY_STATS_RETENTION_POLICY_NAME, DatePart.MONTH).run();
            } else {
                Routines.purgeExpiredClusterStats(dsl.configuration());
            }
        } catch (DataAccessException e) {
            logger.error("Failed to delete expired cluster_stats records", e);
        } finally {
            timer.stop();
        }
        purgeExpiredVolumeAttachmentHistoryRecords(timer);
        purgeExpiredAppServiceDaysEmptyRecords(timer);
    }

    private void purgeExpiredVolumeAttachmentHistoryRecords(MultiStageTimer timer) {
        timer.start("Purge expired volume_attachment_history records");
        try {
            final int deletedRowsCount = dsl.delete(VOLUME_ATTACHMENT_HISTORY)
                    .where(VOLUME_ATTACHMENT_HISTORY.VOLUME_OID
                            .in(dsl.select(VOLUME_ATTACHMENT_HISTORY.VOLUME_OID)
                                    .from(VOLUME_ATTACHMENT_HISTORY)
                                    .groupBy(VOLUME_ATTACHMENT_HISTORY.VOLUME_OID)
                                    .having(dateDiff(currentDate(),
                                            max(VOLUME_ATTACHMENT_HISTORY.LAST_DISCOVERED_DATE))
                                            .gt(VOL_ATTACHMENT_HISTORY_RETENTION_PERIOD))))
                    .execute();
            if (deletedRowsCount > 0) {
                logger.info("Number of rows deleted from Volume Attachment History table: {}",
                        deletedRowsCount);
            }
        } catch (DataAccessException e) {
            logger.error("Failed to delete expired volume_attachment_history records", e);
        } finally {
            timer.stop();
        }
    }

    @VisibleForTesting
    protected int purgeExpiredAppServiceDaysEmptyRecords(MultiStageTimer timer) {
        final String tableName = ASDE.getName();
        timer.start(String.format("Purge expired %s records", tableName));
        final Timestamp expiredDate = Timestamp.from(
                Instant.now().minus(APP_SVC_DAYS_EMPTY_RETENTION_PERIOD, ChronoUnit.DAYS)
        );
        try {
            final int deletedRowsCount = dsl.delete(ASDE)
                    .where(ASDE.ID
                            .in(dsl.select(ASDE.ID)
                                    .from(ASDE)
                                    .where(ASDE.LAST_DISCOVERED.lessThan(expiredDate))))
                    .execute();
            if (deletedRowsCount > 0) {
                logger.info("Number of rows deleted from {} table: {}",
                        tableName, deletedRowsCount);
            }
            return deletedRowsCount;
        } catch (DataAccessException e) {
            logger.error(String.format("Failed to delete expired %s records", tableName), e);
        } finally {
            timer.stop();
        }
        return 0;
    }

    /**
     * Perform retention update on all the stats tables.
     *
     * @param timer timer to use
     */
    private void performEntityStatsRetentioning(MultiStageTimer timer) {
        if (dsl.dialect() == SQLDialect.POSTGRES
                || FeatureFlags.OPTIMIZE_PARTITIONING.isEnabled()) {
            // if we're doing postgres and the FF is off, then we're using the pg_partman-based
            // implementation that has its own hourly scheduled operation to do partition expiry,
            // so in that case there's nothing to do. Otherwise (FF is enabled), this is where
            // we ask the partitioning manager to perform retention processing on all the
            // partitions.
            if (FeatureFlags.OPTIMIZE_PARTITIONING.isEnabled()) {
                timer.start("Purge expired partitions");
                partitioningManager.performRetentionUpdate();
            }
            return;
        }
        // here we've got the legacy implementation that has to do way too much work :(
        timer.start("Repartitioning");
        final Set<Table<? extends Record>> tables = getTablesToRepartition();
        final Stream<Pair<Table<? extends Record>, Future<Void>>> tableFutures = tables.stream()
                .sorted(Comparator.comparing(Table::getName))
                .map(table -> Pair.of(table, scheduleRepartition(table)));
        tableFutures.forEach(tf -> {
            try {
                tf.getRight().get(REPARTITION_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                logger.warn("Timed out during repartitioning of table {}; "
                        + "repartition may still complete normally", tf.getLeft());
            } catch (ExecutionException e) {
                logger.error("Error during repartitioning of table {}: {}",
                        tf.getLeft().getName(), e.toString());
            }
        });
    }

    private Set<Table<? extends Record>> getTablesToRepartition() {
        final Set<Table<? extends Record>> tables = new HashSet<>();
        tables.add(MarketStatsLatest.MARKET_STATS_LATEST);
        tables.add(MarketStatsByHour.MARKET_STATS_BY_HOUR);
        tables.add(MarketStatsByDay.MARKET_STATS_BY_DAY);
        tables.add(MarketStatsByMonth.MARKET_STATS_BY_MONTH);
        EntityType.allEntityTypes().stream()
                .filter(EntityType::rollsUp)
                .forEach(type -> {
                    List<String> missing = new ArrayList<>();
                    type.getLatestTable().map(tables::add).orElseGet(() -> missing.add("Latest"));
                    type.getHourTable().map(tables::add).orElseGet(() -> missing.add("Hourly"));
                    type.getDayTable().map(tables::add).orElseGet(() -> missing.add("Daily"));
                    type.getMonthTable().map(tables::add).orElseGet(() -> missing.add("Monthly"));
                    if (!missing.isEmpty()) {
                        logger.warn("Not rolling up entity type {}, which is missing tables {}",
                                type.getName(), missing);
                    }
                });
        return tables;
    }

    private Future<Void> scheduleRepartition(Table<?> table) {
        return executorService.submit(() -> {
            try {
                Routines.rotatePartition(unpooledDsl.configuration(), table.getName(), null);
            } catch (Exception e) {
                logger.error("Repartitioning failed for table {}: {}", table.getName(),
                        e.toString());
            }
            return null;
        });
    }

    private RollupOutcome performRollups(@Nonnull final Map<Table<?>, Long> tableCounts,
            final Timestamp snapshot, final RollupType rollupType, @Nonnull MultiStageTimer timer) {
        timer.start(rollupType.getLabel() + " Prep");
        // schedule all the task for execution in the thread pool
        final List<RollupTask> tasks = new ArrayList<>();
        List<Table<? extends Record>> tablesToRollUp = tableCounts.entrySet().stream()
                .filter(e -> e.getValue() > 0)
                .map(Entry::getKey)
                .filter(rollupType::canRollup)
                .collect(Collectors.toList());
        CompletionService<RollupTask> cs = new ExecutorCompletionService<>(this.executorService);
        for (Table<? extends Record> table : tablesToRollUp) {
            try {
                tasks.addAll(scheduleRollupTasks(table, rollupType, snapshot, cs,
                        tableCounts.get(table)));
            } catch (PartitionProcessingException e) {
                logger.error("Failed to perform rollups for table {}", table, e);
            }
        }
        timer.stop();
        // submit all the tasks after randomly shuffling them to reduce the likelihood of data gaps
        // if we have chronic timeouts on rollups.
        Collections.shuffle(tasks);
        Map<Future<RollupTask>, RollupTask> futures = tasks.stream()
                .collect(Collectors.toMap(RollupTask::submit, Functions.identity()));
        // now wait for each one to complete, and then we're done
        return waitForRollupTasks(cs, futures, rollupType, tableCounts, timer);
    }

    private List<RollupTask> scheduleRollupTasks(
            @Nonnull Table<? extends Record> table,
            @Nonnull RollupType rollupType, @Nonnull Timestamp snapshotTime,
            CompletionService<RollupTask> cs, Long recordCount)
            throws PartitionProcessingException {
        // Since CLUSTER is also part of EntityType, we need to first check CLUSTER_STATS_LATEST table.
        // Otherwise, entity stats roll up logic will be applied to cluster stats roll up.
        // TODO: improve the logic for cluster stats roll up
        if (table == CLUSTER_STATS_LATEST) {
            return scheduleClusterStatsRollupTasks(table, rollupType, snapshotTime,
                    cs);
        } else if (EntityType.fromTable(table).isPresent()) {
            return scheduleEntityStatsRollupTasks(table, rollupType, snapshotTime, recordCount,
                    cs);
        } else if (table == MARKET_STATS_LATEST) {
            return scheduleMarketStatsRollupTask(table, rollupType, snapshotTime,
                    cs);
        }
        throw new IllegalArgumentException(
                String.format("Cannot schedule rollup tasks for table: %s", table));
    }

    private List<RollupTask> scheduleEntityStatsRollupTasks(
            @Nonnull Table<? extends Record> table, @Nonnull RollupType rollupType,
            @Nonnull Timestamp snapshotTime, long recordCount,
            @Nonnull CompletionService<RollupTask> cs)
            throws PartitionProcessingException {
        Table<? extends Record> source = rollupType.getSourceTable(table);
        Table<? extends Record> rollup = rollupType.getRollupTable(table);
        Timestamp rollupTime = rollupType.getRollupTime(snapshotTime);
        if (dsl.dialect() == SQLDialect.POSTGRES
                || FeatureFlags.OPTIMIZE_PARTITIONING.isEnabled()) {
            partitioningManager.prepareForInsertion(rollup, rollupTime);
        }
        List<RollupTask> tasks = new ArrayList<>();
        if (source == null || rollup == null) {
            logger.warn("Source or rollup for table {} were missing", table.getName());
            return tasks;
        }
        List<Long> hiBounds = getBatchInnerBoundaries(recordCount);
        hiBounds.add(-1L);
        long lowBound = 0L;
        for (long highBound : hiBounds) {
            // we shift our non-negative bounds left one bit to occupy the sign bit, so we'll get
            // hex strings in the full range rather than missing everything starting with 8-f
            String low = lowBound > 0 ? String.format("%016x", lowBound << 1) : null;
            String high = highBound > 0 ? String.format("%016x", highBound << 1) : null;
            Query upsert = new EntityStatsRollups(
                    source, rollup, snapshotTime, rollupTime, rollupType, low, high, unpooledDsl)
                    .getUpsert();
            RollupTask task = new RollupTask(table, upsert, cs);
            tasks.add(task);
            lowBound = highBound;
        }
        return tasks;
    }

    private List<RollupTask> scheduleMarketStatsRollupTask(
            @Nonnull Table<? extends Record> table, @Nonnull RollupType rollupType,
            @Nonnull Timestamp snapshotTime, @Nonnull CompletionService<RollupTask> cs) {
        Table<? extends Record> source = rollupType.getSourceTable(table);
        Table<? extends Record> rollup = rollupType.getRollupTable(table);
        Timestamp rollupTime = rollupType.getRollupTime(snapshotTime);
        Query upsert = FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled()
                       ? new MarketStatsRollups(source, rollup, snapshotTime, rollupTime)
                               .getQuery(unpooledDsl)
                       : unpooledDsl.query(String.format("CALL %s('%s')", MARKET_ROLLUP_PROC,
                               MARKET_TABLE_PREFIX));
        return Collections.singletonList(new RollupTask(table, upsert, cs));
    }

    private List<RollupTask> scheduleClusterStatsRollupTasks(
            @Nonnull Table<? extends Record> table, @Nonnull RollupType rollupType,
            @Nonnull Timestamp snapshotTime, @Nonnull CompletionService<RollupTask> cs) {
        Table<? extends Record> source = rollupType.getSourceTable(table);
        Table<? extends Record> rollup = rollupType.getRollupTable(table);
        Timestamp rollupTime = rollupType.getRollupTime(snapshotTime);
        Query upsert = new ClusterStatsRollups(source, rollup, snapshotTime, rollupTime)
                .getQuery(unpooledDsl);
        return Collections.singletonList(new RollupTask(table, upsert, cs));
    }

    private RollupOutcome waitForRollupTasks(final CompletionService<RollupTask> cs,
            final Map<Future<RollupTask>, RollupTask> futures, final RollupType rollupType,
            Map<Table<?>, Long> tableCounts, final MultiStageTimer timer) {
        Map<Table<?>, Pair<AtomicInteger, AtomicReference<Duration>>> tableStats = new HashMap<>();
        long timeoutMsec = Math.max(minTimeoutMsec, timeoutMsecForBatchSize * futures.size());
        long deadline = System.currentTimeMillis() + timeoutMsec;
        boolean lastChancePhase = false;
        RollupOutcome outcome = RollupOutcome.COMPLETE;
        Deque<RollupTask> tasksForFinalRetry = new ArrayDeque<>();

        while (!futures.isEmpty() || !tasksForFinalRetry.isEmpty()) {
            long msecToWait = deadline - System.currentTimeMillis();
            Future<RollupTask> future;
            RollupTask task = null;
            if (futures.isEmpty()) {
                // must be doing final serial retries of upserts that failed all normal retries
                // force one last retry of this task
                lastChancePhase = true;
                RollupTask lastChanceTask = tasksForFinalRetry.removeFirst();
                futures.put(lastChanceTask.retry(cs, true).get(), lastChanceTask);
            }
            try {
                future = cs.poll(msecToWait, TimeUnit.MILLISECONDS);
                if (Thread.currentThread().isInterrupted()) {
                    // in case we get interrupted when we're not waiting for something, we'll
                    // notice it here
                    throw new InterruptedException();
                }
                if (future == null) {
                    // the poll operation above does not throw like future.get does, so we
                    // do it here for uniform handling.
                    throw new TimeoutException();
                }
                task = futures.get(future);
                futures.remove(future);
                // following shouldn't ever time out since we got it from the poll operation
                // above, but for safety we'll still put a time limit on it
                RollupTask taskFromFuture = future.get(msecToWait, TimeUnit.MILLISECONDS);
                if (task == null || task != taskFromFuture) {
                    logger.warn("Unexpected rollup task associated with future; "
                                    + "discarding results ({} records for table {} in {})",
                            taskFromFuture.getAffectedRecordCount(),
                            taskFromFuture.getTable().getName(),
                            taskFromFuture.getProcessingTime());
                } else {
                    Table<?> table = task.getTable();
                    Pair<AtomicInteger, AtomicReference<Duration>> statsPair =
                            tableStats.computeIfAbsent(table, _t -> Pair.of(
                                    new AtomicInteger(0),
                                    new AtomicReference<>(Duration.ZERO)));
                    logger.debug(
                            "Processed chunk for table {}: {} records in {}, {} batches remaining",
                            task.getTable().getName(), task.getAffectedRecordCount(),
                            task.getProcessingTime(), futures.size());
                    statsPair.getLeft().addAndGet(task.getAffectedRecordCount());
                    statsPair.getRight().set(
                            statsPair.getRight().get().plus(task.getProcessingTime()));
                }
            } catch (InterruptedException | CancellationException e) {
                // it appears we're shutting down... cancel remaining task. This will attempt
                // to cancel any in-progress DB operations as well.
                cancelRemainingTasks(futures);
                outcome = RollupOutcome.INTERRUPTED;
                Thread.currentThread().interrupt();
            } catch (ExecutionException e) {
                if (task != null) {
                    Optional<Future<RollupTask>> retryFuture = task.retry(cs, false);
                    if (retryFuture.isPresent()) {
                        futures.put(retryFuture.get(), task);
                        logger.warn("Rollup task {} failed and will be retried: {}",
                                task, e.getMessage());
                    } else if (lastChancePhase) {
                        logger.error("Rollup task {} failed and ran out of retries, including "
                                + "last-chance serial retry", task, e);
                        outcome = RollupOutcome.FAILED;
                    } else {
                        logger.warn("Rollup task {} failed and ran out of retries; "
                                        + "will be held for final attempt at serial execution: {}",
                                task, e.getMessage());
                        tasksForFinalRetry.add(task);
                    }
                } else {
                    logger.error("A future for a rollup task failed "
                            + "but was unassociated with a task", e);
                }
            } catch (TimeoutException e) {
                logger.error("Rollup operation took too long; canceling {} remaining tasks",
                        futures.size());
                cancelRemainingTasks(futures);
                outcome = RollupOutcome.TIMED_OUT;
            }
        }
        tableStats.keySet().stream().sorted(Comparator.comparing(Table::getName)).forEach(table -> {
            Pair<AtomicInteger, AtomicReference<Duration>> statPair = tableStats.get(table);
            int recordCount = statPair.getLeft().get();
            if (recordCount == 0 && table.getClass() != MarketStatsLatest.class) {
                logger.error("Reported upsert record count for table {} is zero,"
                                + " which probably means there was a problem with the upsert",
                        table.getName());
            } else {
                String countsDesc = getCountsDescription(
                        recordCount, tableCounts.get(table));
                timer.addSegment(
                        String.format("%s %s (%s)", rollupType, table.getName(), countsDesc),
                        statPair.getRight().get());
            }
        });
        return outcome;
    }

    private void cancelRemainingTasks(Map<Future<RollupTask>, RollupTask> futures) {
        futures.values().forEach(RollupTask::cancel);
        futures.clear();
    }

    private String getCountsDescription(int affectedCount, long incomingCount) {
        if (unpooledDsl.dialect() == SQLDialect.POSTGRES) {
            // can't curently distinguish inserts form updates in postgres
            return String.format("%d records", affectedCount);
        } else {
            // The record counts for a MariaDB upsert statement always count 1 for an insert, 2 for
            // an update to an existing record, and 0 for an existing record that participated but
            // was not changed. We have none of the latter because `samples` field is always changed.
            // So we can unambiguously obtain separate counts
            int updates = affectedCount - (int)incomingCount;
            int inserts = (int)incomingCount - updates;
            return String.format("%d inserts, %d updates", inserts, updates);
        }
    }

    private List<Long> getBatchInnerBoundaries(Long recordCount) {
        List<Long> boundaries = new ArrayList<>();
        recordCount = recordCount != null ? recordCount : MISSING_RECORD_COUNT;
        long batchCount =
                recordCount / maxBatchSize + (recordCount % maxBatchSize == 0 ? 0 : 1);
        long step = Long.MAX_VALUE / batchCount + (Long.MAX_VALUE % batchCount == 0 ? 0 : 1);
        long priorBoundary = 0L;
        // x >= 0 ensures we terminate the loop if step*batchCount > Long.MAX_VALUE, which will
        // almost always be true
        for (long boundary = step;
             boundary < Long.MAX_VALUE && boundary >= 0; boundary += step) {
            if (boundary > priorBoundary) {
                boundaries.add(boundary);
                priorBoundary = boundary;
            }
        }
        return boundaries;
    }

    /**
     * Enum of the types of rollup we perform.
     */
    public enum RollupType {
        /** hourly rollups. */
        BY_HOUR(TimeFrame.HOUR, "Hourly Rollups", RetentionPolicy.HOURLY_STATS),
        /** daily rollups. */
        BY_DAY(TimeFrame.DAY, "Daily Rollups", RetentionPolicy.DAILY_STATS),
        /** monthly rollups. */
        BY_MONTH(TimeFrame.MONTH, "Monthly Rollups", RetentionPolicy.MONTHLY_STATS);

        private final TimeFrame timeFrame;
        private final String label;
        private final RetentionPolicy retentionPolicy;

        RollupType(TimeFrame timeFrame, String label, RetentionPolicy retentionPolicy) {
            this.timeFrame = timeFrame;
            this.label = label;
            this.retentionPolicy = retentionPolicy;
        }

        /**
         * Determine whether the given table can be rolled up.
         *
         * @param table the table
         * @return true if the table can be rolled up
         */
        public boolean canRollup(Table<? extends Record> table) {
            if (table == MARKET_STATS_LATEST) {
                // market stats participates in all rollups except in legacy scenario, where it
                // only participates in hourly (the legacy stored proc actually does all three)
                return FeatureFlags.POSTGRES_PRIMARY_DB.isEnabled() || this == BY_HOUR;
            } else if (table == CLUSTER_STATS_LATEST) {
                // cluster stats participates in all rollups
                return true;
            } else {
                // stats tables for entity types marked for rollup participate in all rollups
                return EntityType.fromTable(table).map(EntityType::rollsUp).orElse(false);
            }
        }

        /**
         * Get the source table for rollups of this type given a representative stats table.
         *
         * <p>Hourly rollups work on _latest tables, while daily and monthly rollups work on hourly
         * rollup tables.</p>
         *
         * @param table a stats table for the entity type being rolled up
         * @return the stats table that should serve as the source for rollup data
         */
        public Table<? extends Record> getSourceTable(Table<? extends Record> table) {
            if (table == MARKET_STATS_LATEST) {
                return this == BY_HOUR ? table : MARKET_STATS_BY_HOUR;
            } else if (table == CLUSTER_STATS_LATEST) {
                return this == BY_HOUR ? table : CLUSTER_STATS_BY_HOUR;
            } else {
                EntityType type = EntityType.fromTable(table).orElse(null);
                if (type != null) {
                    return this == BY_HOUR ? type.getLatestTable().orElse(null)
                                           : type.getHourTable().orElse(null);
                }
                return null;
            }
        }

        /**
         * Get the target rollup table for this rollup type given a representative stats table.
         *
         * @param table a stats table for the entity type being rolled up
         * @return the stats table that should be updated for this rollup
         */
        public Table<? extends Record> getRollupTable(Table<? extends Record> table) {
            if (table == MARKET_STATS_LATEST) {
                switch (this) {
                    case BY_HOUR:
                        return MARKET_STATS_BY_HOUR;
                    case BY_DAY:
                        return MARKET_STATS_BY_DAY;
                    case BY_MONTH:
                        return MARKET_STATS_BY_MONTH;
                    default:
                        badValue();
                }
            } else {
                EntityType type = EntityType.fromTable(table).orElse(null);
                if (type != null) {
                    return type.getTimeFrameTable(timeFrame).orElse(null);
                }
            }
            return null;
        }

        /**
         * Get the rollup time (i.e. the snapshot_time column value) for records in the target table
         * for this rollup.
         *
         * @param snapshotTime snapshot time of records in source table
         * @return snapshot time of records in rollup table
         */
        public Timestamp getRollupTime(Timestamp snapshotTime) {
            Instant t = snapshotTime.toInstant();
            switch (this) {
                case BY_HOUR:
                    return Timestamp.from(t.truncatedTo(ChronoUnit.HOURS));
                case BY_DAY:
                    return Timestamp.from(t.truncatedTo((ChronoUnit.DAYS)));
                case BY_MONTH:
                    OffsetDateTime rollupTime = OffsetDateTime.ofInstant(t, ZoneOffset.UTC)
                            .truncatedTo(ChronoUnit.DAYS)
                            .withDayOfMonth(1)
                            .plusMonths(1)
                            .minusDays(1);
                    return Timestamp.from(rollupTime.toInstant());
                default:
                    badValue();
                    return null;
            }
        }

        /**
         * Get the start time of the rollup period covered by this rollup.
         *
         * @param snapshotTime timestamp of data being rolled up
         * @return rollup period start
         */
        public Timestamp getPeriodStart(Timestamp snapshotTime) {
            switch (this) {
                case BY_HOUR:
                case BY_DAY:
                    return getRollupTime(snapshotTime);
                case BY_MONTH:
                    LocalDateTime periodStart = LocalDateTime.ofInstant(
                                    snapshotTime.toInstant(),
                                    ZoneOffset.UTC)
                            .truncatedTo(ChronoUnit.DAYS)
                            .withDayOfMonth(1);
                    return Timestamp.from(periodStart.toInstant(ZoneOffset.UTC));
                default:
                    badValue();
                    return null;
            }
        }

        /**
         * Get the end time of the rollup period covered by this rollup.
         *
         * @param snapshotTime timestamp of data being rolled up
         * @return rollup period end
         */
        public Timestamp getPeriodEnd(Timestamp snapshotTime) {
            Instant t = snapshotTime.toInstant();
            switch (this) {
                case BY_HOUR:
                    return Timestamp.from(
                            t.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS));
                case BY_DAY:
                    return Timestamp.from(
                            t.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS));
                case BY_MONTH:
                    // rollup time is start of final day in month, so we add one day to get start
                    // of first day of next month
                    return Timestamp.from(getRollupTime(snapshotTime).toInstant()
                            .plus(1, ChronoUnit.DAYS));
                default:
                    badValue();
                    return null;
            }
        }

        /**
         * Get a label for this rollup type, for use in logging.
         *
         * @return rollup type label
         */
        public String getLabel() {
            return label;
        }

        public TimeFrame getTimeFrame() {
            return timeFrame;
        }

        public RetentionPolicy getRetentionPolicy() {
            return retentionPolicy;
        }

        private void badValue() {
            throw new IllegalStateException(
                    String.format("Unknown RollupType value: %s", this));
        }
    }

    /**
     * Class to represent a task to perform a single upsert operation during a rollup operation.
     */
    private class RollupTask {

        private final Table<? extends Record> table;
        private final CompletionService<RollupTask>
                cs;
        private Future<RollupTask> future = null;
        private Query upsert;
        private final AtomicInteger affectedRecordCount = new AtomicInteger(0);
        private final AtomicReference<Duration> processingTime = new AtomicReference<>();
        private int retryCount = 0;

        /**
         * Create a new instance.
         *
         * @param table  source table of rollup operation
         * @param upsert jOOQ query to perform
         * @param cs     completion service through which to schedule the task for execution
         */
        RollupTask(Table<? extends Record> table, Query upsert,
                CompletionService<RollupTask> cs) {
            this.table = table;
            this.upsert = upsert;
            this.cs = cs;
        }

        /**
         * Submit a task for execution that will perform an upsert operation as part of an overall
         * rollup.
         *
         * @return a {@link Future} that should resolve to this RollupTask instance
         */
        Future<RollupTask> submit() {
            synchronized (this) {
                Query queryToSubmit = unpooledDsl.dialect() == SQLDialect.POSTGRES
                                      ? wrapPostgresUpsert() : upsert;
                this.future = cs.submit(() -> {
                    Instant start = Instant.now();
                    try {
                        int n = upsert instanceof ResultQuery
                                // Note: currently, wrapping of postgres upserts is suppressed,
                                // so this first branch will never occur. But eventually...
                                // for a Postgres upsert we've just added a RETURNING clause at the
                                // end of the UPSERT so that it now returns a set of ones and zeros
                                // indicating whether input records were inserted or updated by the
                                // UPSERT, respectively. We sum them to get the # of inserts
                                ? ((ResultQuery<? extends Record>)queryToSubmit).stream()
                                        .map(r -> r.getValue(0))
                                        .filter(v -> v instanceof Integer)
                                        .map(v -> (Integer)v)
                                        .mapToInt(Integer::intValue)
                                        .sum()
                                // for MariaDB it's just a straight UPSERT whose affected record
                                // count is INSERT count + 2*(UPDATE count), which we can combine
                                // with total record count to deduce the two separate counts
                                : unpooledDsl.execute(queryToSubmit);
                        affectedRecordCount.addAndGet(n);
                        return this;
                    } finally {
                        processingTime.set(Duration.between(start, Instant.now()));
                    }
                });
                return future;
            }
        }

        public Optional<Future<RollupTask>> retry(CompletionService<RollupTask> cs, boolean force) {
            if (force || retryCount++ < maxBatchRetry) {
                return Optional.of(this.submit());
            } else {
                return Optional.empty();
            }
        }

        /**
         * The goal of this method is to wrap a normal UPSERT statement as needed to result in a
         * statement whose results can be used to obtain a single integer which, in combination with
         * the total record count, can be used to deduce separate insert and update counts. How to
         * accopmlish this depends on the database dialect.
         *
         * @return record count for this operation, with dialet-dependent meaning
         */
        private Query wrapPostgresUpsert() {
            if (upsert instanceof InsertReturningStep
                    // frustratingly, this mechanism does not work when the target of the insert
                    // is a partitioned table. For now we'll live with the combined (inserts plus
                    // updates) count returned from an UPSERT execution.
                    // TODO make this work or just forget trying to report inserts & updates
                    && false) {
                // See https://stackoverflow.com/questions/39058213/postgresql-upsert-differentiate-inserted-and-updated-rows-using-system-columns-x
                // for a detailed explanation of how the `xmax` system column can be used to
                // distinguish inserts from updates in a Postgres upsert operation
                Field<String> xmax =
                        DSL.cast(DSL.field("xmax", Long.class), SQLDataType.CLOB(10));
                InsertReturningStep<?> insert = (InsertReturningStep<?>)upsert;
                return insert.returning(DSL.case_()
                        .when(xmax.eq("0"), 1)
                        .else_(0));
            } else {
                return upsert;
            }
        }

        Table<? extends Record> getTable() {
            return table;
        }

        public int getAffectedRecordCount() {
            return affectedRecordCount.get();
        }

        public Duration getProcessingTime() {
            Duration duration = processingTime.get();
            return duration != null ? duration : Duration.ZERO;
        }

        synchronized void cancel() {
            if (upsert != null) {
                try {
                    upsert.cancel();
                } catch (DataAccessException e) {
                    logger.error("Failed to cancel a rollup task for table {}",
                            table.getName(), e);
                }
            }
            if (future != null) {
                future.cancel(true);
            }
        }
    }

    /**
     * Possible outcomes of overall rollup opeartions.
     */
    public enum RollupOutcome {
        /** All rollup data was successfully applied. */
        COMPLETE,
        /** Some of the rollup data was not applied, despite retry attempts. */
        FAILED,
        /** The rollup operation was interrupted; any unprocessed data was abandoned. */
        INTERRUPTED,
        /** The rollup operation took too long; any unprocessed data was abandoned. */
        TIMED_OUT;

        public boolean isComplete() {
            return this == COMPLETE;
        }
    }
}
