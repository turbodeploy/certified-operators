package com.vmturbo.history.listeners;

import static com.vmturbo.history.schema.abstraction.Tables.AVAILABLE_TIMESTAMPS;
import static org.jooq.impl.DSL.exists;
import static org.jooq.impl.DSL.inline;
import static org.jooq.impl.DSL.selectFrom;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.RetentionPolicy;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.abstraction.routines.EntityStatsRollup;
import com.vmturbo.history.schema.abstraction.routines.MarketAggregate;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByDay;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsByMonth;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.utils.MultiStageTimer;
import com.vmturbo.history.utils.MultiStageTimer.AsyncTimer;
import com.vmturbo.history.utils.MultiStageTimer.Detail;

/**
 * This class performs rollups of stats entity and market stats data in the database, and it can
 * also be used to repartition the tables, as an efficient means of deleting expired data.
 */
public class RollupProcessor {
    private static final Logger logger = LogManager.getLogger(RollupProcessor.class);

    // maximum time we'll wait for a rollup task (for a single entity-type shard) to complete
    private static final long ROLLUP_TIMEOUT_MINS = 20;
    // maximum time we'll wait for a repartitioning task (for a single table) to complete
    private static final long REPARTITION_TIMEOUT_SECS = 60;

    // various strings related to stats related tables and stored procs
    private static final String MARKET_TABLE_PREFIX = "market";
    private static final String MARKET_ROLLUP_PROC = new MarketAggregate().getName();
    private static final String ENTITY_ROLLUP_PROC = new EntityStatsRollup().getName();

    private final HistorydbIO historydbIO;
    private final ExecutorService executorService;

    /**
     * Create a new instance.
     *
     * @param historydbIO     DB utilities
     * @param executorService thread pool to run individual tasks
     */
    public RollupProcessor(HistorydbIO historydbIO, ExecutorService executorService) {
        this.historydbIO = historydbIO;
        this.executorService = executorService;
    }

    /**
     * Perform hourly rollup processing for all tables that were active in ingestions that
     * were performed for the given snapshot time.
     *
     * @param tables       tables that were populated by ingestions for the given snapshot
     * @param msecSnapshot snapshot time of contributing ingestions
     */

    void performHourRollups(@Nonnull List<Table> tables, @Nonnull Instant msecSnapshot) {
        Timestamp snapshot = Timestamp.from(msecSnapshot.truncatedTo(ChronoUnit.SECONDS));
        MultiStageTimer timer = new MultiStageTimer(logger);
        performRollups(tables, snapshot, RollupType.BY_HOUR, timer);
        addAvailableTimestamps(snapshot, RollupType.BY_HOUR, HistoryVariety.ENTITY_STATS, HistoryVariety.PRICE_DATA);
        timer.stopAll().info(
                String.format("Rollup Processing for %s", snapshot), Detail.STAGE_SUMMARY);
    }

    /**
     * Perform daily and monthly rollups for all tables that were updated by ingestions for
     * snapshots falling in a single hour.
     *
     * <p>This should be done after ingestions for the hour have all completed.</p>
     *
     * @param tables       tables that participate in ingestions during the hour
     * @param msecSnapshot instant from within the hour to roll up
     */
    void performDayMonthRollups(final List<Table> tables, final Instant msecSnapshot) {
        performDayMonthRollups(tables, msecSnapshot, true);
    }

    /**
     * Perform daily and monthly rollups for all tables that were updated by ingestions for
     * snapshots falling in a single hour, and perhaps do repartioning as well.
     *
     * <p>This should be done after ingestions for the hour have all completed.</p>
     *
     * @param tables       tables that participate in ingestions during the hour
     * @param msecSnapshot instant from within the hour to roll up
     * @param reparititon  true to do repartioning after rollups
     */
    void performDayMonthRollups(final List<Table> tables, final Instant msecSnapshot, boolean reparititon) {
        Timestamp snapshot = Timestamp.from(msecSnapshot.truncatedTo(ChronoUnit.HOURS));
        MultiStageTimer timer = new MultiStageTimer(logger);
        performRollups(tables, snapshot, RollupType.BY_DAY, timer);
        addAvailableTimestamps(snapshot, RollupType.BY_DAY, HistoryVariety.ENTITY_STATS, HistoryVariety.PRICE_DATA);
        performRollups(tables, snapshot, RollupType.BY_MONTH, timer);
        addAvailableTimestamps(snapshot, RollupType.BY_MONTH, HistoryVariety.ENTITY_STATS, HistoryVariety.PRICE_DATA);
        if (reparititon) {
            performRepartitioning(timer);
        }
        timer.stopAll().info(
                String.format("Rollup Processing for %s", snapshot), Detail.STAGE_SUMMARY);
    }

    private void addAvailableTimestamps(Timestamp snapshot, RollupType rollupType, HistoryVariety... historyVarieties) {
        for (HistoryVariety historyVariety : historyVarieties) {
            addAvailableTimestamp(snapshot, rollupType, historyVariety);
        }
    }

    private void addAvailableTimestamp(Timestamp snapshot, RollupType rollupType, HistoryVariety historyVariety) {
        Timestamp rollupTime = rollupType.getRollupTime(snapshot);
        Timestamp rollupStart = rollupType.getPeriodStart(snapshot);
        Timestamp rollupEnd = rollupType.getPeriodEnd(snapshot);
        try (Connection conn = historydbIO.connection()) {
            String sql = historydbIO.using(conn)
                    .insertInto(AVAILABLE_TIMESTAMPS,
                            AVAILABLE_TIMESTAMPS.TIME_STAMP,
                            AVAILABLE_TIMESTAMPS.TIME_FRAME,
                            AVAILABLE_TIMESTAMPS.HISTORY_VARIETY,
                            AVAILABLE_TIMESTAMPS.EXPIRES_AT)
                    .select(
                            DSL.select(
                                    inline(rollupTime).as(AVAILABLE_TIMESTAMPS.TIME_STAMP),
                                    inline(rollupType.getTimeFrame().name()).as(AVAILABLE_TIMESTAMPS.TIME_FRAME),
                                    inline(historyVariety.name()).as(AVAILABLE_TIMESTAMPS.HISTORY_VARIETY),
                                    DSL.inline(Timestamp.from(
                                            rollupType.getRetentionPolicy().getExpiration(rollupTime.toInstant())))
                                            .as(AVAILABLE_TIMESTAMPS.EXPIRES_AT))
                                    .from(AVAILABLE_TIMESTAMPS)
                                    .where(exists(selectFrom(AVAILABLE_TIMESTAMPS)
                                            .where(AVAILABLE_TIMESTAMPS.TIME_STAMP.between(inline(rollupStart), inline(rollupEnd)))
                                            .and(AVAILABLE_TIMESTAMPS.HISTORY_VARIETY.eq(inline(historyVariety.name()))))
                                    )

                    ).getSQL();
            // JOOQ's onDuplicateKeyIgnore method can't currently be used with its INSERT...SELECT
            // construction. So we need to create the INSERT statement without it, and then modify
            // the generated SQL as needed to get the intended effect.
            sql = Pattern.compile("^INSERT", Pattern.CASE_INSENSITIVE).matcher(sql).replaceFirst("INSERT IGNORE");
            historydbIO.using(conn).execute(sql);
        } catch (VmtDbException | SQLException | DataAccessException e) {
            logger.error("Failed to rollup available_timestamps", e);
        }
    }

    void performRetentionProcessing(MultiStageTimer timer) {
        // retention processing for entity stats tables is done by reconfiguring their partitions so
        // that partitions containing expired records are dropped.
        performRepartitioning(timer);
        // available_timestamps is small, so we just manually delete records that exceed expiration
        timer.start("Expire available_timestamps records");
        try (Connection conn = historydbIO.connection()) {
            historydbIO.using(conn).deleteFrom(AVAILABLE_TIMESTAMPS)
                    .where(DSL.currentTimestamp().ge(AVAILABLE_TIMESTAMPS.EXPIRES_AT)).execute();
        } catch (VmtDbException | SQLException | DataAccessException e) {
            logger.error("Failed to delete expired available_timestamps records", e);
        } finally {
            timer.stop();
        }
    }

    /**
     * Perform repartitioning on all the stats tables. They all need this each hour regardless of
     * whether they participated in any ingestions.
     *
     * <p>We kick off a separate task for each table that needs to be partitioned.</p>
     *
     * @param timer timer to use
     */
    private void performRepartitioning(MultiStageTimer timer) {
        timer.start("Repartitioning");
        final Set<Table> tables = getTablesToRepartition();
        final Stream<Pair<Table, Future<Void>>> tableFutures = tables.stream()
                .map(table -> Pair.of(table, scheduleRepartition(table)));
        tableFutures.forEach(tf -> {
            try {
                tf.getRight().get(REPARTITION_TIMEOUT_SECS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (TimeoutException e) {
                logger.warn("Timed out during repartitioning of table {}; " +
                        "repartition may still complete normally", tf.getLeft());
            } catch (ExecutionException e) {
                logger.error("Error during repartitioning of table {}: {}",
                        tf.getLeft().getName(), e.toString());
            }
        });
    }

    private Set<Table> getTablesToRepartition() {
        final Set<Table> tables = new HashSet<>();
        tables.add(MarketStatsLatest.MARKET_STATS_LATEST);
        tables.add(MarketStatsByHour.MARKET_STATS_BY_HOUR);
        tables.add(MarketStatsByDay.MARKET_STATS_BY_DAY);
        tables.add(MarketStatsByMonth.MARKET_STATS_BY_MONTH);
        EntityType.ROLLED_UP_ENTITIES.forEach(e -> {
            List<String> missing = new ArrayList();
            if (e.getLatestTable() != null) {
                tables.add(e.getLatestTable());
            } else {
                missing.add("Latest");
            }
            if (e.getLatestTable() != null) {
                tables.add(e.getHourTable());
            } else {
                missing.add("Hourly");
            }
            if (e.getLatestTable() != null) {
                tables.add(e.getDayTable());
            } else {
                missing.add("Daily");
            }
            if (e.getLatestTable() != null) {
                tables.add(e.getMonthTable());
            } else {
                missing.add("Monthly");
            }
            if (!missing.isEmpty()) {
                logger.warn("Not rolling up entity type {}, which is missing tables {}",
                        e.name(), missing);
            }
        });
        return tables;
    }

    private Future<Void> scheduleRepartition(Table<?> table) {
        return executorService.submit(() -> {
            try (Connection conn = historydbIO.unpooledConnection()) {
                final String sql = String.format("CALL rotate_partition('%s')", table.getName());
                historydbIO.using(conn).execute(sql);
            } catch (Exception e) {
                logger.error("Repartitioning failed for table {}: {}", table.getName(), e.toString());
            }
            return null;
        });
    }

    private void performRollups(@Nonnull final List<Table> tables,
            final Timestamp snapshot,
            final RollupType rollupType,
            @Nonnull MultiStageTimer timer) {
        timer.start(rollupType.getLabel() + " Prep");
        // schedule all the task for execution in the thread pool
        final List<Pair<Table, List<Future<Void>>>> tableFutures = tables.stream()
                .map(t -> Pair.of(t, scheduleRollupTasks(t, rollupType, snapshot, 8)))
                .collect(Collectors.toList());
        timer.stop();
        // now wait for each one to complete, and then we're done
        waitForRollupTasks(tableFutures, rollupType, timer);
    }

    private List<Future<Void>> scheduleRollupTasks(@Nonnull Table table,
            @Nonnull RollupType rollupType,
            @Nonnull Timestamp snapshotTime,
            int numTasks) {
        if (table == MarketStatsLatest.MARKET_STATS_LATEST) {
            return scheduleMarketRollupTask();
        }
        List<Future<Void>> futures = new ArrayList<>();
        if (rollupType.canRollup(table)) {
            int lowBound = 0;
            for (Integer highBound : getBoundaries(numTasks)) {
                Table source = rollupType.getSourceTable(table);
                Table rollup = rollupType.getRollupTable(table);
                String low = lowBound > 0 ? String.format("'%x'", lowBound) : null;
                String high = highBound < 16 ? String.format("'%x'", highBound) : null;
                Timestamp rollupTime = rollupType.getRollupTime(snapshotTime);
                String sql = String.format(
                        "CALL %s('%s', '%s', '%s', '%s', %s, %s, %d, %d, %d, %d, @count)",
                        ENTITY_ROLLUP_PROC, source.getName(), rollup.getName(), snapshotTime, rollupTime,
                        low, high,
                        rollupType.isCopyHourKey() ? 1 : 0,
                        rollupType.isCopyDayKey() ? 1 : 0,
                        rollupType.isCopyMonthKey() ? 1 : 0,
                        rollupType.sourceHasSamples() ? 1 : 0);
                futures.add(executorService.submit(() -> {
                    try (Connection conn = historydbIO.unpooledTransConnection()) {
                        conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                        historydbIO.using(conn).execute(sql);
                        conn.commit();
                    }
                    return null;
                }));
                lowBound = highBound;
            }
        }
        return futures;
    }

    private List<Future<Void>> scheduleMarketRollupTask() {
        String sql = String.format("CALL %s('%s')", MARKET_ROLLUP_PROC, MARKET_TABLE_PREFIX);
        final Future<Void> future = executorService.submit(() -> {
            try (Connection conn = historydbIO.unpooledTransConnection()) {
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                historydbIO.using(conn).execute(sql);
                conn.commit();
            }
            return null;
        });
        return Collections.singletonList(future);
    }

    private void waitForRollupTasks(final List<Pair<Table, List<Future<Void>>>> tableFutures,
            RollupType rollupType,
            final MultiStageTimer timer) {
        tableFutures.forEach(tf -> {
            String label = String.format(
                    "%s %s", getTablePrefix(tf.getLeft()).get(), rollupType.getLabel());
            try (AsyncTimer tableTimer = timer.async(label)) {
                tf.getRight().forEach(f -> {
                    try {
                        f.get(ROLLUP_TIMEOUT_MINS, TimeUnit.MINUTES);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (TimeoutException e) {
                        logger.warn("Timed out during rollup activity for table {}; " +
                                "rollup may still complete normally", tf.getLeft());
                    } catch (ExecutionException e) {
                        logger.error("Error during rollup activity for table {}: {}",
                                tf.getLeft().getName(), e.toString());
                    }
                });
            }
        });
    }

    private List<Integer> getBoundaries(int numTasks) {
        List<Integer> boundaries = new ArrayList<>();
        double step = 16.0 / numTasks;
        int priorBoundary = 0;
        for (double x = step; x < 16.0; x += step) {
            int boundary = (int)x;
            if (boundary > priorBoundary) {
                boundaries.add(boundary);
                priorBoundary = boundary;
            }
        }
        boundaries.add(16);
        return boundaries;
    }

    /**
     * Enum of the types of rollup we perform.
     */
    enum RollupType {
        BY_HOUR(TimeFrame.HOUR, "Hourly Rollups", RetentionPolicy.HOURLY_STATS),
        BY_DAY(TimeFrame.DAY, "Daily Rollups", RetentionPolicy.DAILY_STATS),
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
        public boolean canRollup(Table table) {
            if (table == MarketStatsLatest.MARKET_STATS_LATEST) {
                return this == BY_HOUR;
            } else {
                return EntityType.ROLLED_UP_ENTITIES.contains(EntityType.fromTable(table));
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
        public Table getSourceTable(Table table) {
            if (table == MarketStatsLatest.MARKET_STATS_LATEST) {
                return table;
            } else {
                EntityType type = EntityType.fromTable(table);
                if (type != null) {
                    return this == BY_HOUR ? type.getLatestTable() : type.getHourTable();
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
        public Table getRollupTable(Table table) {
            EntityType type = EntityType.fromTable(table);
            if (type != null) {
                return type.getTimeFrameTable(timeFrame);
            }
            return null;
        }

        /**
         * Get the rollup time (i.e. the snapshot_time column value) for records in the target
         * table for this rollup.
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
                    LocalDateTime rollupTime = LocalDateTime.ofInstant(t, ZoneOffset.UTC)
                            .truncatedTo(ChronoUnit.DAYS)
                            .withDayOfMonth(1)
                            .plusMonths(1)
                            .minusDays(1);
                    return Timestamp.from(rollupTime.toInstant(ZoneOffset.UTC));
                default:
                    badValue();
                    return null;
            }
        }

        public Timestamp getPeriodStart(Timestamp snapshotTime) {
            switch (this) {
                case BY_HOUR:
                case BY_DAY:
                    return getRollupTime(snapshotTime);
                case BY_MONTH:
                    LocalDateTime periodStart = LocalDateTime.ofInstant(snapshotTime.toInstant(), ZoneOffset.UTC)
                            .truncatedTo(ChronoUnit.DAYS)
                            .withDayOfMonth(1);
                    return Timestamp.from(periodStart.toInstant(ZoneOffset.UTC));
                default:
                    badValue();
                    return null;
            }
        }

        public Timestamp getPeriodEnd(Timestamp snapshotTime) {
            Instant t = snapshotTime.toInstant();
            switch (this) {
                case BY_HOUR:
                    return Timestamp.from(t.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS));
                case BY_DAY:
                    return Timestamp.from(t.truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.DAYS));
                case BY_MONTH:
                    return Timestamp.from(getRollupTime(snapshotTime).toInstant().plus(1, ChronoUnit.DAYS));
                default:
                    badValue();
                    return null;
            }
        }

        /**
         * Determine whether, for this rollup type, the hour key should be copied to the target
         * table.
         *
         * @return true to copy the hour key
         */
        public boolean isCopyHourKey() {
            return this == BY_HOUR;
        }

        /**
         * Determine whether, for this rollup type, the day key should be copied to the target
         * table.
         *
         * @return true to copy the day key
         */
        public boolean isCopyDayKey() {
            return this == BY_HOUR || this == BY_DAY;
        }

        /**
         * Determine whether, for this rollup type, the month key should be copied ot the target
         * table.
         *
         * @return true to copy the month key
         */
        public boolean isCopyMonthKey() {
            return true;
        }

        /**
         * Determine whether the source table for this rollup has a "samples" column.
         *
         * @return true if the source table has a "samples" column
         */
        public boolean sourceHasSamples() {
            return this != BY_HOUR;
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
     * Return the table prefix for the given table.
     *
     * @param table the table
     * @return its prefix, if it's subject to rollups
     */
    private Optional<String> getTablePrefix(@Nonnull Table<?> table) {
        EntityType entityType = EntityType.fromTable(table);
        if (entityType != null) {
            // it's an entity table - get its prefix from reference data map
            return Optional.of(entityType.getTblPrfx());
        } else if (table == MarketStatsLatest.MARKET_STATS_LATEST) {
            // market stats uses 'market'
            return Optional.of(MARKET_TABLE_PREFIX);
        } else {
            // anything else we don't recognize
            return Optional.empty();
        }
    }
}
