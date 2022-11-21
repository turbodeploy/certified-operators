package com.vmturbo.action.orchestrator.stats;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMaps;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.Batch;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.tables.ActionSnapshotLatest;
import com.vmturbo.action.orchestrator.db.tables.records.ActionSnapshotLatestRecord;
import com.vmturbo.action.orchestrator.db.tables.records.ActionStatsLatestRecord;
import com.vmturbo.action.orchestrator.stats.StatsActionViewFactory.StatsActionView;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory;
import com.vmturbo.action.orchestrator.stats.aggregator.ActionAggregatorFactory.ActionAggregator;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroup.ActionGroupKey;
import com.vmturbo.action.orchestrator.stats.groups.ActionGroupStore;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroup.MgmtUnitSubgroupKey;
import com.vmturbo.action.orchestrator.stats.groups.MgmtUnitSubgroupStore;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler;
import com.vmturbo.action.orchestrator.stats.rollup.IActionStatRollupScheduler;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Drives the action stat aggregation framework.
 * <p>
 * Responsible for processing {@link ActionView}s populated in the {@link LiveActionStore},
 * using various {@link ActionAggregator}s to collect the stats into database records, and saving
 * the aggregated records to the database.
 */
public class LiveActionsStatistician {
    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final int batchSize;

    private final ActionGroupStore actionGroupStore;

    private final MgmtUnitSubgroupStore mgmtUnitSubgroupStore;

    private final StatsActionViewFactory snapshotFactory;

    private final Clock clock;

    private final List<ActionAggregatorFactory<? extends ActionAggregator>> aggregatorFactories;

    private final IActionStatRollupScheduler actionStatRollupScheduler;

    private final ActionStatCleanupScheduler actionStatCleanupScheduler;

    private PreviousBroadcastActions previousBroadcastActions;

    public LiveActionsStatistician(@Nonnull final DSLContext dsl,
            final int batchSize,
            @Nonnull final ActionGroupStore actionGroupStore,
            @Nonnull final MgmtUnitSubgroupStore mgmtUnitSubgroupStore,
            @Nonnull final StatsActionViewFactory snapshotFactory,
            @Nonnull final List<ActionAggregatorFactory<? extends ActionAggregator>> aggregatorFactories,
            @Nonnull final Clock clock,
            @Nonnull final IActionStatRollupScheduler rollupScheduler,
            @Nonnull final ActionStatCleanupScheduler cleanupScheduler) {
        this.dslContext = Objects.requireNonNull(dsl);
        this.batchSize = batchSize;
        this.actionGroupStore = Objects.requireNonNull(actionGroupStore);
        this.mgmtUnitSubgroupStore = Objects.requireNonNull(mgmtUnitSubgroupStore);
        this.snapshotFactory = Objects.requireNonNull(snapshotFactory);
        this.clock = Objects.requireNonNull(clock);
        this.aggregatorFactories = Objects.requireNonNull(aggregatorFactories);
        this.actionStatRollupScheduler = Objects.requireNonNull(rollupScheduler);
        this.actionStatCleanupScheduler = Objects.requireNonNull(cleanupScheduler);
        previousBroadcastActions = new PreviousBroadcastActions();
    }

    /**
     * Record the action stats of actions contained in the input into the database.
     * This method may take a long time - it involves calls to other components as well as calls
     * to the database.
     *
     * (roman, Nov 15 2018): It may be worth it to make it asynchronous, but at the time of
     * this writing there's no clear benefit. This is called after populating the actions in the
     * {@link LiveActionStore}, and there are no downstream operations being blocked - once the
     * actions are populated, external API calls can see them.
     *
     * @param actionStream A stream of actions representing a snapshot of actions in a
     *        {@link LiveActionStore} at a particular point in time.
     */
    public void recordActionStats(@Nonnull final TopologyInfo topologyInfo,
                                  @Nonnull final Stream<ActionView> actionStream) {
        if (topologyInfo.getTopologyType() != TopologyType.REALTIME) {
            throw new IllegalArgumentException("Attempting to insert non-realtime topology info " +
                "stats into the actions statistician: " + topologyInfo);
        }

        try {
            internalRecordActionStats(topologyInfo, actionStream);
        } catch (RuntimeException e) {
            logger.error("Failed to record action stats due to error!", e);
        }
    }

    private void internalRecordActionStats(final TopologyInfo sourceTopologyInfo,
                                           @Nonnull final Stream<ActionView> actionStream) {
        final LocalDateTime topologyCreationTime = LocalDateTime.ofInstant(
            Instant.ofEpochMilli(sourceTopologyInfo.getCreationTime()),
            clock.getZone());

        // We snapshot the actions so that subsequent changes to state don't affect the
        // current snapshot.

        final ImmutableLiveActionsSnapshot.Builder snapshotBuilder =
                ImmutableLiveActionsSnapshot.builder()
                    .actionSnapshotTime(LocalDateTime.now(clock))
                    .topologyCreationTime(topologyCreationTime)
                    .topologyId(sourceTopologyInfo.getTopologyId());
        //
        // We translate the actions, because we need to record actions "as the user sees them". The
        // user always sees translated actions, so if any changes occur during translation - for
        // example, dropping invalid actions - the counts should reflect them.
        try (DataMetricTimer timer = Metrics.ACTION_STAT_RECORD_SNAPSHOT_TIME.startTimer()) {
            actionStream.map(actionView -> {
                try {
                    return snapshotFactory.newStatsActionView(actionView);
                } catch (UnsupportedActionException e) {
                    logger.error("Attempting to record stats for unsupported action: "
                        + e.getLocalizedMessage());
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .forEach(snapshotBuilder::addActions);
        }

        // We build the snapshot before running the aggregators because the aggregators can take
        // some time, and we don't want action state changes during aggregation to affect the
        // aggregated stats.
        final LiveActionsSnapshot snapshot = snapshotBuilder.build();

        // Create the aggregators.
        final List<ActionAggregator> aggregators = aggregatorFactories.stream()
            .map(factory -> factory.newAggregator(snapshot.topologyCreationTime()))
            .collect(Collectors.toList());

        logger.info("Running {} aggregators on {} actions",
                aggregators.size(), snapshot.actions().size());

        try (DataMetricTimer timer = Metrics.ACTION_STAT_RECORD_AGGREGATE_TIME.startTimer()) {
            // Aggregate the actions.
            final List<ActionAggregator> startedAggregators = new ArrayList<>(aggregators.size());
            aggregators.forEach(aggregator -> {
                try {
                    aggregator.start();
                    startedAggregators.add(aggregator);
                } catch (RuntimeException e) {
                    logger.error("Failed to start aggregator {}. Error: {}",
                        aggregator, e.getLocalizedMessage());
                }
            });

            final Map<ActionAggregator, Integer> aggregatorErrorCounts = new HashMap<>();
            snapshot.actions().forEach(action -> {
                startedAggregators.forEach(startedAggregator -> {
                    try {
                        startedAggregator.processAction(action, previousBroadcastActions);
                    } catch (RuntimeException e) {
                        logger.debug("Aggregator {} got exception when processing action." +
                                " Message: {}", startedAggregator, e.getLocalizedMessage());
                        aggregatorErrorCounts.compute(startedAggregator,
                            (k, existingCount) -> existingCount == null ? 1 : existingCount + 1);
                    }
                });
            });

            // Update the map to track the action -> action group mappings from the last broadcast.
            previousBroadcastActions.updateActions(snapshot);

            if (!aggregatorErrorCounts.isEmpty()) {
                logger.error("Some aggregators encounted the following number of errors. Turn on " +
                    "debug level logging for more detail. \n{}",
                    Joiner.on(",\n").withKeyValueSeparator(" error count = ").join(aggregatorErrorCounts));
            }
        }
        ActionSnapshotLatestRecord actionSnapshotLatestRecord = snapshot.toDbRecord();
        logger.info("Inserting TopologyId: {}, ActionSnapshotTime: {}, topologyCreationTime {} into ACTION_SNAPSHOT_LATEST", sourceTopologyInfo.getTopologyId(),
                actionSnapshotLatestRecord.getActionSnapshotTime(), topologyCreationTime);

        // Record the snapshot.
        final int modifiedRows = dslContext.insertInto(ActionSnapshotLatest.ACTION_SNAPSHOT_LATEST)
                .set(actionSnapshotLatestRecord)
                .execute();
        if (modifiedRows != 1) {
            logger.warn("{} rows (expected: 1) modified by insert of snapshot into database." +
                    " Snapshot: {}", modifiedRows, snapshot);
        }

        // Make sure the management unit subgroups and action groups we have data for exist in their
        // respective databases.
        final Map<MgmtUnitSubgroupKey, MgmtUnitSubgroup> mgmtSubgroupsByKey =
            mgmtUnitSubgroupStore.ensureExist(aggregators.stream()
                .flatMap(aggregator -> aggregator.getMgmtUnitSubgroupKeys().stream())
                .collect(Collectors.toSet()));
        final Map<ActionGroupKey, ActionGroup> actionGroupsByKey =
            actionGroupStore.ensureExist(aggregators.stream()
                .flatMap(aggregator -> aggregator.getActionGroupKeys().stream())
                .collect(Collectors.toSet()));

        try (DataMetricTimer storeTimer = Metrics.ACTION_STAT_RECORD_STORE_TIME.startTimer()) {
            // Collect the records.
            // We concatenate and partition iterators to avoid intermediate collections.
            final Iterator<ActionStatsLatestRecord> allRecordsIt = aggregators.stream()
                .flatMap(aggregator -> aggregator.createRecords(mgmtSubgroupsByKey, actionGroupsByKey))
                .iterator();
            // Record the records produced by processing the snapshot.
            final AtomicInteger recordCount = new AtomicInteger(0);
            final AtomicInteger successfulInsertions = new AtomicInteger(0);

            Iterators.partition(allRecordsIt, batchSize).forEachRemaining(batch -> {
                if (!batch.isEmpty()) {
                    Metrics.ACTION_STAT_RECORDS.increment();
                    recordCount.addAndGet(batch.size());
                    logger.debug("Writing records batch of size {}", batch.size());
                    dslContext.transaction(transactionContext -> {
                        final DSLContext transaction = DSL.using(transactionContext);
                        final Batch thisBatchInsert = transaction.batchInsert(batch);
                        int[] batchResult = thisBatchInsert.execute();
                        for (int resultIdx = 0; resultIdx < batchResult.length; ++resultIdx) {
                            if (batchResult[resultIdx] != 1) {
                                Metrics.ACTION_STAT_RECORD_ERRORS.increment();
                                logger.warn("Each statement in batch should modify 1 row. " +
                                    "Got result: {} for mgmt group {}", batchResult[resultIdx],
                                    batch.get(resultIdx).getMgmtUnitSubgroupId());
                            } else {
                                successfulInsertions.incrementAndGet();
                            }
                        }
                    });
                }
            });

            logger.info("Completed action stats aggregation of {} records. Inserted {} rows.",
                    recordCount, successfulInsertions);
            if (recordCount.get() > 0) {
                Metrics.ACTION_STAT_RECORDS.increment((double) recordCount.get());
            }
        }

        actionStatRollupScheduler.scheduleRollups();

        final int cleanupsScheduled = actionStatCleanupScheduler.scheduleCleanups();
        if (cleanupsScheduled > 0) {
            logger.info("Scheduled {} cleanups.", cleanupsScheduled);
        }
    }

    /**
     * This is now only used for testing.
     *
     * @return a mapping from action id to its old {@link ActionGroupKey}
     */
    @VisibleForTesting
    PreviousBroadcastActions getPreviousBroadcastActions() {
        return previousBroadcastActions;
    }

    /**
     * Metrics for {@link LiveActionsStatistician}
     */
    private static class Metrics {

        private static final DataMetricCounter ACTION_STAT_RECORDS = DataMetricCounter.builder()
            .withName("ao_action_stat_record_count")
            .withHelp("The number of action stat records in each incoming action snapshot.")
            .build()
            .register();

        private static final DataMetricCounter ACTION_STAT_RECORD_ERRORS = DataMetricCounter.builder()
            .withName("ao_action_stat_record_error_count")
            .withHelp("The number of errored-out inserts from the action stat records in each incoming action snapshot.")
            .build()
            .register();

        private static final DataMetricSummary ACTION_STAT_RECORD_SNAPSHOT_TIME = DataMetricSummary.builder()
            .withName("ao_action_stat_record_snapshot_seconds")
            .withHelp("Information about how long it took to snapshot action stat records.")
            .build()
            .register();

        private static final DataMetricSummary ACTION_STAT_RECORD_AGGREGATE_TIME = DataMetricSummary.builder()
            .withName("ao_action_stat_record_aggregate_seconds")
            .withHelp("Information about how long it took to aggregate action stat records.")
            .build()
            .register();

        private static final DataMetricSummary ACTION_STAT_RECORD_STORE_TIME = DataMetricSummary.builder()
            .withName("ao_action_stat_record_store_seconds")
            .withHelp("Information about how long it took to store action stat records to the DB.")
            .build()
            .register();
    }

    /**
     * A snapshot of all actions passed to {@link LiveActionsStatistician#recordActionStats}.
     */
    @Value.Immutable
    interface LiveActionsSnapshot {
        LocalDateTime topologyCreationTime();

        List<StatsActionView> actions();

        long topologyId();

        LocalDateTime actionSnapshotTime();

        /**
         * Convert this snapshot into a database record.
         *
         * @return The record, ready for insertion into the snapshot database.
         */
        @Nonnull
        default ActionSnapshotLatestRecord toDbRecord() {
            final ActionSnapshotLatestRecord snapshotRecord = new ActionSnapshotLatestRecord();
            snapshotRecord.setActionSnapshotTime(topologyCreationTime());
            snapshotRecord.setSnapshotRecordingTime(actionSnapshotTime());
            snapshotRecord.setTopologyId(topologyId());
            snapshotRecord.setActionsCount(actions().size());
            return snapshotRecord;
        }
    }

    /**
     * A utility class that stores a mapping from action id to its old {@link ActionGroupKey}.
     */
    public static class PreviousBroadcastActions {
        private Long2ObjectMap<ActionGroupKey> actionIdToActionGroupKey = Long2ObjectMaps.emptyMap();

        public int size() {
            return actionIdToActionGroupKey.size();
        }

        public ActionGroupKey getActionGroupKey(final long actionId) {
            return actionIdToActionGroupKey.get(actionId);
        }

        public boolean actionChanged(final long actionId, @Nonnull final ActionGroupKey actionGroupKey) {
            final ActionGroupKey oldKey = actionIdToActionGroupKey.get(actionId);
            // If an action is present in the map, it means that the action has already been recommended
            // and we do not want to consider it twice in the action stat counts. After a component
            // restart we will lose the cached recommended actions, and thus we have no way to tell the
            // ones we should count and the ones we shouldn't. For this reason every time the map is empty,
            // we won't consider any action as changed, we will just populate the map. Those actions
            // will be considered in the total action count, but not in the new action count. At the next
            // incoming market plan we will then be able to calculate the new action count. See OM-61639
            // for more informa
            if (actionIdToActionGroupKey.isEmpty()) {
                return false;
            }
            return oldKey == null || !oldKey.equals(actionGroupKey);
        }

        void updateActions(@Nonnull final LiveActionsSnapshot snapshot) {
            actionIdToActionGroupKey = new Long2ObjectOpenHashMap<>(snapshot.actions().size());
            snapshot.actions().forEach(action ->
                actionIdToActionGroupKey.put(action.recommendation().getId(), action.actionGroupKey()));
        }
    }
}
