package com.vmturbo.history.listeners;

import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.None;
import static com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor.Live;
import static com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor.Projected;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import io.opentracing.SpanContext;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Table;

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.RemoteIteratorDrain;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.components.common.utils.MultiStageTimer.Detail;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.ingesters.IngestionMetrics;
import com.vmturbo.history.ingesters.IngestionMetrics.SafetyValve;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase;
import com.vmturbo.history.ingesters.common.TopologyIngesterBase.IngesterState;
import com.vmturbo.history.ingesters.live.ProjectedRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.live.SourceRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.plan.ProjectedPlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.SourcePlanTopologyIngester;
import com.vmturbo.history.listeners.IngestionStatus.IngestionState;
import com.vmturbo.market.component.api.AnalysisSummaryListener;
import com.vmturbo.market.component.api.ProjectedTopologyListener;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.TopologySummaryListener;

/**
 * This class manages all topology ingestion for the history component.
 *
 * <p>Realtime and Plan source nad projected topologies are all covered. In addition, topology
 * summary topics are monitored for advance notification of available topologies.</p>
 *
 * <p>Included here are listener methods for the relevant topics, methods to process received
 * topologies, and methods to invoke rollup and repartitioning operations in the database. However
 * logic to choose which of operations to perform at any point in time with respect to realtime
 * topologies is outsourced to an instance of {@link ProcessingLoop}. Plan topologies are always
 * processed as they arrive, since they do not contend in any way with rollup processing.</p>
 *
 * <p>The overall state of live topology processing is maintained in an instance of
 * {@link ProcessingStatus}. The way this class coordinates with the {@link ProcessingLoop} is that
 * when an event is detected (arrival of a topology, completion of processing, etc.), the processing
 * status is updated, and then the processing loop is awoken so it can inspect the updated status
 * and decide on what to do next.</p>
 *
 * <p>We use a read-write lock to ensure that live topology ingestion and rollup/repartitioning
 * actions never overlap, since that tends to create lock contention that seriously impairs
 * performance. The lock permits multiple ingestions to proceed concurrently, or a single
 * rollup/repartitioning operation to run, but never both at once.</p>
 */
public class TopologyCoordinator extends TopologyListenerBase implements EntitiesListener,
        ProjectedTopologyListener, TopologySummaryListener, AnalysisSummaryListener {
    private final Logger logger = LogManager.getLogger();

    private final SourceRealtimeTopologyIngester sourceRealtimeTopologyIngester;
    private final ProjectedRealtimeTopologyIngester projectedRealtimeTopologyIngester;
    private final SourcePlanTopologyIngester sourcePlanTopologyIngester;
    private final ProjectedPlanTopologyIngester projectedPlanTopologyIngester;
    private final RollupProcessor rollupProcessor;
    private final Thread processingLoop;

    // Our read-write lock and its read & write sides. We don't need to configure fairness because
    // when the processing loop initiates rollup activity, it waits for that activity to complete
    // before considering, let alone initiating any other activity
    private final ReadWriteLock ingestionRollupLock = new ReentrantReadWriteLock(true);
    private final Lock ingestionLock = ingestionRollupLock.readLock();
    private final Lock rollupLock = ingestionRollupLock.writeLock();
    // successfully ingested topologies are announced with this
    private final StatsAvailabilityTracker availabilityTracker;
    // our processing status
    private final ProcessingStatus processingStatus;
    private final int ingestionTimeoutSecs;
    // following latch allows kafka listeners to wait for component startup to complete
    // before processing any messages.
    private final CountDownLatch startupLatch = new CountDownLatch(1);

    /**
     * Create a new instance.
     *
     * @param sourceRealtimeTopologyIngester          used to fully process a live topology
     * @param projectedRealtimeTopologyIngester used to fully process a projected topology
     * @param sourcePlanTopologyIngester          used to fully process a plan topology
     * @param projectedPlanTopologyIngester used to fully process a projected plan topology
     * @param rollupProcessor               used to perform rollup and repartitioning operations
     * @param statsAvailabilityTracker      used to announce successful ingestions of topologies
     * @param historydbIO                   database utils
     * @param config                        config parameters
     */
    public TopologyCoordinator(
            @Nonnull final SourceRealtimeTopologyIngester sourceRealtimeTopologyIngester,
            @Nonnull final ProjectedRealtimeTopologyIngester projectedRealtimeTopologyIngester,
            @Nonnull final SourcePlanTopologyIngester sourcePlanTopologyIngester,
            @Nonnull final ProjectedPlanTopologyIngester projectedPlanTopologyIngester,
            @Nonnull final RollupProcessor rollupProcessor,
            @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker,
            @Nonnull final HistorydbIO historydbIO,
            @Nonnull final TopologyCoordinatorConfig config) {
        this(
                sourceRealtimeTopologyIngester,
                projectedRealtimeTopologyIngester,
                sourcePlanTopologyIngester,
                projectedPlanTopologyIngester,
                rollupProcessor,
                null,
                null,
                statsAvailabilityTracker,
                historydbIO,
                config
        );
    }

    /**
     * Create a new instance.
     *
     * <p>This constructor includes field values that are only provided in testing.</p>
     *
     * @param sourceRealtimeTopologyIngester          used to fully process a live topology
     * @param projectedRealtimeTopologyIngester used to fully process a projected topology
     * @param sourcePlanTopologyIngester          used to fully process a plan topology
     * @param projectedPlanTopologyIngester used to fully process a projected plan topology
     * @param rollupProcessor               used to perform rollup and repartitioning operations
     * @param maybeNullProcessingStatus     processing status object; if null a new one is created
     *                                      (nonnull is used testing)
     * @param maybeNullProcessingLoop       thread to run a processing loop; if null a new one is created
     *                                      (nonnull is used in testing)
     * @param statsAvailabilityTracker      used to announce successful ingestions of topologies
     * @param historydbIO                   database utils
     * @param config                        config parameters
     */
    @VisibleForTesting
    TopologyCoordinator(
            @Nonnull final SourceRealtimeTopologyIngester sourceRealtimeTopologyIngester,
            @Nonnull final ProjectedRealtimeTopologyIngester projectedRealtimeTopologyIngester,
            @Nonnull final SourcePlanTopologyIngester sourcePlanTopologyIngester,
            @Nonnull final ProjectedPlanTopologyIngester projectedPlanTopologyIngester,
            @Nonnull final RollupProcessor rollupProcessor,
            final ProcessingStatus maybeNullProcessingStatus,
            final Thread maybeNullProcessingLoop,
            @Nonnull final StatsAvailabilityTracker statsAvailabilityTracker,
            @Nonnull final HistorydbIO historydbIO,
            @Nonnull final TopologyCoordinatorConfig config) {
        this.sourceRealtimeTopologyIngester = Objects.requireNonNull(sourceRealtimeTopologyIngester);
        this.projectedRealtimeTopologyIngester = Objects.requireNonNull(projectedRealtimeTopologyIngester);
        this.sourcePlanTopologyIngester = Objects.requireNonNull(sourcePlanTopologyIngester);
        this.projectedPlanTopologyIngester = Objects.requireNonNull(projectedPlanTopologyIngester);
        this.rollupProcessor = Objects.requireNonNull(rollupProcessor);
        this.availabilityTracker = Objects.requireNonNull(statsAvailabilityTracker);
        this.processingStatus = maybeNullProcessingStatus != null ? maybeNullProcessingStatus
                : new ProcessingStatus(config, historydbIO);
        this.ingestionTimeoutSecs = config.ingestionTimeoutSecs();
        // Create a processing loop that will be driven from our processing stats. We'll
        // start it the first time we receive a message
        this.processingLoop = maybeNullProcessingLoop != null ? maybeNullProcessingLoop
                : new Thread(new ProcessingLoop(this, processingStatus, config), "tp-loop");
    }

    @Override
    public void onTopologyNotification(@Nonnull TopologyInfo info,
                                       @Nonnull RemoteIterator<Topology.DataSegment> topology,
                                       @Nonnull final SpanContext tracingContext) {
        awaitStartup();
        try (TracingScope tracingScope = Tracing.trace("history_on_topology_notification", tracingContext)) {
            if (info.getTopologyType() == TopologyType.REALTIME) {
                String topologyLabel = TopologyDTOUtil.getSourceTopologyLabel(info);
                int count = handleTopology(info, topologyLabel, topology, sourceRealtimeTopologyIngester, Live);
                SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL,
                        SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                    .observe((double)count);
            } else if (info.getTopologyType() == TopologyType.PLAN) {
                // these have no impact on rollups, so we can just perform ingestion as they arrive (in the listener
                // thread for plan topologies topic)
                try {
                    if (PlanDTOUtil.isTransientPlan(info)) {
                        // For the reservation plan we don't save stats, because we only care about the
                        // projected topology, as parsed by the ReservationManager in the plan orchestrator.
                        logger.info("Ignoring plan source topology for reservation plan {}",
                            info.getTopologyContextId());
                    } else {
                        final Pair<Integer, BulkInserterFactoryStats> result
                            = sourcePlanTopologyIngester.processBroadcast(info, topology);
                        SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                            .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL,
                                SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                            .observe((double)result.getLeft());
                    }
                } catch (Exception e) {
                    logger.error("Plan topology ingestion failed", e);
                } finally {
                    RemoteIteratorDrain.drainIterator(topology, TopologyDTOUtil.getSourceTopologyLabel(info), true);
                }
                try {
                    availabilityTracker.topologyAvailable(
                        info.getTopologyContextId(), TopologyContextType.PLAN, true);
                } catch (InterruptedException | CommunicationException e) {
                    logger.warn("Failed to notify of  plan topology ingestion", e);
                }
            }
        }
    }

    @Override
    public void onProjectedTopologyReceived(final long projectedTopologyId,
                                            @Nonnull final TopologyInfo info,
                                            @Nonnull final RemoteIterator<ProjectedTopologyEntity>
                                                topology,
                                            @Nonnull final SpanContext tracingContext) {
        awaitStartup();
        try (TracingScope scope = Tracing.trace("history_on_projected_topology", tracingContext)) {
            final String topologyLabel = TopologyDTOUtil.getProjectedTopologyLabel(info);
            if (info.getTopologyType() == TopologyType.REALTIME) {
                int count = handleTopology(info, topologyLabel, topology,
                        projectedRealtimeTopologyIngester, Projected);
                SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                    .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL,
                        SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL)
                    .observe((double)count);
            } else {
                // these have no impact on rollups, so we can just perform ingestion as they arrive (in the listener
                // thread for the projected topologies topic)
                try {
                    if (PlanDTOUtil.isTransientPlan(info)) {
                        // For some plans we don't care about saving stats, because we just need
                        // the raw projected topology for processing in the Plan Orchestrator.
                        logger.info("Ignoring projected topology for plan {}",
                            info.getTopologyContextId());
                    } else {
                        final Pair<Integer, BulkInserterFactoryStats> result
                            = projectedPlanTopologyIngester.processBroadcast(info, topology);
                        SharedMetrics.TOPOLOGY_ENTITY_COUNT_HISTOGRAM
                            .labels(SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL,
                                SharedMetrics.PLAN_CONTEXT_TYPE_LABEL)
                            .observe((double)result.getLeft());
                    }
                } catch (Exception e) {
                    logger.error("Projected plan topology ingestion failed", e);
                } finally {
                    RemoteIteratorDrain.drainIterator(topology, topologyLabel, true);
                }
                try {
                    availabilityTracker.projectedTopologyAvailable(
                        info.getTopologyContextId(), TopologyContextType.PLAN, true);
                } catch (InterruptedException | CommunicationException e) {
                    logger.warn("Failed to notify of projected plan topology ingestion", e);
                }
            }
        }
    }

    /**
     * This method is invoked when a new live topology (source or projected) arrives.
     *
     * <p>We don't immediately start ingesting it. Instead, we update process status and kick
     * the processing loop and wait for it to kick us back. When that happens, if we find the
     * ingestion to be in processing state, then we proceed with the ingestion. In any other case
     * we'll exit, and drain the topology as a side-effect.</p>
     *
     * @param info          the topology info describing this topology
     * @param topologyLabel a human-readable label, for logging
     * @param topology      a remote iterator conveying the topology
     * @param ingester      a {@link TopologyIngesterBase} to ingest the topology
     * @param flavor        the topology flavor
     * @param <T>           the type of individual topology members
     * @return number of entities processed
     */
    private <T> int handleTopology(TopologyInfo info,
            String topologyLabel,
            RemoteIterator<T> topology,
            TopologyIngesterBase<T> ingester,
            TopologyFlavor flavor) {
        try {
            // note receipt of the broadcast
            logger.info("Received {}", topologyLabel);
            processingStatus.receive(flavor, info, topologyLabel);
            // do the kick exchange with processing loop
            if (waitForGreenLight(flavor, info, topologyLabel, IngestionState.Received)) {
                // now ingest the topology if processing loop put us in processing state
                if (processingStatus.isProcessing(flavor, info)) {
                    // lock out rollup operations
                    ingestionLock.lock();
                    // we create shared state in advance rather than just letting the ingester
                    // create it, so we'll still be able to get partial stats after a failure
                    IngesterState sharedState = ingester.createSharedState(info);
                    try (DataMetricTimer timer = getDurationMetric(info, flavor)) {
                        // perform the actual ingestion and stash the results
                        processingStatus.startIngestion(flavor, info, topologyLabel);
                        final Pair<Integer, BulkInserterFactoryStats> results
                            = ingester.processBroadcast(info, topology, sharedState);
                        processingStatus.finishIngestion(
                            flavor, info, topologyLabel, results.getRight());
                        // announce it to the world
                        try {
                            switch (flavor) {
                                case Live:
                                    availabilityTracker.topologyAvailable(
                                            info.getTopologyContextId(), TopologyContextType.LIVE, true);
                                    break;
                                case Projected:
                                    availabilityTracker.projectedTopologyAvailable(
                                            info.getTopologyContextId(), TopologyContextType.LIVE, true);
                                    break;
                                default:
                                    logger.warn("Unknown topology flavor: {}", flavor.name());
                                    break;
                            }
                        } catch (Exception e) {
                            logger.info("Failed to notify availability after ingestion of {}",
                                    topologyLabel, e);
                        }
                        return results.getLeft();
                    } catch (Exception e) {
                        logger.error("Ingestion of {} failed", topologyLabel, e);
                        processingStatus.failIngestion(
                                flavor, info, topologyLabel,
                                Optional.of(sharedState.getLoaders().getStats()), e);
                    } finally {
                        // don't leave without dropping our rollup-preventing lock
                        ingestionLock.unlock();
                        // one final kick the the processing loop (and then run off before it can kick
                        // back :-)
                        triggerProcessingLoop();
                    }
                }
            } else {
                logger.error("Ingestion timed out waiting to be scheduled for processing, "
                        + "so skipping it; this could indicate a serious issue with topology "
                        + "processing in history component");
                processingStatus.skip(flavor, info, topologyLabel);
                IngestionMetrics.SAFETY_VALVE_ACTIVATION_COUNTER
                        .labels(getLabelsForSkipSafetyValve(info, flavor))
                        .increment();
            }
        } catch (Exception e) {
            logger.error("Ingestion of {} failed with an uncaught exception; "
                    + "remaining chunks, if any, will be discarded", topologyLabel, e);
        } finally {
            // We do this in all cases, to make sure we don't end up stuck in mid-topology.
            // If we were successful this will be a no-op.
            RemoteIteratorDrain.drainIterator(topology, topologyLabel, true);
        }
        // here on any sort of failure or skip - we'll report no entities processed (even though
        // this may have been a partial ingestion, so zero's not really accurate)
        return 0;
    }

    @VisibleForTesting
    static String[] getLabelsForSkipSafetyValve(TopologyInfo info, TopologyFlavor flavor) {
        final String typeTag = info.getTopologyType() == TopologyType.REALTIME
                ? SharedMetrics.LIVE_CONTEXT_TYPE_LABEL
                : SharedMetrics.PLAN_CONTEXT_TYPE_LABEL;
        final String stageTag = flavor == Projected
                ? SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL
                : SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL;
        return new String[]{SafetyValve.SKIP_TOPOLOGY.getLabel(), typeTag, stageTag};
    }

    @Override
    public void onTopologySummary(final TopologySummary topologySummary) {
        awaitStartup();
        final TopologyInfo topologyInfo = topologySummary.getTopologyInfo();
        if (topologyInfo.getTopologyType() == TopologyType.PLAN) {
            // not interested in plan topologies
            return;
        }
        // no need to log this if the topology is earlier than our latest known topology of this type
        Optional<Instant> latestKnown = processingStatus.getSnapshotTimes().findFirst();
        Instant topologyTime = Instant.ofEpochMilli(topologyInfo.getCreationTime());
        if (latestKnown.isPresent() && latestKnown.get().isAfter(topologyTime)) {
            return;
        }
        // only process this notification if this is the first we've heard of this topology
        if (processingStatus.getIngestion(Live, topologyInfo).getState() == None) {
            final String topologyLabel = TopologyDTOUtil.getSourceTopologyLabel(topologyInfo);
            logger.info("{} has been announced", topologyLabel);
            // update process status to announce this topology, and kick the processing loop
            processingStatus.expect(Live, topologyInfo, topologyLabel);
            triggerProcessingLoop();
        }
    }

    @Override
    public void onAnalysisSummary(@Nonnull final AnalysisSummary analysisSummary) {
        awaitStartup();
        final TopologyInfo topologyInfo = analysisSummary.getSourceTopologyInfo();
        if (topologyInfo.getTopologyType() == TopologyType.PLAN) {
            // not interested in plan topologies
            return;
        }
        // no need to log this if the topology is earlier than our latest known topology of this type
        Optional<Instant> latestKnown = processingStatus.getSnapshotTimes().findFirst();
        Instant topologyTime = Instant.ofEpochMilli(topologyInfo.getCreationTime());
        if (latestKnown.isPresent() && latestKnown.get().isAfter(topologyTime)) {
            return;
        }
        if (processingStatus.getIngestion(Projected, topologyInfo).getState() == None) {
            final String topologyLabel = TopologyDTOUtil.getProjectedTopologyLabel(topologyInfo);
            logger.info("{} has been announced", topologyLabel);
            // update process status to announce this topology, and kick the processing loop
            processingStatus.expect(Projected, topologyInfo, topologyLabel);
        }
    }

    /**
     * Start topology processing.
     *
     * <p>This means starting the ProcessingLoop thread and clearing the startup latch, so any
     * threads waiting for startup can proceed.</p>
     */
    public void startup() {
        if (startupLatch.getCount() > 0) {
            if (!processingLoop.isAlive()) {
                // load saved state of processing status, if any
                processingStatus.load();
                // and let the processing commence!
                processingLoop.start();
            }
            startupLatch.countDown();
        }
    }

    /**
     * Wait until the {@link #startup()} method has been called.
     *
     * <p>This prevents message listeners from initiating any processing until component startup
     * has completed. The latter is what makes the call to {@link #startup}.</p>
     */
    private void awaitStartup() {
        try {
            startupLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * This is where we kick the processing loop so it will wake up and decide what, if anything,
     * to do, based on the current processing state.
     *
     * <p>The processing loop is always synchronized on its own thread, except when it's waiting
     * to be kicked, so it will never miss a kick.</p>
     */
    private void triggerProcessingLoop() {
        synchronized (processingLoop) {
            processingLoop.notify();
        }
    }

    /**
     * Wait for a kick back (not a kickback) from processing loop when we're sitting on a newly
     * arrived topology.
     *
     * <p>The kick back is communicated via a {@link java.util.concurrent.locks.Condition}
     * associated with the ingestion status for this topology.</p>
     *
     * @param flavor        topology flavor
     * @param topologyInfo  topology info
     * @param topologyLabel label, for logging
     * @param waitState     ingestion state to move from to signify green light
     * @return true if we were awoken by a signal, else we timed out
     */
    private boolean waitForGreenLight(final TopologyFlavor flavor, final TopologyInfo topologyInfo,
            final String topologyLabel, final IngestionState waitState) {
        triggerProcessingLoop();
        try {
            final IngestionStatus ingestion = processingStatus.getIngestion(flavor, topologyInfo);
            return ingestion.await(ingestionTimeoutSecs, ChronoUnit.SECONDS, waitState);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            processingStatus.failIngestion(flavor, topologyInfo, topologyLabel, Optional.empty(), e);
            // prevent caller treating this as a timeout
            return true;
        } finally {
            triggerProcessingLoop();
        }
    }

    /**
     * Configure a {@link DataMetricTimer} to measure duration of processing
     * a topology, given its {@link TopologyInfo} and {@link TopologyFlavor}.
     *
     * @param info   topology info
     * @param flavor topology flavor
     * @return Configured {@link DataMetricTimer}
     */
    private DataMetricTimer getDurationMetric(TopologyInfo info, TopologyFlavor flavor) {
        String label = flavor == Live ? SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL
                : flavor == Projected ? SharedMetrics.PROJECTED_TOPOLOGY_TYPE_LABEL
                : "unknown";
        String value = info.getTopologyType() == TopologyType.REALTIME
                ? SharedMetrics.LIVE_CONTEXT_TYPE_LABEL
                : SharedMetrics.PLAN_CONTEXT_TYPE_LABEL;
        return SharedMetrics.UPDATE_TOPOLOGY_DURATION_SUMMARY
                .labels(label, value)
                .startTimer();
    }

    /**
     * Run hourly rollups on the records with the given snapshot time.
     *
     * <p>The ingestion stats for any ingestions that were performed for that snapshot time
     * are passed to support optimization of the rollup processing.</p>
     *
     * @param snapshot snapshot time of the topology to be rolled up
     */
    void runHourRollup(Instant snapshot) {
        processingStatus.startHourRollup(snapshot);
        List<Table> tables = processingStatus.getIngestionTables(snapshot)
                .distinct()
                .collect(Collectors.toList());
        // nothing to do if there are no stats
        if (!tables.isEmpty()) {
            // this can't execute while ingestions are in progress
            rollupLock.lock();
            logger.info("Running hourly rollups for snapshot time {}", snapshot);
            try {
                rollupProcessor.performHourRollups(tables, snapshot);
            } finally {
                rollupLock.unlock();
            }
        }
        processingStatus.finishHourRollup(snapshot);
    }

    /**
     * Run daily and monthly rollups, based using data from the hourly rollup.
     *
     * <p>The ingestion stats for any ingestions that were performed during that hour are
     * passed to support optimization of the rollup processing</p>
     *
     * @param snapshot any time that is included in the hour whose data will be rolled up
     *                 to day/month rollups
     */
    void runDayMonthRollup(final Instant snapshot) {
        processingStatus.startDayMonthRollup(snapshot);
        List<Table> tables = processingStatus.getIngestionTablesForHour(snapshot)
                .distinct()
                .collect(Collectors.toList());
        if (!tables.isEmpty()) {
            rollupLock.lock();
            logger.info("Running daily/monthly rollups and repartitioning for hour {}",
                    snapshot.truncatedTo(ChronoUnit.HOURS));
            try {
                rollupProcessor.performDayMonthRollups(tables, snapshot);
            } finally {
                rollupLock.unlock();
            }
        }
        processingStatus.finishDayMonthRollup(snapshot);
    }

    void runRetentionProcessing() {
        rollupLock.lock();
        logger.info("Repartitioning all stats tables");
        try {
            MultiStageTimer timer = new MultiStageTimer(logger);
            rollupProcessor.performRetentionProcessing(timer);
            timer.stopAll().info("Repartitioning completed in", Detail.OVERALL_SUMMARY);
        } finally {
            rollupLock.unlock();
        }
    }

    private boolean isRolledUpTable(Table<?> table) {
        return EntityType.fromTable(table).map(EntityType::rollsUp).orElse(false);
    }

    /**
     * Every topology we process is one of these "flavors".
     *
     * <p>(unfortunately, "type" already means something else!)</p>
     */
    enum TopologyFlavor {
        /**
         * A real-time topology produced by topology-processor.
         */
        Live('L'),
        /**
         * A product of the market component, providing price index information for the
         * live topology. Its <code>sourceTopologyInfo</code> is identical to the topology info
         * appearing on the live topology on which it is based.
         */
        Projected('P');

        private final char processingStatusChar;

        /**
         * Create a member.
         *
         * @param processingStatusChar alphabetic character to use when in status summary for an
         *                             ingestion of this flavor in Processing or Processed state;
         *                             letter case will distinguish those states
         */
        TopologyFlavor(char processingStatusChar) {
            this.processingStatusChar = processingStatusChar;
        }

        public char getProcessingStatusChar(boolean processingIsComplete) {
            return processingIsComplete ? Character.toUpperCase(processingStatusChar)
                    : Character.toLowerCase(processingStatusChar);
        }
    }
}
