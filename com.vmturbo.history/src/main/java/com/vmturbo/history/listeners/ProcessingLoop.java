package com.vmturbo.history.listeners;

import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Expected;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.None;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Processing;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Received;
import static com.vmturbo.history.listeners.ProcessingLoop.ProcessingAction.Repartition;
import static com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor.Live;
import static com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor.Projected;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.sun.istack.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.history.ingesters.IngestionMetrics;
import com.vmturbo.history.ingesters.IngestionMetrics.SafetyValve;
import com.vmturbo.history.listeners.IngestionStatus.IngestionState;
import com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor;

/**
 * This class directs the activities of the {@link TopologyCoordinator}, ingesting source and
 * projected real-time topologies and performing periodic rollup and repartitioning operations.
 *
 * <p>Plan topologies are not managed by {@link ProcessingLoop} because there's no need to
 * coordinate their ingestion with rollup processing. Plan topologies are processed as soon
 * as they're received.</p>
 *
 * <p>The instance operates as something like an event loop. Events detected by the topology
 * coordinator result in changes in an associated {@link ProcessingStatus} instance. Whenever
 * the processing loop fires, it scans the current processing status and chooses one or more
 * actions to carry out. These actions also may cause updates to the processing state.
 *
 * <p>In general, the coordinator should not take actions except as directed by the processing
 * loop.</p>
 *
 * <p>The class is designed as a {@link Runnable} that does not exit. It and its associated
 * coordinator "communicate" solely via updates posted to the processing status object, and use
 * object monitors to signal the need for attention.</p>
 */
class ProcessingLoop implements Runnable {

    private final ProcessingStatus processingStatus;
    private final int repartioningTimeoutSecs;
    private final long maxSleepMillis;
    private Logger logger = LogManager.getLogger(ProcessingLoop.class);

    private TopologyCoordinator topologyCoordinator;
    private Future<TopologyCoordinator> topologyCoordinatorFuture;
    private int maxIterations;
    private int iterationsPerformed = 0;

    /**
     * Create a a new instance to drive activities of a given topology coordinator.
     *
     * @param topologyCoordinator the process coordinator
     * @param processingStatus    its processing status object
     * @param config              config parameters
     */
    ProcessingLoop(final TopologyCoordinator topologyCoordinator,
            final ProcessingStatus processingStatus, TopologyCoordinatorConfig config) {
        this.topologyCoordinator = topologyCoordinator;
        this.processingStatus = processingStatus;
        this.repartioningTimeoutSecs = config.repartitioningTimeoutSecs();
        this.maxSleepMillis = TimeUnit.SECONDS.toMillis(config.processingLoopMaxSleepSecs());
    }

    /**
     * This method permits certain testing setups to be constructed that would otherwise face a
     * chicken-egg situation.
     *
     * <p>The issue arises because TopologyCoordinator either creates its own ProcessingLoop
     * thread, or it gets passed in. But ProcessingLoop needs its associated TopologyCoordinator
     * as a constructor argument. This alternative constructor allows things to be wired
     * together using a {@link java.util.concurrent.CompletableFuture} for the topology coordinator
     * handed to the processing loop. In that case, a FutureResolvingTopologyCoordinator is
     * created and passed to the normal constructor, and the procesing loop will resolve that to
     * the real topology coordinator at startup.</p>
     *
     * @param topologyCoordinatorFuture future that will resolve to the associated coordiator
     * @param processingStatus          processing status object
     * @param config                    configuration values
     */
    @VisibleForTesting
    ProcessingLoop(final Future<TopologyCoordinator> topologyCoordinatorFuture,
            final ProcessingStatus processingStatus, TopologyCoordinatorConfig config) {
        this((TopologyCoordinator)null, processingStatus, config);
        this.topologyCoordinatorFuture = topologyCoordinatorFuture;
    }

    /**
     * Runs processing loop and then exits after the given number of iterations.
     *
     * <p>This is used in some tests, since mocks don't generally work well across threads.</p>
     *
     * @param maxIterations maximum number of iterations to run
     */
    @VisibleForTesting
    void run(int maxIterations) {
        this.maxIterations = maxIterations;
        run();
    }

    /**
     * The main processing loop - never exits unless interrupted in a wait.
     */
    public void run() {
        // take care of special testing scenario where topology coordinator is provided via a
        // future
        if (topologyCoordinatorFuture != null) {
            try {
                topologyCoordinator = topologyCoordinatorFuture.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("Failed to resolve topology coordinator from future; exiting", e);
                return;
            }
        }
        // we're always locked on our thread except when we wait at the bottom of the loop.
        // otherwise we could risk missing a notification from a listener waiting to see
        // whether it should ingest a newly received topology
        synchronized (this) {
            // Right after a restart, if our status shows anything currently in Processing state,
            // we transition them to failed state, because there's no way to know any better, and
            // if we crashed and restarted, we're definitely not processing it anymore.
            cancelProcessingIngestionsAtStartup();
            // we loop until we can't find anything to do at the moment. Then we wait to be
            // woken by some event like topology arrival or end of an ingestion.
            while (true) {
                try {
                    final Pair<ProcessingAction, Object> nextAction = chooseNextAction();
                    switch (nextAction.getLeft()) {
                        case RunIngestion:
                            final IngestionStatus ingestion = (IngestionStatus)nextAction.getRight();
                            // mark the ingestion for processing
                            ingestion.startIngestion();
                            // and wake up the waiting thread
                            release(ingestion);
                            break;
                        case RunHourRollup:
                            topologyCoordinator.runHourRollup((Instant)nextAction.getRight());
                            break;
                        case RunDayMonthRollup:
                            topologyCoordinator.runDayMonthRollup((Instant)nextAction.getRight());
                            // repartitioning happens as a side-effect of this
                            processingStatus.setLastRepartitionTime(Instant.now());
                            break;
                        case Repartition:
                            topologyCoordinator.runRetentionProcessing();
                            if (processingStatus.getLastRepartitionTime() != Instant.MIN) {
                                logger.error("Running repartioning because the time limit was " +
                                        "exceeded; this could signal a serious issue with " +
                                        "topology processing in history component.");
                                IngestionMetrics.SAFETY_VALVE_ACTIVATION_COUNTER
                                        .labels(SafetyValve.REPARTITION.getLabel(), "-")
                                        .increment();
                            }
                            processingStatus.setLastRepartitionTime(Instant.now());
                            break;
                        case Idle:
                            try {
                                // nothing left to do at the moment, persist state and wait for a status change
                                processingStatus.store();
                                logStatusSummary();
                                synchronized (Thread.currentThread()) {
                                    // here we'll wait for an external event that might require
                                    // a response, but with a timeout so we can check on our
                                    // various safety-valves like not allowing too much time
                                    // to pass between repartitioning runs
                                    Thread.currentThread().wait(maxSleepMillis);
                                }
                            } catch (InterruptedException e) {
                                // if we're interrupted we should wrap up
                                return;
                            }
                            break;
                    }
                    logStatusSummary();
                    processingStatus.store();
                } catch (Exception e) {
                    logger.error("Exception in topology processing loop", e);
                }

                // honor a max-iterations testing constraint if there is one
                if (maxIterations > 0) {
                    if (++iterationsPerformed >= maxIterations) {
                        return;
                    }
                }
            }
        }
    }

    private void cancelProcessingIngestionsAtStartup() {
        for (final TopologyFlavor flavor : TopologyFlavor.values()) {
            processingStatus.getIngestions(flavor).forEach(ingestion -> {
                if (ingestion.getState() == Processing) {
                    final IllegalStateException failureReason = new IllegalStateException(
                            String.format("Ingestion %s was found to be in Processing state at " +
                                            "startup; presumed failed due to component shutdown",
                                    ingestion));
                    ingestion.failIngestion(Optional.empty(), failureReason);
                }
            });
        }
    }

    @VisibleForTesting
    Pair<ProcessingAction, Object> chooseNextAction() {
        // if we're overdue for repartitioning, do that now... if that doesn't happen regularly
        // we risk exhausting space on the DB server
        final Instant cutoff = processingStatus.getLastRepartitionTime()
                .plus(repartioningTimeoutSecs, ChronoUnit.SECONDS);
        if (Instant.now().isAfter(cutoff)) {
            return Pair.of(Repartition, null);
        }
        // first a little housekeeping... drop any expired processing status data
        processingStatus.prune();
        // and a little more...  check for ingestions that should be marked as Missed or Skipped
        processStaleIngestions();
        // now we're all up-to-date, figure out what to do

        // There may be a snapshot that have been waiting for hourly rollups beyond the time
        // limit, in which case we force it to a resolved state so that the the snapshot will
        // be picked up as ready-for-hourly rollups in the following logic.
        Instant snapshot;
        if ((snapshot = findTimedoutHourlyRollup()) != null) {
            logger.error("Forcing resolution for snapshot time {} because a time limit was " +
                    "exceeded; this may indicate a serious issue with topology processing " +
                    "in history component.", snapshot);
            for (IngestionStatus ingestion: processingStatus.forceResolved(snapshot)) {
                release(ingestion);
            }
            IngestionMetrics.SAFETY_VALVE_ACTIVATION_COUNTER
                    .labels(SafetyValve.RESOLVE_SNAPSHOT.getLabel(), snapshot.toString())
                    .increment();
        }

        // we need to be up-to-date on hourly rollups before attempting a day-month
        // rollup, because the former is the source of the latter
        IngestionStatus ingestion;
        if ((snapshot = findReadyHourlyRollup()) != null) {
            return Pair.of(ProcessingAction.RunHourRollup, snapshot);
        }

        // hourlies complete, see if their are any daily/monthly rollups to do
        if ((snapshot = findReadyDayMonthRollup()) != null) {
            return Pair.of(ProcessingAction.RunDayMonthRollup, snapshot);
        }
        // Now see if we have any received topologies waiting to be ingested.
        if ((ingestion = findReadyIngestion()) != null) {
            return Pair.of(ProcessingAction.RunIngestion, ingestion);
        }
        return Pair.of(ProcessingAction.Idle, null);
        // Any such ingestions will be waiting in their own thread, on their
        // ingestion object from the processing status. If we wake the thread
        // without setting the ingestion state to Processing, the topology will
        // be discarded.
    }

    /**
     * Find the least recent snapshot for which hourly rollups should be processed and do it.
     *
     * <p>A snapshot qualifies for hourly snapshots if it is fully qualified and hs not
     * had rollups processed previously./p>
     *
     * @return true if an hourly rollup was performed
     */
    @Nullable
    private Instant findReadyHourlyRollup() {
        // least recent fully resolved snapshot that has not had hourly rollups needs to do it
        final List<Instant> snapshots = processingStatus.getSnapshotTimes()
                .collect(Collectors.toList());
        for (int i = snapshots.size() - 1; i >= 0; i--) {
            final Instant snapshot = snapshots.get(i);
            if (processingStatus.needsHourlyRollup(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    private Instant findTimedoutHourlyRollup() {
        // find the earliest timestamp that has timed out for hourly rollups, if any
        final List<Instant> snapshots = processingStatus.getSnapshotTimes()
                .collect(Collectors.toList());
        for (int i = snapshots.size() - 1; i >= 0; i--) {
            final Instant snapshot = snapshots.get(i);
            if (processingStatus.exceedsHourlyRollupTimeout(snapshot)
                    && !processingStatus.isAnyIngestionProcessing(snapshot)) {
                return snapshot;
            }
        }
        return null;
    }

    /**
     * Find the least recent snapshot for which daily/monthly rollups should be processed and do it.
     *
     * <p>A snapshotqualifies if the next later snapshot falls in a different hour, and if
     * it and all prior snapshot in the same hour are resolved and have been rolled up into an
     * hourly rollup, and if daily/monthly snapshots have not already occurred. If the snapshot
     * itself is resolved, all prior snapshots must also be, so we need not actually check all of
     * them.</p>
     *
     * @return true if a day/month rollup was performed
     */
    private Instant findReadyDayMonthRollup() {
        final List<Instant> snapshots = processingStatus.getSnapshotTimes()
                .collect(Collectors.toList());
        for (int i = snapshots.size() - 1; i > 0; i--) {
            final Instant snapshot = snapshots.get(i);
            if (snapshot.truncatedTo(ChronoUnit.HOURS)
                    .isBefore(snapshots.get(i - 1).truncatedTo(ChronoUnit.HOURS))) {
                if (processingStatus.needsDayMonthRollup(snapshot)) {
                    return snapshot;
                }
            }
        }
        return null;
    }

    /**
     * Try to find an ingestion that we can run.
     *
     * @return Ingestion to run, or null if none found
     */
    private IngestionStatus findReadyIngestion() {
        // a ready live ingestion is an ingestion in Received state that is the most recent
        // live ingestion that we know of.
        final IngestionStatus firstLiveIngestion
                = processingStatus.getIngestions(Live)
                .findFirst()
                .filter(i -> i.getState() == Received)
                .orElse(null);
        // a ready projected exception is similar, but we do allow for more recent ingestions
        // in None state
        final IngestionStatus firstProjectedIngestion
                = processingStatus.getIngestions(Projected)
                .filter(i -> i.getState() != None)
                .findFirst()
                .filter(i -> i.getState() == Received)
                .orElse(null);
        // we prefer a projected ingestion since the topologies arrive later and they may
        // be holding up needed rollups
        return firstProjectedIngestion != null ? firstProjectedIngestion : firstLiveIngestion;
    }

    /**
     * Look for ingestions that we will choose to skip, or which we never received.
     *
     * <p>We favor projected topologies, because otherwise they hold up rollups.</p>
     */
    private void processStaleIngestions() {
        // For projected topologies, the most recent entries may exist solely because their associated
        // live ingestions exist, in which case their state will be None. Any other state means
        // we've independently heard of (and maybe even attempted to process) the topology, so in that
        // case it supersedes all prior projected topology ingestions.
        final List<IngestionStatus> projectedIngestions = processingStatus.getIngestions(Projected)
                .collect(Collectors.toList());
        // if we remove everything that's not one of our anchor stats, the first remaining
        // ingestion will be our anchor
        Integer anchor = IntStream.range(0, projectedIngestions.size())
                .boxed()
                .filter(i -> projectedIngestions.get(i).getState() != None)
                .findFirst().orElse(null);
        if (anchor != null) {
            // do this in reverse chrono order so the logs are more sensible
            for (int i = projectedIngestions.size() - 1; i > anchor; i--) {
                final IngestionStatus projectedIngestion = projectedIngestions.get(i);
                final IngestionState state = projectedIngestion.getState();
                if (state == Received) {
                    final String topologyLabel = projectedIngestion.getTopologyLabel();
                    logger.info("Skipping {} because a later one has been received.",
                            topologyLabel);
                    processingStatus.skip(Projected,
                            projectedIngestion.getInfo(),
                            projectedIngestion.getTopologyLabel());
                    release(projectedIngestion);
                } else if (state == Expected) {
                    String topologyLabel = projectedIngestion.getTopologyLabel();
                    logger.warn("Expected topology {} never arrived, marking as missed.", topologyLabel);
                    projectedIngestion.miss();
                } else if (state == None) {
                    projectedIngestion.miss();
                }
            }
        }
        // For live ingestions, the most recent topology is presumed live and, if not yet received,
        // then imminent. So we choose not to process any older topologies. Any that have been
        // received already are skipped, while any that were expected are marked missing.
        final List<IngestionStatus> liveIngestions = processingStatus.getIngestions(Live)
                .collect(Collectors.toList());
        // loop from least to most recent so logs make sense
        for (
                int i = liveIngestions.size() - 1;
                i > 0; i--) {
            final IngestionStatus liveIngestion = liveIngestions.get(i);
            final IngestionState state = liveIngestion.getState();
            final String topologyLabel = liveIngestion.getTopologyLabel();
            if (state == Received) {
                logger.info("Skipping {} because a latter one has been received or announced.",
                        topologyLabel);
                processingStatus.skip(
                        Live, liveIngestion.getInfo(), liveIngestion.getTopologyLabel());
                // ingestions in this state are waiting for a decision, so release them
                release(liveIngestion);
            } else if (state == Expected) {
                logger.warn("Expected topology {} never arrived, marking as missed.", topologyLabel);
                liveIngestion.miss();
            }
        }
    }

    private String lastSummary = null;

    /**
     * Log a message with a brief summary of recent processing history.
     */
    private void logStatusSummary() {
        if (!processingStatus.isEmpty()) {
            final String summary = processingStatus.getSummary("    ");
            // try not to be repetitive
            if (!summary.equals(lastSummary)) {
                logger.info("Recent processing summary:\n{}", summary);
                lastSummary = summary;
            }
        }
    }

    /**
     * Release a thread from a wait on the ingestion status condition for a topology it has
     * recieved, so it can get on with either processing or skipping the topology.
     *
     * @param ingestion the ingestion status whose condition variable is being awaited
     */
    private void release(IngestionStatus ingestion) {
        ingestion.signal();
    }

    /**
     * Actions the processing loop can initiate, based on current processing status.
     */
    enum ProcessingAction {
        /** Run a ready ingestion. */
        RunIngestion,
        /** Run hourly rollups for a timestamp. */
        RunHourRollup,
        /** Run dayly/monthly rollups for a timestamp. */
        RunDayMonthRollup,
        /** Run repartioning, outside of normal rollup processing. */
        Repartition,
        /** Nothing to do until status changes. */
        Idle
    }
}
