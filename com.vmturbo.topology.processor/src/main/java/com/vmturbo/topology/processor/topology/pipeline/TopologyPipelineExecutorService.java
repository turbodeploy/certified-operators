package com.vmturbo.topology.processor.topology.pipeline;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.EntityOids;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastFailure;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.components.common.utils.ComponentRestartHelper;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;

import common.HealthCheck.HealthState;

/**
 * This class controls the building and running of topology pipelines. It is responsible for
 * limiting the number of concurrent topology pipelines (to reduce resource contention), and
 * handling redundant broadcasts.
 *
 * <p>It uses an internal threadpool to asynchronously run the queued pipelines, and returns
 * {@link TopologyPipelineRequest}s that callers can wait on if they want to block until
 * a broadcast is successful.
 */
@ThreadSafe
public class TopologyPipelineExecutorService implements AutoCloseable {

    private final ExecutorService planExecutorService;

    private final ExecutorService realtimeExecutorService;

    private final LivePipelineFactory livePipelineFactory;

    private final PlanPipelineFactory planPipelineFactory;

    private final EntityStore entityStore;

    private final TopologyPipelineQueue realtimePipelineQueue;

    private final TopologyPipelineQueue planPipelineQueue;

    private final TargetStore targetStore;

    private final StalenessInformationProvider stalenessInformationProvider;

    private static final TargetHealth DEFAULT_TARGET_HEALTH =
            TargetHealth.newBuilder().setHealthState(HealthState.NORMAL).build();

    private static final StalenessInformationProvider defaultStalenessInformationProvider =
            new StalenessInformationProvider() {
                @Override
                public TargetHealth getLastKnownTargetHealth(long targetOid) {
                    return DEFAULT_TARGET_HEALTH;
                }
            };

    /**
     * This is the "real" constructor. Intended to be invoked from a Spring configuration.
     *
     * @param concurrentPlansAllowed The number of concurrent plan pipelines that will be allowed to
     *                                   run. One pipeline per topology context ID. Must be
     *                                   positive.
     * @param maxQueuedPlansAllowed The number of maximum plans that can be queued up. Any additional
     *                              queue requests will result in a {@link QueueCapacityExceededException}.
     * @param topologyPipelineFactory {@link LivePipelineFactory}.
     * @param planPipelineFactory {@link PlanPipelineFactory}.
     * @param entityStore The {@link EntityStore} used as input to construct "live" topologies.
     * @param notificationSender The {@link TopologyProcessorNotificationSender} used to broadcast
     *                           notifications about successful/failed topology broadcasts.
     * @param targetStore The {@link TargetStore} containing target information.
     * @param clock The system clock.
     * @param maxBroadcastWait The maximum block time for broadcasts at startup.
     * @param timeUnit The time unit for the broadcast wait time.
     * @param componentRestartHelper helping decide whether we need to restart component
     */
    public TopologyPipelineExecutorService(final int concurrentPlansAllowed,
            final int maxQueuedPlansAllowed,
            @Nonnull final LivePipelineFactory topologyPipelineFactory,
            @Nonnull final PlanPipelineFactory planPipelineFactory,
            @Nonnull final EntityStore entityStore,
            @Nonnull final TopologyProcessorNotificationSender notificationSender,
            @Nonnull final TargetStore targetStore,
            @Nonnull final Clock clock,
            final long maxBroadcastWait,
            @Nonnull final TimeUnit timeUnit,
            @Nonnull final ComponentRestartHelper componentRestartHelper,
            @Nonnull final StalenessInformationProvider stalenessInformationProvider) {
        this(concurrentPlansAllowed, createPlanExecutorService(concurrentPlansAllowed), createRealtimeExecutorService(),
            new PlanPipelineQueue(clock, maxQueuedPlansAllowed),
            // We only expect one queued live pipeline, because we collapse all the other ones.
            new RealtimePipelineQueue(clock),
            topologyPipelineFactory,
            planPipelineFactory,
            entityStore,
            notificationSender,
            targetStore,
            maxBroadcastWait,
            timeUnit,
            componentRestartHelper,
            stalenessInformationProvider);
    }

    @VisibleForTesting
    TopologyPipelineExecutorService(final int concurrentPipelinesAllowed,
            final ExecutorService planExecutorService,
            final ExecutorService realtimeExecutorService,
            @Nonnull final PlanPipelineQueue planPipelineQueue,
            @Nonnull final RealtimePipelineQueue realtimePipelineQueue,
            @Nonnull final LivePipelineFactory topologyPipelineFactory,
            @Nonnull final PlanPipelineFactory planPipelineFactory,
            @Nonnull final EntityStore entityStore,
            @Nonnull final TopologyProcessorNotificationSender notificationSender,
            @Nonnull final TargetStore targetStore,
            final long maxBroadcastWait,
            @Nonnull final TimeUnit timeUnit,
            @Nonnull final ComponentRestartHelper componentRestartHelper,
            @Nonnull final StalenessInformationProvider stalenessInformationProvider) {
        this.planExecutorService = planExecutorService;
        this.realtimeExecutorService = realtimeExecutorService;
        this.livePipelineFactory = topologyPipelineFactory;
        this.planPipelineFactory = planPipelineFactory;
        this.entityStore = entityStore;
        this.realtimePipelineQueue = realtimePipelineQueue;
        this.planPipelineQueue = planPipelineQueue;
        this.targetStore = targetStore;
        this.stalenessInformationProvider = stalenessInformationProvider;
        for (int i = 0; i < concurrentPipelinesAllowed; ++i) {
            planExecutorService.submit(new TopologyPipelineWorker(planPipelineQueue,
                    notificationSender, componentRestartHelper));
        }
        realtimeExecutorService.submit(new TopologyPipelineWorker(realtimePipelineQueue,
                notificationSender, componentRestartHelper));

        // Block live broadcasts. At initialization time we will schedule a thread that will unblock
        // them when appropriate.
        //
        // These get unblocked by the PipelineUnblockLauncher, configured in Spring.
        blockBroadcasts(maxBroadcastWait, timeUnit);
    }

    /**
     * Block all broadcasts. Effective until
     * {@link TopologyPipelineExecutorService#unblockBroadcasts()} is called.
     *
     * <p/>Multiple calls do not stack. The reason for this is because the two main use cases for blocking broadcasts are:
     * 1) At startup, to prevent sending out inconsistent topologies.
     * 2) At diag loading time, to prevent sending out inconsistent topologies in the middle of
     *    diag loading.
     *
     * <p/>These two use cases SHOULDN'T stack on top of each other. When loading diags is complete
     * we should allow broadcasts, regardless of the startup blocking logic.
     *
     * @param blockTime The maximum time to block broadcasts for.
     * @param timeUnit The time unit for the block time.
     */
    public void blockBroadcasts(final long blockTime, @Nonnull final TimeUnit timeUnit) {
        realtimePipelineQueue.block(blockTime, timeUnit);
        planPipelineQueue.block(blockTime, timeUnit);
    }

    /**
     * Unblock all broadcasts, if they are currently blocked.
     */
    public void unblockBroadcasts() {
        realtimePipelineQueue.unblock();
        planPipelineQueue.unblock();
    }

    /**
     * Return whether or not broadcasts are blocked.
     *
     * @return True if broadcasts are blocked.
     */
    public boolean areBroadcastsBlocked() {
        return realtimePipelineQueue.isBlocked() || planPipelineQueue.isBlocked();
    }

    @VisibleForTesting
    static ExecutorService createPlanExecutorService(final int concurrentPipelinesAllowed) {
        Preconditions.checkArgument(concurrentPipelinesAllowed >= 1);
        return Executors.newFixedThreadPool(concurrentPipelinesAllowed,
            // Thread factory to set a pretty name.
            new ThreadFactoryBuilder()
                .setNameFormat("plan-pipeline-runner-%d")
                .build());
    }

    @VisibleForTesting
    static ExecutorService createRealtimeExecutorService() {
        return Executors.newSingleThreadExecutor(
            // Thread factory to set a pretty name.
            new ThreadFactoryBuilder()
                .setNameFormat("realtime-pipeline-runner")
                .build());
    }

    /**
     * Queue a "live" topology broadcast (i.e. the realtime broadcast). This does not queue a
     * broadcast if there is already a live broadcast queued, but it DOES queue a broadcast if there
     * is a live broadcast in progress.
     *
     * @param pendingTopologyInfo The {@link TopologyInfo} that serves as the starting point for the
     *                            topology to broadcast. Note - the {@link TopologyPipelineExecutorService}
     *                            is not responsible for validating the topology info. It trusts that
     *                            the caller formatted it correctly (e.g. setting the context to
     *                            the realtime context).
     * @param additionalBroadcastManagers Additional {@link TopoBroadcastManager} to allow callers
     *                                    to look at the resulting topology.
     * @param journalFactory The {@link StitchingJournalFactory} to use for the broadcast.
     * @return A {@link TopologyPipelineRequest} that can be used to wait for the broadcast.
     * @throws QueueCapacityExceededException If the live pipeline queue is full.
     */
    @Nonnull
    public TopologyPipelineRequest queueLivePipeline(
            @Nonnull final TopologyInfo pendingTopologyInfo,
            @Nonnull final List<TopoBroadcastManager> additionalBroadcastManagers,
            @Nonnull final StitchingJournalFactory journalFactory) throws QueueCapacityExceededException {
        final TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline =
                livePipelineFactory.liveTopology(pendingTopologyInfo, additionalBroadcastManagers, journalFactory);

        return realtimePipelineQueue.queuePipeline(pipeline::getTopologyInfo, () -> pipeline.run(
                PipelineInput
                        .builder()
                        .setEntityStore(entityStore)
                        .setStalenessProvider(stalenessInformationProvider)
                        .build()));
    }

    /**
     * Queue a "plan" topology broadcast.
     *
     * @param pendingTopologyInfo The {@link TopologyInfo} that serves as the starting point for the
     *                           topology to broadcast.
     * @param changes The {@link ScenarioChange}s to apply for the plan.
     * @param scope The {@link PlanScope} describing the scope of the plan.
     * @param userScopeEntityTypes A list of {@link EntityOids} by EntityType, describing the user scope.
     * @param journalFactory The {@link StitchingJournalFactory} to use for the broadcast.
     * @return A {@link TopologyPipelineRequest} that can be used to wait for the broadcast to complete.
     * @throws QueueCapacityExceededException If the live pipeline queue is full.
     */
    @Nonnull
    public TopologyPipelineRequest queuePlanPipeline(
            @Nonnull final TopologyInfo pendingTopologyInfo,
            @Nonnull final List<ScenarioChange> changes,
            @Nullable final PlanScope scope,
            @Nullable final Map<Integer, EntityOids> userScopeEntityTypes,
            @Nonnull final StitchingJournalFactory journalFactory) throws QueueCapacityExceededException {
        final TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline =
                planPipelineFactory.planOverLiveTopology(pendingTopologyInfo, changes, scope,
                        userScopeEntityTypes, journalFactory);
        return planPipelineQueue.queuePipeline(pipeline::getTopologyInfo, () -> pipeline.run(
                PipelineInput
                        .builder()
                        .setEntityStore(entityStore)
                        .setStalenessProvider(defaultStalenessInformationProvider)
                        .build()));
    }

    /**
     * Queue a "plan-over-plan" topology broadcast.
     *
     * @param oldTopologyId The ID of the topology to run this plan on top of. This is the main
     *                      difference between plan-over-plan and a regular plan. A regular plan
     *                      runs on top of the live topology.
     * @param pendingTopologyInfo The {@link TopologyInfo} that serves as the starting point for the
     *                           topology to broadcast.
     * @param changes The {@link ScenarioChange}s to apply for the plan.
     * @param scope The {@link PlanScope} describing the scope of the plan.
     * @return A {@link TopologyPipelineRequest} that can be used to wait for the broadcast to complete.
     * @throws QueueCapacityExceededException If the live pipeline queue is full.
     */
    @Nonnull
    public TopologyPipelineRequest queuePlanOverPlanPipeline(
            final long oldTopologyId,
            @Nonnull final TopologyInfo pendingTopologyInfo,
            @Nonnull final List<ScenarioChange> changes,
            @Nullable final PlanScope scope) throws QueueCapacityExceededException {
        final TopologyPipeline<Long, TopologyBroadcastInfo> pipeline =
            planPipelineFactory.planOverOldTopology(pendingTopologyInfo, changes, scope);
        return planPipelineQueue.queuePipeline(pipeline::getTopologyInfo,
            () -> pipeline.run(oldTopologyId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // We use shutdownNow instead of shutdown because pipelines can take a very long time,
        // and it doesn't make sense to wait for all queued broadcasts to finish before closing
        // the service.
        //
        // Note - sending a topology failure notification is part of the pipeline interrupt handling.
        planExecutorService.shutdownNow();
        realtimeExecutorService.shutdownNow();
    }

    /**
     * Utility interface to abstract away the differences between running live, plan, and
     * plan-over-plan topology pipelines.
     */
    @FunctionalInterface
    @VisibleForTesting
    interface TopologyPipelineRunnable {

        TopologyBroadcastInfo runPipeline() throws PipelineException, InterruptedException;
    }

    /**
     * Helper class to manage the queueing of {@link TopologyPipelineRequest}s by the
     * {@link TopologyPipelineExecutorService}, and their de-queueing by the
     * {@link TopologyPipelineWorker}s.
     */
    @VisibleForTesting
    @ThreadSafe
    static class TopologyPipelineQueue {

        private final AtomicLong blockQueueUntil = new AtomicLong(0);

        private final Clock clock;

        private final int capacity;

        /**
         * The main FIFO queue. This is where requests queued by
         * {@link TopologyPipelineQueue#queuePipeline(Supplier, TopologyPipelineRunnable)}
         * end up.
         */
        private final BlockingDeque<TopologyPipelineRequest> queuedRequests;

        TopologyPipelineQueue(@Nonnull final Clock clock,
                final int capacity) {
            this.clock = clock;
            this.capacity = capacity;
            queuedRequests = new LinkedBlockingDeque<>(capacity);
        }

        /**
         * Block all requests to the pipeline queue from returning. To unblock the queue, call
         * {@link TopologyPipelineQueue#unblock()}.
         *
         * @param blockTime The maximum time to block the pipeline for.
         * @param timeUnit The time unit for the block time.
         */
        void block(final long blockTime,
                   @Nonnull final TimeUnit timeUnit) {
            synchronized (blockQueueUntil) {
                blockQueueUntil.set(clock.millis() + timeUnit.toMillis(blockTime));
                blockQueueUntil.notifyAll();
            }
        }

        /**
         * Unblock queue requests, if they are blocked.
         */
        void unblock() {
            synchronized (blockQueueUntil) {
                blockQueueUntil.set(0);
                blockQueueUntil.notifyAll();
            }
        }

        /**
         * Return whether or not the queue is blocked.
         *
         * @return True if the queue is blocked. False otherwise.
         */
        boolean isBlocked() {
            synchronized (blockQueueUntil) {
                return blockQueueUntil.get() > clock.millis();
            }
        }

        private void waitForUnblock() throws InterruptedException {
            synchronized (blockQueueUntil) {
                while (isBlocked()) {
                    final long maxWaitMs = blockQueueUntil.get() - clock.millis();
                    blockQueueUntil.wait(maxWaitMs);
                }
            }
        }

        /**
         * Take the next request for the queue. This is meant to be used by the
         * {@link TopologyPipelineWorker} class. This is a blocking call - it will block
         * until a request is available.
         *
         * @return The next {@link TopologyPipelineRequest} the worker should execute.
         * @throws InterruptedException If the thread is interrupted while waiting.
         */
        @Nonnull
        TopologyPipelineRequest take() throws InterruptedException {
            // Wait for the next request from the main FIFO queue.
            waitForUnblock();
            final TopologyPipelineRequest req = queuedRequests.take();
            waitForUnblock();
            return req;
        }

        @Nonnull
        Optional<TopologyPipelineRequest> poll() {
            return Optional.ofNullable(queuedRequests.poll());
        }

        /**
         * Queue a {@link TopologyPipelineRunnable}. If a runnable for the same topology context is
         * already queued, this has no effect (and will return the {@link TopologyPipelineRequest} for
         * the already queued runnable).
         *
         * @param pendingTopologyInfo A function that returns the {@link TopologyInfo} associated with
         *                            the pipeline. We use a function because the running of the
         *                            pipeline can modify the internal {@link TopologyInfo}, and we want to
         *                            be able to access its final state.
         * @param execution The pipeline to run.
         * @return A {@link TopologyPipelineRequest} that callers can use to wait for the pipeline
         * to finish.
         * @throws QueueCapacityExceededException If the request could not be queued because the
         *  queue is full.
         */
        @Nonnull
        TopologyPipelineRequest queuePipeline(@Nonnull final Supplier<TopologyInfo> pendingTopologyInfo,
                                              @Nonnull final TopologyPipelineRunnable execution)
                throws QueueCapacityExceededException {
            final long contextId = pendingTopologyInfo.get().getTopologyContextId();
            final TopologyPipelineRequest request;
            synchronized (queuedRequests) {
                final Optional<TopologyPipelineRequest> existingRequest = queuedRequests.stream()
                    .filter(queuedReq -> queuedReq.getTopologyContextId() == contextId)
                    .findFirst();
                if (existingRequest.isPresent()) {
                    request = existingRequest.get();
                } else {
                    request = new TopologyPipelineRequest(execution, pendingTopologyInfo);
                    final boolean success = queuedRequests.offerLast(request);
                    if (!success) {
                        throw new QueueCapacityExceededException(this);
                    }
                }
            }
            return request;
        }

        /**
         * Get the capacity of the queue.
         *
         * @return The capacity.
         */
        public int getCapacity() {
            return capacity;
        }
    }

    /**
     * Exception thrown when a pipeline cannot be queued because the queue is full.
     * Try again later!
     */
    public static class QueueCapacityExceededException extends Exception {
        QueueCapacityExceededException(final TopologyPipelineQueue queue) {
            super(queue.getClass().getSimpleName() + " is full (capacity: " + queue.getCapacity()
                    + "), and cannot accept additional requests right now.");
        }
    }

    /**
     * A pipeline queue for plans. Using subclass for clarity in logs/debugging.
     */
    static class PlanPipelineQueue extends TopologyPipelineQueue {
        PlanPipelineQueue(@Nonnull final Clock clock, final int capacity) {
            super(clock, capacity);
        }
    }

    /**
     * A pipeline queue for realtime. Using subclass for clarity in logs/debugging.
     */
    static class RealtimePipelineQueue extends TopologyPipelineQueue {
        RealtimePipelineQueue(@Nonnull final Clock clock) {
            super(clock, 1);
        }
    }

    /**
     * A request for a topology pipeline (i.e. a topology broadcast). Allows users of
     * {@link TopologyPipelineExecutorService} to wait for queued pipelines to complete.
     */
    public static class TopologyPipelineRequest {

        private final CompletableFuture<TopologyBroadcastInfo> future = new CompletableFuture<>();

        private final TopologyPipelineRunnable pipelineRunnable;

        private final Supplier<TopologyInfo> pendingTopologyInfo;

        private final long topologyContextId;

        private final long topologyId;

        @VisibleForTesting
        TopologyPipelineRequest(@Nonnull final TopologyPipelineRunnable pipelineRunnable,
                               @Nonnull final Supplier<TopologyInfo> pendingTopologyInfo) {
            final TopologyInfo info = pendingTopologyInfo.get();
            this.topologyContextId = info.getTopologyContextId();
            this.topologyId = info.getTopologyId();
            this.pipelineRunnable = pipelineRunnable;
            this.pendingTopologyInfo = pendingTopologyInfo;
        }

        /**
         * Get the ID of the topology this pipeline is constructing and broadcasting.
         *
         * @return The topology OID.
         */
        public long getTopologyId() {
            return topologyId;
        }

        /**
         * Get the topology context ID of the topology this pipeline is constructing and broadcasting.
         *
         * @return The topology context OID.
         */
        public long getTopologyContextId() {
            return topologyContextId;
        }

        @Nonnull
        TopologyInfo getTopologyInfo() {
            return pendingTopologyInfo.get();
        }

        /**
         * Wait for the pipeline and broadcast to finish.
         *
         * @param timeToWait The time to wait.
         * @param timeUnit The time unit for the time to wait.
         * @return The {@link TopologyBroadcastInfo} of the broadcast topology.
         * @throws PipelineException If the pipeline terminates with an error.
         * @throws TimeoutException If the pipeline is still running after the specified timeout.
         * @throws InterruptedException If the thread is interrupted while waiting.
         */
        @Nonnull
        public TopologyBroadcastInfo waitForBroadcast(final long timeToWait,
                                                      @Nonnull final TimeUnit timeUnit)
                throws PipelineException, TimeoutException, InterruptedException {
            try {
                return future.get(timeToWait, timeUnit);
            } catch (ExecutionException e) {
                throw convertExecutionException(e);
            }
        }

        @Nonnull
        private PipelineException convertExecutionException(@Nonnull final ExecutionException e) {
            if (e.getCause() instanceof PipelineException) {
                return (PipelineException)e.getCause();
            } else if (e.getCause() instanceof InterruptedException) {
                // Don't set the interrupt status of the CURRENT thread, because it is the
                // internal thread (doing the broadcast) that got interrupted, not this one.
                // We re-throw it as a regular TopologyPipelineException.
                return new PipelineException("Pipeline thread interrupted: " + e.getMessage(),
                    e.getCause());
            } else {
                return new PipelineException("Pipeline terminated with unexpected exception", e.getCause());
            }
        }
    }

    /**
     * Runs on a thread in the internal executor. The worker is responsible for taking
     * queued {@link TopologyPipelineRequest}s from the {@link TopologyPipelineQueue}, running them,
     * recording the results, and sending notifications about successes/failures. It does so
     * repeatedly, and is not intended to terminate while the topology processor is up.
     */
    @VisibleForTesting
    static class TopologyPipelineWorker implements Runnable {
        private static final Logger logger = LogManager.getLogger();

        private final TopologyPipelineQueue queuedRequests;

        private final TopologyProcessorNotificationSender notificationSender;
        private final ComponentRestartHelper componentRestartHelper;

        @VisibleForTesting
        TopologyPipelineWorker(@Nonnull final TopologyPipelineQueue queuedRequests,
                @Nonnull final TopologyProcessorNotificationSender notificationSender,
                @Nonnull final ComponentRestartHelper componentRestartHelper) {
            this.queuedRequests = queuedRequests;
            this.notificationSender = notificationSender;
            this.componentRestartHelper = componentRestartHelper;
        }

        @VisibleForTesting
        void runPipeline(@Nonnull final TopologyPipelineRequest pipelineRequest)
                throws InterruptedException {
            try {
                final TopologyBroadcastInfo successfulBroadcast = pipelineRequest.pipelineRunnable.runPipeline();
                logger.debug("Successfully ran pipeline for context {} topology {}",
                    pipelineRequest.getTopologyContextId(), pipelineRequest.getTopologyId());
                // Notify listeners that the pipeline finished and was broadcast.
                notificationSender.broadcastTopologySummary(TopologySummary.newBuilder()
                    .setTopologyInfo(pipelineRequest.getTopologyInfo())
                    .setSuccess(TopologyBroadcastSuccess.getDefaultInstance())
                    .build());
                pipelineRequest.future.complete(successfulBroadcast);
                componentRestartHelper.updateResult(true);
            } catch (PipelineException e) {
                // If the pipeline fails with an internal error, we send a notification
                // over Kafka.
                sendFailureNotification(pipelineRequest.getTopologyInfo(), e.getCause());
                pipelineRequest.future.completeExceptionally(e);
                componentRestartHelper.updateResult(false);
            } catch (InterruptedException e) {
                sendFailureNotification(pipelineRequest.getTopologyInfo(), e);
                pipelineRequest.future.completeExceptionally(e);
                componentRestartHelper.updateResult(false);
                throw e;
            }
        }

        private void sendFailureNotification(@Nonnull final TopologyInfo topologyInfo,
                                             Throwable throwable) {
            logger.error("Failed to complete topology pipeline "
                    + "(context: {}, topology: {}) due to error: ",
                topologyInfo.getTopologyContextId(), topologyInfo.getTopologyId(), throwable);
            notificationSender.broadcastTopologySummary(TopologySummary.newBuilder()
                .setTopologyInfo(topologyInfo)
                .setFailure(TopologyBroadcastFailure.newBuilder()
                    .setErrorDescription(throwable.getMessage() == null ? "" : throwable.getMessage()))
                .build());
        }

        /**
         * Run the worker. This method never terminates unless the thread is interrupted.
         */
        @Override
        public void run() {
            while (true) {
                try {
                    final TopologyPipelineRequest future = queuedRequests.take();
                    runPipeline(future);
                } catch (InterruptedException e) {
                    logger.error("Pipeline worker interrupted! Exiting.", e);
                    return;
                } catch (Exception e) {
                    logger.error("Pipeline worker hit unexpected exception. Continuing.", e);
                    componentRestartHelper.updateResult(false);
                } catch (OutOfMemoryError oome) {
                    // we aren't looking to handle this error, just log it.
                    logger.error("Pipeline worker {} received OutOfMemoryError. This pipeline runner task will terminate.",
                            Thread.currentThread().getName(), oome);
                    throw oome;
                }
            }
        }
    }
}
