package com.vmturbo.topology.processor.topology.pipeline;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastSuccess;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineException;
import com.vmturbo.components.common.utils.ComponentRestartHelper;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.test.utils.FeatureFlagTestRule.FeatureFlagTest;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.staledata.StalenessInformationProvider;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyBroadcastInfo;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.PlanPipelineQueue;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.RealtimePipelineQueue;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineQueue;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineRequest;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineRunnable;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineWorker;

/**
 * Unit tests for {@link TopologyPipelineExecutorService}.
 */
public class TopologyPipelineExecutorServiceTest {

    private static final List<ScenarioChange> SCENARIO_CHANGES = Collections.singletonList(
        ScenarioChange.getDefaultInstance());

    private static final PlanScope PLAN_SCOPE = PlanScope.getDefaultInstance();

    private static final StitchingJournalFactory JOURNAL_FACTORY =
        mock(StitchingJournalFactory.class);

    private static final List<TopoBroadcastManager> BROADCAST_MANAGERS =
        Collections.singletonList(mock(TopoBroadcastManager.class));

    private PlanPipelineFactory mockPlanPipelineFactory = mock(PlanPipelineFactory.class);
    private LivePipelineFactory mockLivePipelineFactory = mock(LivePipelineFactory.class);

    private TopologyProcessorNotificationSender mockNotificationSender =
        mock(TopologyProcessorNotificationSender.class);

    private EntityStore mockEntityStore = mock(EntityStore.class);

    private ExecutorService mockPlanExecutorService = mock(ExecutorService.class);

    private ExecutorService mockRealtimeExecutorService = mock(ExecutorService.class);

    private PlanPipelineQueue mockPlanQueue = mock(PlanPipelineQueue.class);
    private RealtimePipelineQueue mockRealtimeQueue = mock(RealtimePipelineQueue.class);
    private TargetStore targetStore = mock(TargetStore.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private static final long MAX_BLOCK_TIME = 10_000;

    private static final int CONCURRENT_PLANS_ALLOWED = 2;

    private static final ComponentRestartHelper componentRestartHelper = new ComponentRestartHelper(6);

    private StalenessInformationProvider mockStalenessInformationProvider = mock(
            StalenessInformationProvider.class);

    private TopologyPipelineExecutorService pipelineExecutorService =
        new TopologyPipelineExecutorService(CONCURRENT_PLANS_ALLOWED,
            mockPlanExecutorService,
            mockRealtimeExecutorService,
            mockPlanQueue,
            mockRealtimeQueue,
            mockLivePipelineFactory,
            mockPlanPipelineFactory,
            mockEntityStore,
            mockNotificationSender,
            targetStore,
            MAX_BLOCK_TIME,
            TimeUnit.MILLISECONDS,
            componentRestartHelper,
            mockStalenessInformationProvider);

    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
        .setTopologyContextId(77)
        .build();

    /**
     * Rule to initialize FeatureFlags store.
     **/
    @Rule
    public FeatureFlagTestRule featureFlagTestRule = new FeatureFlagTestRule();

    /**
     * Captor for the {@link TopologyBroadcastInfo} submitted to the pipeline queue.
     */
    @Captor
    public ArgumentCaptor<TopologyPipelineRunnable> pipelineRunnableCaptor;

    /**
     * Captor for the {@link TopologyInfo} supplier submitted to the pipeline queue.
     */
    @Captor
    public ArgumentCaptor<Supplier<TopologyInfo>> infoSupplierCaptor;

    /**
     * Common setup to run before each test.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Test that closing the executor shuts down the internal threadpool.
     */
    @Test
    public void testCloseShutsDownExecutor() {
        pipelineExecutorService.close();
        verify(mockPlanExecutorService).shutdownNow();
        verify(mockRealtimeExecutorService).shutdownNow();
    }

    /**
     * Test that the right number of workers get submitted to the executor.
     */
    @Test
    public void testWorkerSubmission() {
        verify(mockPlanExecutorService, times(CONCURRENT_PLANS_ALLOWED))
            .submit(isA(TopologyPipelineWorker.class));
        verify(mockRealtimeExecutorService, times(1))
            .submit(isA(TopologyPipelineWorker.class));
    }

    /**
     * Verify that the executor created internally to accommodate concurrent pipelines is
     * neither too big nor too small.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testCreateExecutorAccomodatesPipelines() throws Exception {
        final ExecutorService executorSvc =
            TopologyPipelineExecutorService.createPlanExecutorService(CONCURRENT_PLANS_ALLOWED);
        verifyExecutor(executorSvc, CONCURRENT_PLANS_ALLOWED);
    }

    /**
     * Verify that the executor created internally to accommodate the realtime pipeline has room
     * for exactly one thread.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testCreateRealtimeExecutor() throws Exception {
        final ExecutorService executorSvc =
            TopologyPipelineExecutorService.createRealtimeExecutorService();
        verifyExecutor(executorSvc, 1);
    }

    private void verifyExecutor(@Nonnull final ExecutorService executorSvc,
                                final int expectedThreads) throws InterruptedException, TimeoutException, RetriableOperationFailedException {
        try {
            final Semaphore semaphore = new Semaphore(0);
            // The callable will block forever.
            final Callable<Void> callable = () -> {
                semaphore.acquire();
                return null;
            };

            // Submit one extra callable. We expect the CONCURRENT_PIPELINES_ALLOWED callables
            // to run, and the extra one to stay in the queue.
            for (int i = 0; i < expectedThreads + 1; ++i) {
                executorSvc.submit(callable);
            }

            // The executor might not IMMEDIATELY begin executing the queued tasks, so we
            // wait a little bit.
            int runningTasks = RetriableOperation.newOperation(semaphore::getQueueLength)
                .retryOnOutput(qLen -> qLen != expectedThreads)
                .backoffStrategy(curTry -> 10)
                .run(1, TimeUnit.MINUTES);

            // This means that the executor CAN accommodate the desired number of concurrent
            // pipelines, which is the most important thing to check.
            assertThat(runningTasks, is(expectedThreads));

            // Now we shutdown the executor, and verify that the "extra" task hasn't started
            // execution, which is somewhat convincing proof that the executor doesn't contain
            // EXTRA threads.
            List<Runnable> notStartedExecution = executorSvc.shutdownNow();
            // The 1 extra hasn't started execution.
            assertThat(notStartedExecution.size(), is(1));
        } finally {
            executorSvc.shutdownNow();
        }
    }

    /**
     * Test that the realtime queue doesn't fill up regardless of how many requests (with the
     * same context) we queue - even though the underlying queue capacity is 1. This is because
     * we collapse queued requests with the same context.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRealtimeQueueNeverFull() throws Exception {
        RealtimePipelineQueue realtimePipelineQueue = new RealtimePipelineQueue(clock);
        for (long i = 0; i < 100; ++i) {
            final long topologyId = i;
            realtimePipelineQueue.queuePipeline(() -> TopologyInfo.newBuilder()
                // The context ID remains the same every time.
                .setTopologyContextId(777)
                .setTopologyId(topologyId)
                .build(), () -> null);
        }
    }

    /**
     * Test running the "live" pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    @FeatureFlagTest(testCombos = {"USE_EXTENDABLE_PIPELINE_INPUT"})
    public void testRunLivePipeline() throws Exception {
        // Reset the Realtime Queue mock so that it can be used for all Feature Flag combinations.
        Mockito.reset(mockRealtimeQueue);
        final TopologyBroadcastInfo expectedBroadcastInfo = mock(TopologyBroadcastInfo.class);
        final TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline = mock(TopologyPipeline.class);
        when(pipeline.getTopologyInfo()).thenReturn(topologyInfo);
        when(pipeline.run(any(PipelineInput.class))).thenReturn(expectedBroadcastInfo);
        when(mockLivePipelineFactory.liveTopology(topologyInfo, BROADCAST_MANAGERS, JOURNAL_FACTORY))
            .thenReturn(pipeline);

        // Run
        pipelineExecutorService.queueLivePipeline(
            topologyInfo, BROADCAST_MANAGERS, JOURNAL_FACTORY);

        verify(mockRealtimeQueue).queuePipeline(infoSupplierCaptor.capture(), pipelineRunnableCaptor.capture());

        // Verify that the correct supplier and pipeline runnable got passed to the pipeline queue.
        assertThat(infoSupplierCaptor.getValue().get(), is(topologyInfo));
        assertThat(pipelineRunnableCaptor.getValue().runPipeline(), is(expectedBroadcastInfo));
    }

    /**
     * Test running the "plan" pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    @FeatureFlagTest(testCombos = {"USE_EXTENDABLE_PIPELINE_INPUT"})
    public void testRunPlanPipeline() throws Exception {
        // Reset the Plan Queue mock so that it can be used for all Feature Flag combinations.
        Mockito.reset(mockPlanQueue);
        final TopologyBroadcastInfo expectedBroadcastInfo = mock(TopologyBroadcastInfo.class);
        final TopologyPipeline<PipelineInput, TopologyBroadcastInfo> pipeline = mock(TopologyPipeline.class);
        when(pipeline.getTopologyInfo()).thenReturn(topologyInfo);
        when(pipeline.run(any(PipelineInput.class))).thenReturn(expectedBroadcastInfo);
        when(mockPlanPipelineFactory.planOverLiveTopology(topologyInfo, SCENARIO_CHANGES,
                PLAN_SCOPE, null, JOURNAL_FACTORY))
            .thenReturn(pipeline);

        // Run
        pipelineExecutorService.queuePlanPipeline(
            topologyInfo, SCENARIO_CHANGES, PLAN_SCOPE, null, JOURNAL_FACTORY);

        // Not realtime broadcast.
        verify(mockPlanQueue).queuePipeline(infoSupplierCaptor.capture(), pipelineRunnableCaptor.capture());

        // Verify that the correct supplier and pipeline runnable got passed to the pipeline queue.
        assertThat(infoSupplierCaptor.getValue().get(), is(topologyInfo));
        assertThat(pipelineRunnableCaptor.getValue().runPipeline(), is(expectedBroadcastInfo));
    }


    /**
     * Test running the "plan over plan" pipeline.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRunPlanOverPlanPipeline() throws Exception {
        final long oldTopology = 7129;
        final TopologyBroadcastInfo expectedBroadcastInfo = mock(TopologyBroadcastInfo.class);
        final TopologyPipeline<Long, TopologyBroadcastInfo> pipeline = mock(TopologyPipeline.class);
        when(pipeline.getTopologyInfo()).thenReturn(topologyInfo);
        when(pipeline.run(oldTopology)).thenReturn(expectedBroadcastInfo);
        when(mockPlanPipelineFactory.planOverOldTopology(topologyInfo, SCENARIO_CHANGES, PLAN_SCOPE))
            .thenReturn(pipeline);

        // Run
        pipelineExecutorService.queuePlanOverPlanPipeline(
            oldTopology, topologyInfo, SCENARIO_CHANGES, PLAN_SCOPE);

        // Not realtime broadcast.
        verify(mockPlanQueue).queuePipeline(infoSupplierCaptor.capture(), pipelineRunnableCaptor.capture());

        // Verify that the correct supplier and pipeline runnable got passed to the pipeline queue.
        assertThat(infoSupplierCaptor.getValue().get(), is(topologyInfo));
        assertThat(pipelineRunnableCaptor.getValue().runPipeline(), is(expectedBroadcastInfo));
    }

    /**
     * Test that queueing multiple times in the same topology context doesn't result in
     * multiple broadcasts.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testQueuedPlanMultipleTimesRunsOnce() throws Exception {
        final TopologyBroadcastInfo expectedBroadcastInfo = mock(TopologyBroadcastInfo.class);
        final TopologyPipelineQueue pipelineQueue = new TopologyPipelineQueue(clock, CONCURRENT_PLANS_ALLOWED);

        // Queue the same thing multiple times.
        pipelineQueue.queuePipeline(() -> topologyInfo, () -> expectedBroadcastInfo);
        pipelineQueue.queuePipeline(() -> topologyInfo, () -> expectedBroadcastInfo);
        pipelineQueue.queuePipeline(() -> topologyInfo, () -> expectedBroadcastInfo);

        final TopologyPipelineRequest queued = pipelineQueue.poll().get();
        assertThat(queued.getTopologyInfo(), is(topologyInfo));

        // No more in the queue.
        assertFalse(pipelineQueue.poll().isPresent());
    }

    /**
     * Test that queueing a broadcast for a context when there is already a running pipeline in
     * that context DOES result in another broadcast after the running one is finished.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineQueue() throws Exception {
        final TopologyBroadcastInfo expectedBroadcastInfo = mock(TopologyBroadcastInfo.class);
        final TopologyPipelineQueue pipelineQueue = new TopologyPipelineQueue(clock, CONCURRENT_PLANS_ALLOWED);

        final TopologyInfo otherTopologyInfo = topologyInfo.toBuilder()
            .setTopologyContextId(topologyInfo.getTopologyContextId() + 100)
            .build();

        pipelineQueue.queuePipeline(() -> topologyInfo, () -> expectedBroadcastInfo);
        pipelineQueue.queuePipeline(() -> otherTopologyInfo, () -> expectedBroadcastInfo);

        // Should be retrieved in FIFO order.
        assertThat(pipelineQueue.poll().get().getTopologyInfo(), is(topologyInfo));
        assertThat(pipelineQueue.poll().get().getTopologyInfo(), is(otherTopologyInfo));

        // No more in the queue.
        assertFalse(pipelineQueue.poll().isPresent());

        // Queue up something else to an empty queue.
        pipelineQueue.queuePipeline(() -> topologyInfo, () -> expectedBroadcastInfo);
        // Get it
        assertThat(pipelineQueue.poll().get().getTopologyInfo(), is(topologyInfo));
        // No more in the queue.
        assertFalse(pipelineQueue.poll().isPresent());
    }

    /**
     * Test that a blocked {@link TopologyPipelineQueue} unblocks after the specified block time
     * has passed.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testQueueBlockingExpires() throws Exception {
        // ARRANGE
        TopologyPipelineQueue pipelineQueue = new TopologyPipelineQueue(clock, 1);
        final long topologyId = 123;
        CompletableFuture<Void> waitForPipelineAndCheckResult =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return pipelineQueue.take().getTopologyId();
                    } catch (InterruptedException e) {
                        Assert.fail("test failed due to " + e);
                        return null;
                    }
                }).thenAccept(topologyIdResult -> assertEquals(topologyId, (long)topologyIdResult));

        // ACT - ASSERT

        // Block the pipeline and check that it is blocked
        pipelineQueue.block(10, TimeUnit.MILLISECONDS);
        assertTrue("pipeline should be blocked", pipelineQueue.isBlocked());

        // Queue a request and check that it is not executing
        pipelineQueue.queuePipeline(() -> TopologyInfo
                .newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(topologyId)
                .build(), () -> null);

        try {
            waitForPipelineAndCheckResult.get(1, TimeUnit.MILLISECONDS);
            Assert.fail("request should not run yet");
        } catch (TimeoutException e) {
        }

        // Time travel to when the pipeline is not blocked and assert that the result is correct
        clock.addTime(10, ChronoUnit.MILLIS);
        waitForPipelineAndCheckResult.get(1, TimeUnit.SECONDS);
    }

    /**
     * Test blocking and unblocking a {@link TopologyPipelineQueue}.
     *
     * @throws Exception If anything goes wrong.
     */
    @Test
    public void testQueueBlockUnblock() throws Exception {
        // ARRANGE
        TopologyPipelineQueue pipelineQueue = new TopologyPipelineQueue(clock, 1);
        final long topologyId = 123;
        CompletableFuture<Void> waitForPipelineAndCheckResult =
                CompletableFuture.supplyAsync(() -> {
                    try {
                        return pipelineQueue.take().getTopologyId();
                    } catch (InterruptedException e) {
                        Assert.fail("test failed due to " + e);
                        return null;
                    }
                }).thenAccept(topologyIdResult -> assertEquals(topologyId, (long)topologyIdResult));

        // ACT -ASSERT

        // Block the pipeline and check that it is blocked
        pipelineQueue.block(MAX_BLOCK_TIME, TimeUnit.MILLISECONDS);
        assertTrue(pipelineQueue.isBlocked());

        // Queue a request and check that it is not executing
        pipelineQueue.queuePipeline(() -> TopologyInfo.newBuilder()
                .setTopologyContextId(777)
                .setTopologyId(topologyId)
                .build(), () -> null);

        try {
            waitForPipelineAndCheckResult.get(1, TimeUnit.MILLISECONDS);
            Assert.fail("request should not run yet");
        } catch (TimeoutException e) {
        }

        // Unblock the pipeline and check that the request is processed correctly
        pipelineQueue.unblock();
        waitForPipelineAndCheckResult.get(1, TimeUnit.SECONDS);
    }

    /**
     * Test that an exception thrown by the pipeline gets re-thrown to the caller waiting
     * for the broadcast to finish.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineRequestPipelineException() throws Exception {
        final PipelineException exception =
            new PipelineException("BOO", new IllegalStateException("Boa"));

        final TopologyPipelineRunnable pipelineRunnable = mock(TopologyPipelineRunnable.class);
        when(pipelineRunnable.runPipeline()).thenThrow(exception);
        final TopologyPipelineRequest pipelineRequest =
            new TopologyPipelineRequest(pipelineRunnable, () -> topologyInfo);

        TopologyPipelineWorker worker = new TopologyPipelineWorker(mockRealtimeQueue,
                mockNotificationSender, componentRestartHelper);
        worker.runPipeline(pipelineRequest);

        try {
            pipelineRequest.waitForBroadcast(1, TimeUnit.MILLISECONDS);
            Assert.fail("Expected exception.");
        } catch (PipelineException e) {
            assertThat(e, is(exception));
        }
    }

    /**
     * Test that an interruption of the {@link TopologyPipelineRequest} throws the correct exception
     * back to the caller.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineRequestInterruptedException() throws Exception {
        final ExecutorService executorService = Executors.newSingleThreadExecutor();

        final TopologyPipelineRequest pipelineRequest =
            new TopologyPipelineRequest(mock(TopologyPipelineRunnable.class), () -> topologyInfo);

        final CompletableFuture<InterruptedException> future = new CompletableFuture<>();

        executorService.submit(() -> {
            try {
                pipelineRequest.waitForBroadcast(30, TimeUnit.MINUTES);
                future.completeExceptionally(new AssertionError("Expected interrupt."));
            } catch (InterruptedException e) {
                // This is expected.
                future.complete(e);
            } catch (Exception e) {
                future.completeExceptionally(e);
            }
        });

        // Shutdown now will interrupt the callable.
        executorService.shutdownNow();

        // Expect this to complete.
        future.get(10, TimeUnit.SECONDS);
    }

    /**
     * Test that running a topology pipeline triggers the sending of a notification.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineWorkerRunSendSummary() throws Exception {
        final TopologyBroadcastInfo expectedBroadcastInfo = mock(TopologyBroadcastInfo.class);
        final TopologyPipelineWorker worker =
            new TopologyPipelineWorker(mockRealtimeQueue, mockNotificationSender,
                    componentRestartHelper);
        TopologyPipelineRequest request =
            new TopologyPipelineRequest(() -> expectedBroadcastInfo, () -> topologyInfo);


        worker.runPipeline(request);

        // Request should have succeeded.
        assertThat(request.waitForBroadcast(1, TimeUnit.MILLISECONDS),
            is(expectedBroadcastInfo));

        verify(mockNotificationSender).broadcastTopologySummary(TopologySummary.newBuilder()
            .setTopologyInfo(topologyInfo)
            .setSuccess(TopologyBroadcastSuccess.getDefaultInstance())
            .build());
    }

    /**
     * Test that a failure in a pipeline triggers the sending of a notification about the failed
     * broadcast.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineFailSendSummary() throws Exception {
        final PipelineException exception =
            new PipelineException("Bad Pipe!", new Exception("exception"));

        final TopologyPipelineRequest request = new TopologyPipelineRequest(() -> {
                throw exception;
            }, () -> topologyInfo);


        // Run the erroneous pipeline!
        new TopologyPipelineWorker(mockRealtimeQueue, mockNotificationSender,
                componentRestartHelper).runPipeline(request);

        try {
            // Wait for results - should return immediately.
            request.waitForBroadcast(1, TimeUnit.MILLISECONDS);
            Assert.fail("Expected pipeline exception.");
        } catch (PipelineException e) {
            assertThat(e, is(exception));
            final ArgumentCaptor<TopologySummary> summaryCapture =
                ArgumentCaptor.forClass(TopologySummary.class);
            verify(mockNotificationSender).broadcastTopologySummary(summaryCapture.capture());
            TopologySummary broadcastSummary = summaryCapture.getValue();
            assertThat(broadcastSummary.getTopologyInfo(), is(topologyInfo));
            assertThat(broadcastSummary.getFailure().getErrorDescription(),
                containsString(exception.getCause().getMessage()));
        }
    }

    /**
     * Test that the interruption of a pipeline triggers the sending of a notification about the
     * failed broadcast.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPipelineWorkerInterruptedSendSummary() throws Exception {
        final InterruptedException exception = new InterruptedException("exception");

        final TopologyPipelineRequest request = new TopologyPipelineRequest(() -> {
            throw exception;
        }, () -> topologyInfo);


        // Run the erroneous pipeline!
        when(mockRealtimeQueue.take()).thenReturn(request);
        // This should terminate because of the interrupted exception.
        new TopologyPipelineWorker(mockRealtimeQueue, mockNotificationSender,
                componentRestartHelper).run();

        // Run
        try {
            request.waitForBroadcast(1, TimeUnit.MINUTES);
        } catch (PipelineException e) {
            if (e.getCause() instanceof InterruptedException) {
                assertThat(e.getCause(), is(exception));
                final ArgumentCaptor<TopologySummary> summaryCapture =
                    ArgumentCaptor.forClass(TopologySummary.class);
                verify(mockNotificationSender).broadcastTopologySummary(summaryCapture.capture());
                TopologySummary broadcastSummary = summaryCapture.getValue();
                assertThat(broadcastSummary.getTopologyInfo(), is(topologyInfo));
                assertThat(broadcastSummary.getFailure().getErrorDescription(),
                    containsString(exception.getMessage()));
            } else {
                throw e;
            }
        }
    }
}
