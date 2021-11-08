package com.vmturbo.topology.processor.staledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ConnectionTimeOutErrorType;
import com.vmturbo.topology.processor.rpc.TargetHealthRetriever;

/**
 * Tests the {@link StaleDataManager} class.
 */
public class StaleDataManagerTest {
private static final ErrorTypeInfo connectionTimeoutError = ErrorTypeInfo.newBuilder()
        .setConnectionTimeOutErrorType(ConnectionTimeOutErrorType.getDefaultInstance()).build();
    private static final Map<Long, TargetHealth> dummyHealths =
            new ImmutableMap.Builder<Long, TargetHealth>()
                    .put(1L, TargetHealth
                            .newBuilder()
                            .addErrorTypeInfo(connectionTimeoutError)
                            .setTargetName("target1")
                            .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                            .build())
                    .put(2L, TargetHealth
                            .newBuilder()
                            .addErrorTypeInfo(connectionTimeoutError)
                            .setTargetName("target1")
                            .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                            .build())
                    .build();

    /**
     * This is a Map that is used to store the health states that the active check is fetching.
     * It is initialized once and cleared after each test.
     */
    private final Map<Long, TargetHealth> results = new HashMap<>();

    /**
     * This latch is used to set the minimum number of executions of StaleDataActiveCheck in a test,
     * to avoid sleeping and non-determinism. It can be reset in the ARRANGE part of the test.
     */
    private CountDownLatch countDownLatch;

    @Mock
    private TargetHealthRetriever targetHealthRetriever;

    private ScheduledExecutorService executorService;

    private StaleDataManager staleDataManager;

    /**
     * Sets up:
     * - the executor service to a single threaded scheduler,
     * - the mock health retriever to return the dummy health data,
     * - the countDownLatch with a starting value of 1, meaning at least one execution of the active
     * check.
     */
    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        executorService = Executors.newSingleThreadScheduledExecutor();
        when(targetHealthRetriever.getTargetHealth(anySetOf(Long.class), anyBoolean())).thenReturn(
                dummyHealths);
        countDownLatch = new CountDownLatch(1);
    }

    /**
     * Shuts down the executor service and clears the results Map.
     */
    @After
    public void tearDown() {
        executorService.shutdownNow();
        results.clear();
    }

    /**
     * Tests that no task is running when the manager is initialized.
     *
     * @throws InterruptedException if the test gets interrupted
     */
    @Test
    public void shouldHaveInitialDelay() throws InterruptedException {
        // ACT
        staleDataManager =
                new StaleDataManager(Collections.singletonList(getInMemoryConsumerFactory()),
                        targetHealthRetriever, executorService, TimeUnit.MINUTES.toMillis(10));
        // ASSERT
        assertFalse("No tasks should run for the first 10 minutes",
                countDownLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue("Results should be empty", results.isEmpty());
    }

    private Supplier<StaleDataConsumer> getInMemoryConsumerFactory() {
        return () -> report -> {
            results.putAll(report);
            countDownLatch.countDown();
        };
    }

    /**
     * Tests that the correct data is pushed to the consumers.
     *
     * @throws InterruptedException if the test gets interrupted
     * @throws ExecutionException if an error occurs in the task
     */
    @Test
    public void shouldPushTheCorrectData() throws InterruptedException, ExecutionException {
        // ACT
        staleDataManager =
                new StaleDataManager(Collections.singletonList(getInMemoryConsumerFactory()),
                        targetHealthRetriever, executorService, TimeUnit.MILLISECONDS.toMillis(1));
        ScheduledFuture<?> scheduledFuture = staleDataManager.getScheduledFuture();
        try {
            assertTrue("a task should have executed", countDownLatch.await(1, TimeUnit.SECONDS));
            scheduledFuture.get(10, TimeUnit.MILLISECONDS);
            Assert.fail();
        } catch (TimeoutException ex) {
            // ASSERT
            assertEquals(dummyHealths, results);
        }
    }

    /**
     * Tests that when the schedule is canceled there are no more tasks running.
     */
    @Test
    public void shouldNotHaveTasksRunningAfterCancel() {
        // ACT
        staleDataManager =
                new StaleDataManager(Collections.singletonList(getInMemoryConsumerFactory()),
                        targetHealthRetriever, executorService, TimeUnit.MILLISECONDS.toMillis(1));
        ScheduledFuture<?> scheduledFuture = staleDataManager.getScheduledFuture();

        // ASSERT
        assertTrue(scheduledFuture.cancel(true)); // cancel the schedule and expect success
        assertFalse(scheduledFuture.cancel(true)); // cancel again and expect failure
    }

    /**
     * Tests that the scheduling does not stop if the task receives a RuntimeException.
     *
     * @throws InterruptedException if the test gets interrupted
     */
    @Test
    public void shouldNotStopSchedulingAfterRuntimeException() throws InterruptedException {
        // ARRANGE
        targetHealthRetriever = mock(TargetHealthRetriever.class);
        when(targetHealthRetriever.getTargetHealth(anySetOf(Long.class), anyBoolean())).thenThrow(
                new RuntimeException());

        // ACT
        staleDataManager =
                new StaleDataManager(Collections.singletonList(getInMemoryConsumerFactory()),
                        targetHealthRetriever, executorService, TimeUnit.MILLISECONDS.toMillis(1));
        ScheduledFuture<?> scheduledFuture = staleDataManager.getScheduledFuture();
        countDownLatch.await();

        // ASSERT
        assertFalse("should not stop scheduling after "
                        + ((Throwable)new RuntimeException()).getClass(),
                scheduledFuture.isDone() || scheduledFuture.isCancelled());
    }

    /**
     * Tests that the trigger operation works and that the scheduling is not blocked by it.
     *
     * @throws InterruptedException if the test gets interrupted
     */
    @Test
    public void shouldTriggerAnImmediateExecution() throws InterruptedException {
        // ACT
        staleDataManager =
                new StaleDataManager(Collections.singletonList(getInMemoryConsumerFactory()),
                        targetHealthRetriever, executorService, TimeUnit.MINUTES.toMillis(10));
        staleDataManager.notifyImmediately();

        // ASSERT
        assertTrue(countDownLatch.await(1, TimeUnit.SECONDS));
        assertEquals(dummyHealths, results);
    }

    /**
     * Tests that even if a consumer throws an exception we will keep scheduling.
     *
     * @throws ExecutionException if an error occurs in the task
     * @throws InterruptedException if the test gets interrupted
     * @throws TimeoutException if the scheduler has not broken down. This is the expected
     *         exception
     */
    @Test(expected = TimeoutException.class)
    public void shouldContinueSchedulingAfterExceptionInConsumer()
            throws ExecutionException, InterruptedException, TimeoutException {
        // ACT
        staleDataManager = new StaleDataManager(
                Collections.singletonList(getExceptionThrowingConsumerFactory()),
                targetHealthRetriever, executorService, TimeUnit.MILLISECONDS.toMillis(1));
        ScheduledFuture<?> scheduledFuture = staleDataManager.getScheduledFuture();
        countDownLatch.await();
        scheduledFuture.get(3, TimeUnit.MILLISECONDS);

        // ASSERT
        Assert.fail();
    }

    private Supplier<StaleDataConsumer> getExceptionThrowingConsumerFactory() {
        return () -> report -> {
            countDownLatch.countDown();
            throw new RuntimeException();
        };
    }
}