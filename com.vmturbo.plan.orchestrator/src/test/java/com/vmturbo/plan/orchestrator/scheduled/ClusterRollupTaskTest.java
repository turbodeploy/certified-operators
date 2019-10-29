package com.vmturbo.plan.orchestrator.scheduled;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Optional;

import io.grpc.Status;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceImplBase;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.kvstore.KeyValueStore;

/**
 * Tests for the ClusterRollupTask - periodically scheduling the Cluster Rollup process.
 */
public class ClusterRollupTaskTest {

    private StatsHistoryServiceImplBase statsHistoryService = spy(new StatsHistoryServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private StatsHistoryServiceBlockingStub statsHistoryClient;
    private GroupServiceBlockingStub groupServiceClient;
    private ThreadPoolTaskScheduler threadPoolTaskScheduler = mock(ThreadPoolTaskScheduler.class);
    private CronTrigger cronTrigger = mock(CronTrigger.class);

    private KeyValueStore keyValueStore = mock(KeyValueStore.class);

    private MutableFixedClock clock = new MutableFixedClock(1_000_000);

    @Rule
    public GrpcTestServer grpcTestServer = GrpcTestServer.newServer(statsHistoryService, groupServiceSpy);

    private ClusterRollupTask clusterRollupTask;

    @Before
    public void init() throws Exception {
        statsHistoryClient = StatsHistoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel());
        clusterRollupTask = new ClusterRollupTask(statsHistoryClient,
            groupServiceClient, threadPoolTaskScheduler, cronTrigger, keyValueStore, clock);
    }

    /**
     * Test that the rollup task is scheduled by the Spring @Scheduled annotation.
     *
     * In this test we set the schedule to every 10 seconds and wait for 3 seconds.
     * Afterwards, we check an internal counter, since there is no output from this scheduler
     * process (currently).
     *
     * Warning: this is a time-dependent test. The time to wait, currently 3 seconds, may not be
     * sufficient in an overloaded build/test environment.
     */
    @Test
    public void testStartClusterRollupSchedule() throws Exception {
        // Arrange
        when(keyValueStore.get(any())).thenReturn(Optional.empty());

        when(cronTrigger.getExpression()).thenReturn("test-cron-expression");
        // schedule two executions and then "null" -> end of execution
        when(cronTrigger.nextExecutionTime(anyObject()))
                .thenReturn(new Date())
                .thenReturn(new Date())
                .thenReturn(null);

        // Act
        clusterRollupTask.initializeSchedule();

        final ArgumentCaptor<Runnable> initialExecutionCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPoolTaskScheduler).execute(initialExecutionCaptor.capture());

        // Do the thing we scheduled.
        initialExecutionCaptor.getValue().run();

        // Assert
        int count = clusterRollupTask.getRollupCount();
        // there's one rollup requested immediately, since we have no previous rollup time.
        Assert.assertEquals(1, count);
        verify(statsHistoryService).computeClusterRollup(anyObject(), anyObject());
        verify(groupServiceSpy, times(3)).getGroups(any());
        verify(keyValueStore).put(ClusterRollupTask.LAST_ROLLUP_TIME_KEY, clock.instant().toString());

        // Verify we scheduled with the right cron trigger.
        final ArgumentCaptor<Runnable> scheduledExecutionCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPoolTaskScheduler).schedule(scheduledExecutionCaptor.capture(), eq(cronTrigger));

        // Add another day to the clock.
        clock.addTime(1, ChronoUnit.DAYS);

        // Run the runnable manually - this should do another rollup.
        initialExecutionCaptor.getValue().run();

        // Running the rollup.
        Assert.assertEquals(2, clusterRollupTask.getRollupCount());
        verify(statsHistoryService, times(2)).computeClusterRollup(anyObject(), anyObject());
        verify(groupServiceSpy, times(6)).getGroups(any());

        verify(keyValueStore).put(ClusterRollupTask.LAST_ROLLUP_TIME_KEY, clock.instant().toString());
    }

    /**
     * Test that initializing the task does not trigger a rollup if a rollup
     * ran recently.
     */
    @Test
    public void testNoRescheduleRollupOnRestart() {
        // Arrange
        when(keyValueStore.get(ClusterRollupTask.LAST_ROLLUP_TIME_KEY))
            .thenReturn(Optional.of(clock.instant().minus(1, ChronoUnit.DAYS).toString()));

        when(cronTrigger.getExpression()).thenReturn("test-cron-expression");
        // schedule two executions and then "null" -> end of execution
        when(cronTrigger.nextExecutionTime(anyObject()))
            .thenReturn(new Date())
            .thenReturn(new Date())
            .thenReturn(null);

        // Act
        clusterRollupTask.initializeSchedule();

        verify(threadPoolTaskScheduler, never()).execute(any());

        // But we still schedule the next rollup with the cron trigger.
        // Let's try executing it to make sure it works.
        final ArgumentCaptor<Runnable> runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(threadPoolTaskScheduler).schedule(runnableCaptor.capture(), eq(cronTrigger));

        // Add another day to the clock.
        clock.addTime(1, ChronoUnit.DAYS);

        // Run the runnable manually - this should do another rollup.
        runnableCaptor.getValue().run();

        // Running the rollup.
        Assert.assertEquals(1, clusterRollupTask.getRollupCount());
        verify(statsHistoryService).computeClusterRollup(anyObject(), anyObject());
        verify(groupServiceSpy, times(3)).getGroups(any());

        verify(keyValueStore).put(ClusterRollupTask.LAST_ROLLUP_TIME_KEY, clock.instant().toString());
    }

    @Test
    public void testClusterRollupExceptionIsCaught() {
        when(groupServiceSpy.getGroupsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        try {
            clusterRollupTask.requestClusterRollup();
        } catch (RuntimeException e) {
            Assert.fail(
                    "Exception should have been caught during rollup request: " + e.getMessage());
        }
    }
}