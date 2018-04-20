package com.vmturbo.plan.orchestrator.scheduled;

import static com.vmturbo.common.protobuf.stats.Stats.ClusterRollupRequest;
import static com.vmturbo.common.protobuf.stats.Stats.ClusterRollupResponse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.Duration;
import java.util.Date;
import java.util.Optional;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceImplBase;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for the ClusterRollupTask - periodically scheduling the Cluster Rollup process.
 */
public class ClusterRollupTaskTest {

    // Time to sleep while waiting for the schedule to run: 3 seconds
    private static final long TEST_SLEEP_TIME = Duration.ofSeconds(3).toMillis();
    private static final String CRON_TRIGGER_SCHEDULE = "*/1 * * * * *";


    private StatsHistoryServiceImplBase statsHistoryService = spy(new StatsHistoryServiceImplBase() {
        @Override
        public void computeClusterRollup(ClusterRollupRequest request,
                StreamObserver<ClusterRollupResponse> responseObserver) {
            responseObserver.onNext(
                    ClusterRollupResponse.newBuilder().build());
            responseObserver.onCompleted();
        }
    });

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private StatsHistoryServiceBlockingStub statsHistoryClient;
    private GroupServiceBlockingStub groupServiceClient;
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private CronTrigger cronTrigger;

    @Rule
    public GrpcTestServer grpcTestServer =
            GrpcTestServer.newServer(statsHistoryService, groupServiceSpy);

    @Before
    public void init() throws Exception {
        statsHistoryClient = StatsHistoryServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        groupServiceClient = GroupServiceGrpc.newBlockingStub(grpcTestServer.getChannel());

        threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setThreadFactory(new ThreadFactoryBuilder()
                .setNameFormat("cluster-rollup-test-%d")
                .build());
        threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        threadPoolTaskScheduler.initialize();

        cronTrigger = Mockito.mock(CronTrigger.class);
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
        when(cronTrigger.getExpression()).thenReturn("test-cron-expression");
        // schedule two executions and then "null" -> end of execution
        when(cronTrigger.nextExecutionTime(anyObject()))
                .thenReturn(new Date())
                .thenReturn(new Date())
                .thenReturn(null);
        ClusterRollupTask clusterRollupTask = new ClusterRollupTask(statsHistoryClient,
                groupServiceClient, threadPoolTaskScheduler, cronTrigger);

        // Act
        clusterRollupTask.initializeSchedule();
        Thread.sleep(TEST_SLEEP_TIME);

        // Assert
        int count = clusterRollupTask.getRollupCount();
        // there's one rollup requested immediately and two scheduled rollups via mock scheduler
        Assert.assertEquals(3, count);
        verify(statsHistoryService, times(count)).computeClusterRollup(anyObject(), anyObject());
        verify(groupServiceSpy, times(count)).getGroups(any());
    }

    @Test
    public void testClusterRollupExceptionIsCaught() {
        when(groupServiceSpy.getGroupsError(any()))
            .thenReturn(Optional.of(Status.INTERNAL.asException()));

        ClusterRollupTask clusterRollupTask = new ClusterRollupTask(statsHistoryClient,
                groupServiceClient, threadPoolTaskScheduler, cronTrigger);

        try {
            clusterRollupTask.requestClusterRollup();
        } catch (RuntimeException e) {
            Assert.fail(
                    "Exception should have been caught during rollup request: " + e.getMessage());
        }
    }
}