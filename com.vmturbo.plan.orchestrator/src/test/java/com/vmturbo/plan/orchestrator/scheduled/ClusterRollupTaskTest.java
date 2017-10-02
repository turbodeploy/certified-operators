package com.vmturbo.plan.orchestrator.scheduled;

import static com.vmturbo.common.protobuf.stats.Stats.ClusterRollupRequest;
import static com.vmturbo.common.protobuf.stats.Stats.ClusterRollupResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import java.time.Duration;
import java.util.Date;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.scheduling.support.CronTrigger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.ClusterServiceGrpc;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceBlockingStub;
import com.vmturbo.common.protobuf.group.ClusterServiceGrpc.ClusterServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.Cluster;
import com.vmturbo.common.protobuf.group.GroupDTO.GetClustersRequest;
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


    private StatsHistoryServiceImplBase statsHistoryService;
    private StatsHistoryServiceBlockingStub statsHistoryClient;
    private ClusterServiceImplBase clusterService;
    private ClusterServiceBlockingStub clusterServiceClient;
    private ThreadPoolTaskScheduler threadPoolTaskScheduler;
    private CronTrigger cronTrigger;
    private final ClusterServiceHelper clusterServiceHelper = new ClusterServiceHelper();

    @Before
    public void init() throws Exception {
        statsHistoryService = Mockito.spy(new StatsHistoryServiceImplBase() {
            @Override
            public void computeClusterRollup(ClusterRollupRequest request,
                                             StreamObserver<ClusterRollupResponse> responseObserver) {
                responseObserver.onNext(
                        ClusterRollupResponse.newBuilder().build());
                responseObserver.onCompleted();
            }
        });
        GrpcTestServer statsHistoryRpc = GrpcTestServer.withServices(statsHistoryService);
        statsHistoryClient = StatsHistoryServiceGrpc.newBlockingStub(statsHistoryRpc.getChannel());

        clusterService = Mockito.spy(new ClusterServiceImplBase() {
            @Override
            public void getClusters(GetClustersRequest request,
                                    io.grpc.stub.StreamObserver<Cluster> responseObserver) {
                clusterServiceHelper.onGetClusters();
                responseObserver.onCompleted();
            }
        });
        GrpcTestServer clusterServiceRpc = GrpcTestServer.withServices(clusterService);
        clusterServiceClient = ClusterServiceGrpc.newBlockingStub(clusterServiceRpc.getChannel());

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
                clusterServiceClient, threadPoolTaskScheduler, cronTrigger);

        // Act
        clusterRollupTask.initializeSchedule();
        Thread.sleep(TEST_SLEEP_TIME);

        // Assert
        int count = clusterRollupTask.getRollupCount();
        // there's one rollup requested immediately and two scheduled rollups via mock scheduler
        assertThat(count).isEqualTo(3);
        verify(statsHistoryService, times(count)).computeClusterRollup(anyObject(), anyObject());
        verify(clusterService, times(count)).getClusters(anyObject(), anyObject());
    }

    @Test
    public void testClusterRollupExceptionIsCaught() {
        clusterServiceHelper.setRunner(() -> {
            throw new RuntimeException("Bad!");
        });

        ClusterRollupTask clusterRollupTask = new ClusterRollupTask(statsHistoryClient,
            clusterServiceClient, threadPoolTaskScheduler, cronTrigger);

        try {
            clusterRollupTask.requestClusterRollup();
        } catch (RuntimeException e) {
            fail("Exception should have been caught during rollup request: ", e);
        }
    }

    private static class ClusterServiceHelper {
        private Optional<Runnable> runner = Optional.empty();

        public void onGetClusters() {
            runner.ifPresent(Runnable::run);
        }

        public void setRunner(@Nonnull final Runnable runner) {
            this.runner = Optional.of(runner);
        }
    }
}