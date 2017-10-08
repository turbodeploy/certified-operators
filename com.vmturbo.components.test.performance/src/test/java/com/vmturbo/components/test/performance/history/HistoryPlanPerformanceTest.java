package com.vmturbo.components.test.performance.history;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Channel;
import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;
import com.vmturbo.history.component.api.impl.HistoryMessageReceiver;

/**
 * Performance tests for the history component.
 */
@Alert({
    "history_update_topology_duration_seconds_sum{topology_type='source',context_type='plan'}/10minutes",
    "history_update_topology_duration_seconds_sum{topology_type='projected',context_type='plan'}/10minutes",
    "history_update_price_index_duration_seconds_sum{context_type='plan'}",
    "history_get_stats_snapshot_duration_seconds_sum{context_type='plan'}",

    "jvm_memory_bytes_used_max"})
public class HistoryPlanPerformanceTest extends HistoryPerformanceTest {
    private final CompletableFuture<Long> statsAvailableFuture = new CompletableFuture<>();
    private StatsListener statsListener = new StatsListener(statsAvailableFuture);

    private final long PLAN_ID = 9999;

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
        .withComponentCluster(ComponentCluster.newBuilder()
            .withService(ComponentCluster.newService("history")
                .withConfiguration("topologyProcessorHost", ComponentUtils.getDockerHostRoute())
                .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                .withMemLimit(2, MetricPrefix.GIGA)
                .withHealthCheckTimeoutMinutes(10)
                .logsToLogger(logger)))
        .withStubs(ComponentStubHost.newBuilder()
            .withNotificationStubs(topologyProcessorStub, marketStub, priceIndexStub))
        .scrapeClusterAndLocalMetricsToInflux();

    @Before
    public void setup() {
        historyMessageReceiver = new HistoryMessageReceiver(
                componentTestRule.getCluster().getConnectionConfig("history"), threadPool);
        historyComponent =
                new HistoryComponentNotificationReceiver(historyMessageReceiver, threadPool);
        historyComponent.addStatsListener(statsListener);

        final Channel historyChannel = componentTestRule.getCluster().newGrpcChannel("history");
        statsService = StatsHistoryServiceGrpc.newBlockingStub(historyChannel);
    }

    @After
    public void teardown() {
        try {
            historyMessageReceiver.close();

            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in RepositoryPerformanceTest!", e);
        }
    }

    @Test
    public void test50kPlanTopology() throws Exception {
        executeTest(50_000, PLAN_ID);
    }

    @Test
    public void test100kPlanTopology() throws Exception {
        executeTest(100_000, PLAN_ID);
    }

    @Test
    public void test200kPlanTopology() throws Exception {
        executeTest(200_000, PLAN_ID);
    }

    @Override
    protected String getTestContextType() {
        return "plan";
    }

    @Override
    protected CompletableFuture<Long> getStatsAvailableFuture() {
        return statsAvailableFuture;
    }

    private static class StatsListener implements com.vmturbo.history.component.api.StatsListener {
        private final CompletableFuture<Long> statsAvailableFuture;

        public StatsListener(@Nonnull final CompletableFuture<Long> statsAvailableFuture) {
            this.statsAvailableFuture = Objects.requireNonNull(statsAvailableFuture);
        }

        @Override
        public void onStatsAvailable(@Nonnull final StatsAvailable statsAvailable) {
            statsAvailableFuture.complete(statsAvailable.getTopologyContextId());
        }
    }
}
