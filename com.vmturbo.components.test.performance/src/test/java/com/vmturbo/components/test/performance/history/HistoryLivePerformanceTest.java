package com.vmturbo.components.test.performance.history;

import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Channel;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;
import com.vmturbo.history.component.api.impl.HistoryMessageReceiver;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;

/**
 * Performance tests for the history component.
 */
@Alert({
    "history_update_topology_duration_seconds_sum{topology_type='source',context_type='live'}/10minutes",
    "history_update_topology_duration_seconds_sum{topology_type='projected',context_type='live'}/10minutes",
    "history_update_price_index_duration_seconds_sum{context_type='live'}",
    "history_get_stats_snapshot_duration_seconds_sum{context_type='live'}",
    "history_get_entity_stats_duration_seconds_sum",
    "jvm_memory_bytes_used_max"})
public class HistoryLivePerformanceTest extends HistoryPerformanceTest {
    private final CompletableFuture<Long> statsAvailableFuture = new CompletableFuture<>();
    private StatsListener statsListener = new StatsListener(statsAvailableFuture);
    private KafkaMessageConsumer messageConsumer;

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
        .withComponentCluster(ComponentCluster.newBuilder()
            .withService(ComponentCluster.newService("history")
                .withConfiguration("topologyProcessorHost", ComponentUtils.getDockerHostRoute())
                .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                    .withConfiguration("writeTopologyChunkSize", "1000") // default is 100
                    .withConfiguration("migrationTimeoutSeconds", "900") // 15 mins
                .withMemLimit(2048, MetricPrefix.MEGA)
                .withHealthCheckTimeoutMinutes(15)
                .logsToLogger(logger)))
        .withoutStubs()
        .scrapeClusterAndLocalMetricsToInflux();

    @Before
    public void setup() {
        messageConsumer = new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                "HistoryPerformanceTest");
        historyMessageReceiver = HistoryMessageReceiver.create(messageConsumer);
        historyComponent =
                new HistoryComponentNotificationReceiver(historyMessageReceiver, threadPool, 0);
        historyComponent.addListener(statsListener);
        messageConsumer.start();

        final Channel historyChannel = componentTestRule.getCluster().newGrpcChannel("history");
        statsService = StatsHistoryServiceGrpc.newBlockingStub(historyChannel);
    }

    @After
    public void teardown() {
        try {
            messageConsumer.close();
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in RepositoryPerformanceTest!", e);
        }
    }

    @Test
    public void test50kLiveTopology() throws Exception {
        executeTest(50_000, ComponentUtils.REALTIME_TOPOLOGY_CONTEXT);
    }

    @Test
    public void test100kLiveTopology() throws Exception {
        executeTest(100_000, ComponentUtils.REALTIME_TOPOLOGY_CONTEXT);
    }

    @Test
    public void test200kLiveTopology() throws Exception {
        executeTest(200_000, ComponentUtils.REALTIME_TOPOLOGY_CONTEXT, 30);
    }

    @Nonnull
    @Override
    protected KafkaMessageProducer getKafkaMessageProducer() {
        return componentTestRule.getKafkaMessageProducer();
    }

    @Nonnull
    @Override
    protected void broadcastSourceTopology(TopologyInfo topologyInfo, Collection<TopologyEntityDTO> topoDTOs)
        throws CommunicationException, InterruptedException, OversizedElementException {
        final TopologyBroadcast topologyBroadcast = tpSender.broadcastLiveTopology(topologyInfo);
        for (final TopologyEntityDTO entity: topoDTOs) {
            topologyBroadcast.append(entity);
        }
        topologyBroadcast.finish();
    }

    @Override
    protected String getTestContextType() {
        return "live";
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
