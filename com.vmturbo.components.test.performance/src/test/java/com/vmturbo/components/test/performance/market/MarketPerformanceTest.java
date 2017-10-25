package com.vmturbo.components.test.performance.market;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.communication.TopologyProcessorStub;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentClient;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;

/**
 * Performance tests for the market component.
 */
@Alert({"mkt_analysis_duration_seconds_sum/10minutes", "jvm_memory_bytes_used_max"})
public class MarketPerformanceTest {

    private static final Logger logger = LogManager.getLogger();

    private TopologyProcessorStub topologyProcessorStub =
            new TopologyProcessorStub();

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
             .withComponentCluster(ComponentCluster.newBuilder()
                    .withService(ComponentCluster.newService("market")
                            .withConfiguration("topologyProcessorHost",
                                ComponentUtils.getDockerHostRoute())
                            .withMemLimit(4, MetricPrefix.GIGA)
                            .logsToLogger(logger)))
            .withStubs(ComponentStubHost.newBuilder()
                .withNotificationStubs(topologyProcessorStub))
            .scrapeClusterAndLocalMetricsToInflux();

    private MarketComponent marketComponent;
    private IMessageReceiver<ActionPlan> actionsReceiver;
    private IMessageReceiver<ProjectedTopology> projectedTopologyReceiver;
    private KafkaMessageConsumer kafkaMessageConsumer;

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    @Before
    public void setup() {
        final ComponentApiConnectionConfig connectionConfig =
                componentTestRule.getCluster().getConnectionConfig("market");
        kafkaMessageConsumer =
                new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                        "market-perf-test");
        actionsReceiver = kafkaMessageConsumer.messageReceiver(MarketComponentClient.ACTION_PLANS_TOPIC,ActionPlan::parseFrom);
        projectedTopologyReceiver = kafkaMessageConsumer.messageReceiver(MarketComponentClient.PROJECTED_TOPOLOGIES_TOPIC,ProjectedTopology::parseFrom);
        marketComponent = MarketComponentClient.rpcAndNotification(connectionConfig, threadPool,
                projectedTopologyReceiver, actionsReceiver);
    }

    @After
    public void teardown() {
        kafkaMessageConsumer.close();
        try {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in MarketPerformanceTest!", e);
        }
    }

    private void testTopology(int topologySize) throws Exception {
        final List<TopologyEntityDTO> topoDTOs = TopologyUtils.generateTopology(topologySize);

        final TopologyBroadcast topologyBroadcast =
                topologyProcessorStub.getBackend().broadcastTopology(
                        ComponentUtils.REALTIME_TOPOLOGY_CONTEXT, 10, TopologyType.REALTIME);
        topoDTOs.forEach(entity -> {
            try {
                topologyBroadcast.append(entity);
            } catch (InterruptedException e) {
                throw new RuntimeException("Broadcast interrupted.", e);
            }
        });
        topologyBroadcast.finish();

        final long start = System.currentTimeMillis();
        final CompletableFuture<ActionPlan> actionPlanFuture = new CompletableFuture<>();
        marketComponent.addActionsListener(new TestActionsListener(actionPlanFuture));
        final ActionPlan receivedActionPlan = actionPlanFuture.get(20, TimeUnit.MINUTES);

        logger.info("Took {} seconds to receive action plan of size {} for topology of size {}.",
                (System.currentTimeMillis() - start) / 1000.0f,
                receivedActionPlan.getActionCount(),
                topoDTOs.size());
    }

    @Test
    public void test10kTopology() throws Exception {
        testTopology(10000);
    }

    @Test
    public void test25kTopology() throws Exception {
        testTopology(25000);
    }

    @Test
    public void test50kTopology() throws Exception {
        testTopology(50000);
    }

    @Test
    public void test75kTopology() throws Exception {
        testTopology(75000);
    }

    @Test
    public void test100kTopology() throws Exception {
        testTopology(100000);
    }

    @Test
    public void test200kTopology() throws Exception {
        testTopology(200000);
    }

    private static class TestActionsListener implements ActionsListener {
        private final CompletableFuture<ActionPlan> actionPlanFuture;

        public TestActionsListener(@Nonnull final CompletableFuture<ActionPlan> actionPlanFuture) {
            this.actionPlanFuture = actionPlanFuture;
        }

        @Override
        public void onActionsReceived(@Nonnull final ActionPlan actionPlan) {
            actionPlanFuture.complete(actionPlan);
        }
    }
}
