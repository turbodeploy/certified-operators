package com.vmturbo.components.test.performance.market;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.stub.StreamObserver;
import io.opentracing.SpanContext;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityCosts;
import com.vmturbo.common.protobuf.cost.Cost.ProjectedEntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceImplBase;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.market.component.api.ActionsListener;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.api.server.TopologyProcessorKafkaSender;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

/**
 * Performance tests for the market component.
 */
@Alert({
    "mkt_analysis_duration_seconds_sum/10minutes",
    "mkt_economy_scoping_duration_seconds_sum/90seconds",
    "mkt_economy_convert_to_traders_duration_seconds_sum/2minutes",
    "mkt_economy_convert_from_traders_duration_seconds/90seconds",
    "jvm_memory_bytes_used_max"})
public class MarketPerformanceTest {

    private static final Logger logger = LogManager.getLogger();

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
             .withComponentCluster(ComponentCluster.newBuilder()
                    .withService(ComponentCluster.newService("market")
                        .withConfiguration("groupHost", ComponentUtils.getDockerHostRoute())
                        .withConfiguration("topologyProcessorHost", ComponentUtils.getDockerHostRoute())
                        .withMemLimit(4, MetricPrefix.GIGA)
                        .logsToLogger(logger)))
        .withStubs(ComponentStubHost.newBuilder()
            .withGrpcServices(new GlobalSettingsStub(), new GetGroupsStub()))
        .scrapeClusterAndLocalMetricsToInflux();

    private MarketComponent marketComponent;
    private IMessageReceiver<ActionPlan> actionsReceiver;
    private IMessageReceiver<ProjectedTopology> projectedTopologyReceiver;
    private IMessageReceiver<ProjectedEntityCosts> projectedEntityCostReceiver;
    private IMessageReceiver<AnalysisSummary> analysisSummaryReceiver;
    private IMessageReceiver<AnalysisStatusNotification> analysisStatusReceiver;
    private IMessageReceiver<ProjectedEntityReservedInstanceCoverage> projectedEntityRiCoverageReceiver;
    private KafkaMessageConsumer kafkaMessageConsumer;

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    @Before
    public void setup() {
        kafkaMessageConsumer =
                new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                        "market-perf-test");
        actionsReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.ACTION_PLANS_TOPIC, ActionPlan::parseFrom);
        projectedTopologyReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_TOPOLOGIES_TOPIC,
                ProjectedTopology::parseFrom);
        projectedEntityCostReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.PROJECTED_ENTITY_COSTS_TOPIC,
                ProjectedEntityCosts::parseFrom);
        analysisSummaryReceiver = kafkaMessageConsumer.messageReceiver(
                MarketComponentNotificationReceiver.ANALYSIS_SUMMARY_TOPIC,
            AnalysisSummary::parseFrom);
        analysisStatusReceiver = kafkaMessageConsumer.messageReceiver(
                       MarketComponentNotificationReceiver.ANALYSIS_STATUS_NOTIFICATION_TOPIC,
                           AnalysisStatusNotification::parseFrom);
        marketComponent = new MarketComponentNotificationReceiver(projectedTopologyReceiver,
                projectedEntityCostReceiver, projectedEntityRiCoverageReceiver, actionsReceiver,
            null, analysisSummaryReceiver, analysisStatusReceiver, threadPool, 0);
        kafkaMessageConsumer.start();
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

    private void testTopology(@Nonnull final List<TopologyEntityDTO> topoDTOs,
                              @Nonnull final Optional<List<Long>> scopeSeedOids) throws Exception {
        final TopologyProcessorNotificationSender tpNotificationSender =
            TopologyProcessorKafkaSender.create(threadPool,
                componentTestRule.getKafkaMessageProducer(), new MutableFixedClock(1_000_000));

        final TopologyInfo.Builder topologyInfoBuilder = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .setTopologyId(10)
            .setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
            .setCreationTime(0);
        scopeSeedOids.ifPresent(topologyInfoBuilder::addAllScopeSeedOids);

        final TopologyBroadcast topologyBroadcast =
            tpNotificationSender.broadcastLiveTopology(topologyInfoBuilder.build());
        for (final TopologyEntityDTO entity : topoDTOs) {
            topologyBroadcast.append(entity);
        }
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

    private void testTopology(int topologySize) throws Exception {
        final List<TopologyEntityDTO> topoDTOs = TopologyUtils.generateTopology(topologySize);

        testTopology(topoDTOs, Optional.empty());
    }

    private void testScopedTopology(int topologySize) throws Exception {
        final List<TopologyEntityDTO> topoDTOs = TopologyUtils.generateTopology(topologySize);

        // Pick a random host cluster to scope to.
        final List<Long> seeds = scopeToClusterOfSize(topoDTOs, 12);
        logger.info("Scoped to cluster with {} hosts.", seeds.size());

        testTopology(topoDTOs, Optional.of(seeds));
    }

    private List<Long> scopeToClusterOfSize(@Nonnull final List<TopologyEntityDTO> topoDTOs,
                                            long targetSize) {
        final Map<CommoditySoldDTO, Long> clustersBySize = topoDTOs.stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .map(this::clusterCommodityFor)
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        logger.info("Total hosts: ", topoDTOs.stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .count());
        logger.info("Total hosts in clusters: ", topoDTOs.stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .map(this::clusterCommodityFor)
            .filter(Optional::isPresent)
            .count());

        // Select the cluster closest in size to the target size.
        final CommoditySoldDTO selectedClusterCommodity = clustersBySize.entrySet().stream()
            .sorted((o1, o2) -> {
                // Pick the one closer to the target size.
                return Long.compare(
                    Math.abs(o1.getValue() - targetSize),
                    Math.abs(o2.getValue() - targetSize)
                );
            }).findFirst().get().getKey();

        return topoDTOs.stream()
            .filter(entity -> entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE)
            .filter(host -> clusterCommodityFor(host)
                .filter(clusterCommodity -> clusterCommodity.equals(selectedClusterCommodity))
                .isPresent())
            .map(TopologyEntityDTO::getOid)
            .collect(Collectors.toList());
    }

    private Optional<CommoditySoldDTO> clusterCommodityFor(@Nonnull final TopologyEntityDTO entityDTO) {
        return entityDTO.getCommoditySoldListList().stream()
            .filter(commodity -> commodity.getCommodityType().getType() == CommodityType.CLUSTER_VALUE)
            .findFirst();
    }

    @Test
    public void test10kTopology() throws Exception {
        testTopology(10_000);
    }

    @Test
    public void test25kTopology() throws Exception {
        testTopology(25_000);
    }

    @Test
    public void test50kTopology() throws Exception {
        testTopology(50_000);
    }

    @Test
    public void test75kTopology() throws Exception {
        testTopology(75_000);
    }

    @Test
    public void test100kTopology() throws Exception {
        testTopology(100_000);
    }

    @Test
    public void test200kTopology() throws Exception {
        testTopology(200_000);
    }

    @Test
    public void test200kScopedTopology() throws Exception {
        testScopedTopology(200_000);
    }

    private static class TestActionsListener implements ActionsListener {
        private final CompletableFuture<ActionPlan> actionPlanFuture;

        public TestActionsListener(@Nonnull final CompletableFuture<ActionPlan> actionPlanFuture) {
            this.actionPlanFuture = actionPlanFuture;
        }

        @Override
        public void onActionsReceived(@Nonnull final ActionPlan actionPlan,
                                      @Nonnull final SpanContext tracingContext) {
            actionPlanFuture.complete(actionPlan);
        }
    }


    public class GlobalSettingsStub extends SettingServiceImplBase {
        @Override
        public void getGlobalSetting(final GetSingleGlobalSettingRequest request,
                                   final StreamObserver<GetGlobalSettingResponse> responseObserver) {
            responseObserver.onCompleted();
        }
    }

    public class GetGroupsStub extends GroupServiceImplBase {
        @Override
        public void getGroups(GroupDTO.GetGroupsRequest request,
                              StreamObserver<GroupDTO.Grouping> responseObserver) {
            responseObserver.onCompleted(); // Don't return any groups
        }
    }
}
