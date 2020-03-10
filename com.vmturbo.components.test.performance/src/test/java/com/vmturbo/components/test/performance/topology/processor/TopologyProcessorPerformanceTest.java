package com.vmturbo.components.test.performance.topology.processor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.stub.StreamObserver;
import io.prometheus.client.Summary;
import io.prometheus.client.Summary.Timer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition.EntityFilters.EntityFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.SearchParametersCollection;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.BindToGroupPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceImplBase;
import com.vmturbo.common.protobuf.search.Search.ComparisonOperator;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.NumericFilter;
import com.vmturbo.common.protobuf.search.Search.PropertyFilter.StringFilter;
import com.vmturbo.common.protobuf.search.Search.SearchFilter;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.StoppingCondition;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sdk.examples.stressProbe.StressAccount;
import com.vmturbo.topology.processor.api.AccountValue;
import com.vmturbo.topology.processor.api.DiscoveryStatus;
import com.vmturbo.topology.processor.api.EntitiesListener;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.ProbeListener;
import com.vmturbo.topology.processor.api.TargetData;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TargetListener;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

@Alert({
    "tp_broadcast_duration_seconds_sum/5minutes",
    "tp_discovery_duration_seconds_sum/5minutes"
})
public class TopologyProcessorPerformanceTest {

    private static final Logger logger = LogManager.getLogger();

    private TopologyServiceBlockingStub topologyService;

    private KafkaMessageConsumer kafkaMessageConsumer;

    private TopologyProcessor topologyProcessor;
    private IMessageReceiver<TopologyProcessorNotification> messageReceiver;
    private IMessageReceiver<Topology> liveTopologyReceiver;

    private ExecutorService threadPool = Executors.newCachedThreadPool();

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
            .withComponentCluster(ComponentCluster.newBuilder()
                .withService(ComponentCluster.newService("topology-processor")
                        .withConfiguration("groupHost", ComponentUtils.getDockerHostRoute())
                        .withMemLimit(4, MetricPrefix.GIGA)
                        .logsToLogger(logger))
                .withService(ComponentCluster.newService("mediation-stressprobe")
                        .withConfiguration("serverHttpPort", "8080")
                        .withConfiguration("consul_host", "consul")
                        .withConfiguration("consul_port", "8500")
                        .withConfiguration("serverGrpcPort", "9001")
                        .withMemLimit(2, MetricPrefix.GIGA)
                        .logsToLogger(logger)))
            .withStubs(ComponentStubHost.newBuilder()
                .withGrpcServices(new PolicyServiceStub()))
            .scrapeClusterAndLocalMetricsToInflux();

    private static final Summary TP_PERFORMANCE_TEST_SUMMARY = Summary.build()
        .name("tp_perftest_duration_seconds")
        .help("Duration of a topology processor performance test.")
        .labelNames("topology_size")
        .register();

    @Before
    public void setup() {
        final ComponentApiConnectionConfig connectionConfig =
                componentTestRule.getCluster().getConnectionConfig("topology-processor");
        kafkaMessageConsumer = new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                        "tp-performance-test");
        messageReceiver =
                kafkaMessageConsumer.messageReceiver(TopologyProcessorClient.NOTIFICATIONS_TOPIC,
                        TopologyProcessorNotification::parseFrom);
        liveTopologyReceiver =
                kafkaMessageConsumer.messageReceiver(TopologyProcessorClient.TOPOLOGY_LIVE,
                        Topology::parseFrom);
        topologyService = TopologyServiceGrpc.newBlockingStub(
                componentTestRule.getCluster().newGrpcChannel("topology-processor"));
        topologyProcessor = TopologyProcessorClient.rpcAndNotification(connectionConfig, threadPool,
                messageReceiver, liveTopologyReceiver, null, null);
        kafkaMessageConsumer.start();
    }

    @After
    public void teardown() {
        kafkaMessageConsumer.close();
        try {
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in TopologyProcessorPerformanceTest!", e);
        }
    }

    @Test
    public void testDiscoveryAndBroadcast10K() throws Exception {
        testDiscoveryAndBroadcast(10_000, TP_PERFORMANCE_TEST_SUMMARY.labels("10K").startTimer());
    }

    @Test
    public void testDiscoveryAndBroadcast25K() throws Exception {
        testDiscoveryAndBroadcast(25_000, TP_PERFORMANCE_TEST_SUMMARY.labels("25K").startTimer());
    }

    @Test
    public void testDiscoveryAndBroadcast50K() throws Exception {
        testDiscoveryAndBroadcast(50_000, TP_PERFORMANCE_TEST_SUMMARY.labels("50K").startTimer());
    }

    @Test
    public void testDiscoveryAndBroadcast75K() throws Exception {
        testDiscoveryAndBroadcast(75_000, TP_PERFORMANCE_TEST_SUMMARY.labels("75K").startTimer());
    }

    @Test
    public void testDiscoveryAndBroadcast100K() throws Exception {
        testDiscoveryAndBroadcast(100_000, TP_PERFORMANCE_TEST_SUMMARY.labels("100K").startTimer());
    }

    @Test
    public void testDiscoveryAndBroadcast200K() throws Exception {
        testDiscoveryAndBroadcast(200_000, TP_PERFORMANCE_TEST_SUMMARY.labels("200K").startTimer());
    }

    /**
     * 1. Connect a probe
     * 2. Add a target for that probe
     * 3. Wait for discovery to complete
     * 4. Send a topology broadcast and receive it.
     */
    private void testDiscoveryAndBroadcast(int topologySize, Timer timer) throws Exception {
        final CompletableFuture<Long> probeFuture = new CompletableFuture<>();
        topologyProcessor.addProbeListener(new TestProbeListener(probeFuture));
        checkIfProbeAlreadyRegistered(topologyProcessor, probeFuture);
        final long probeId = probeFuture.get(10, TimeUnit.MINUTES);
        logger.info("Probe registered with id {}", probeId);


        final CompletableFuture<TargetInfo> targetFuture = new CompletableFuture<>();
        final CompletableFuture<DiscoveryStatus> discoveryFuture = new CompletableFuture<>();
        topologyProcessor.addTargetListener(new TestTargetListener(targetFuture, discoveryFuture));

        // Now add the target for the registered probe.
        final StressAccount account = TopologyUtils.generateStressAccount(topologySize);
        topologyProcessor.addTarget(probeId, buildTargetData(account));
        final TargetInfo target = targetFuture.get(10, TimeUnit.MINUTES);
        logger.info("Added target with id {} for probe {}", target.getId(), target.getProbeId());

        // The added target should be automatically discovered. Wait for the discovery to complete.
        final DiscoveryStatus discovery = discoveryFuture.get(10, TimeUnit.MINUTES);
        logger.info("Discovery {} completed success={}", discovery.getId(), discovery.isSuccessful());

        // Request a broadcast and wait for it to complete.
        final CompletableFuture<Integer> entitiesFuture = new CompletableFuture<>();
        topologyProcessor.addLiveTopologyListener(new TestEntitiesListener(entitiesFuture));
        topologyService.requestTopologyBroadcast(TopologyBroadcastRequest.getDefaultInstance());
        int numEntitiesReceived = entitiesFuture.get(10, TimeUnit.MINUTES);
        logger.info("Received {} entities", numEntitiesReceived);
        timer.observeDuration();
    }

    private TargetData buildTargetData(@Nonnull final StressAccount account) {
        return new TargetData() {
            @Nonnull
            @Override
            public Set<AccountValue> getAccountData() {
                return new HashSet<>(Arrays.asList(
                    new TestAccountValue("targetId", "stress"),
                    new TestAccountValue("hostCount", String.valueOf(account.getHostCount())),
                    new TestAccountValue("vmCount", String.valueOf(account.getVmCount())),
                    new TestAccountValue("appCount", String.valueOf(account.getAppCompCount())),
                    new TestAccountValue("storageCount", String.valueOf(account.getStorageCount())),
                    new TestAccountValue("clusterCount", String.valueOf(account.getClusterCount())),
                    new TestAccountValue("dcCount", String.valueOf(account.getDcCount())),
                    new TestAccountValue("seed", String.valueOf(account.getSeed()))
                ));
            }
        };
    }

    /**
     * There is a chance the probe registration process has completed prior to our health check for the
     * stress probe container finishing. So check if the topology processor has already registered a probe.
     *
     * @param topologyProcessor The topology processor where the probe may have registered.
     * @param probeFuture The future to complete on probe registration.
     * @throws Exception If something goes wrong.
     */
    private void checkIfProbeAlreadyRegistered(@Nonnull final TopologyProcessor topologyProcessor,
                                               @Nonnull final CompletableFuture<Long> probeFuture) throws Exception {
        Set<ProbeInfo> probes = topologyProcessor.getAllProbes();
        if (probes.size() > 1) {
            throw new RuntimeException("There should not be multiple probes registered");
        } else if (probes.isEmpty()) {
            return; // No probes registered
        }

        // Complete the future with the registered probe
        probeFuture.complete(probes.iterator().next().getId());
    }

    private static class TestAccountValue implements AccountValue {
        final String name;
        final String value;

        public TestAccountValue(@Nonnull final String name, @Nonnull final String value) {
            this.name = name;
            this.value = value;
        }

        @Nonnull
        @Override
        public String getName() {
            return name;
        }

        @Nullable
        @Override
        public String getStringValue() {
            return value;
        }

        @Nullable
        @Override
        public List<List<String>> getGroupScopeProperties() {
            return Collections.emptyList();
        }
    }

    private static class TestProbeListener implements ProbeListener {
        private final CompletableFuture<Long> probeRegistrationFuture;

        public TestProbeListener(@Nonnull final CompletableFuture<Long> probeRegistrationFuture) {
            this.probeRegistrationFuture = probeRegistrationFuture;
        }

        public void onProbeRegistered(@Nonnull TopologyProcessorDTO.ProbeInfo probe) {
            probeRegistrationFuture.complete(probe.getId());
        }
    }

    private static class TestTargetListener implements TargetListener {
        private final CompletableFuture<TargetInfo> targetFuture;
        private final CompletableFuture<DiscoveryStatus> discoveryFuture;

        public TestTargetListener(@Nonnull final CompletableFuture<TargetInfo> targetFuture,
                                  @Nonnull final CompletableFuture<DiscoveryStatus> discoveryFuture) {
            this.targetFuture = targetFuture;
            this.discoveryFuture = discoveryFuture;
        }

        @Override
        public void onTargetAdded(@Nonnull TargetInfo target) {
            targetFuture.complete(target);
        }

        @Override
        public void onTargetDiscovered(@Nonnull DiscoveryStatus result) {
            if (result.isCompleted()) {
                discoveryFuture.complete(result);
            }
        }
    }

    private static class TestEntitiesListener implements EntitiesListener {
        private final CompletableFuture<Integer> entitiesFuture;

        public TestEntitiesListener(@Nonnull final CompletableFuture<Integer> entitiesFuture) {
            this.entitiesFuture = entitiesFuture;
        }

        @Override
        public void onTopologyNotification(TopologyInfo topologyInfo,
                                           @Nonnull RemoteIterator<Topology.DataSegment> topologyDTOs) {
            int entityCount = 0;
            while (topologyDTOs.hasNext()) {
                try {
                    for (Topology.DataSegment ignored : topologyDTOs.nextChunk()) {
                        entityCount++;
                    }
                } catch (Exception e) {
                    logger.error("Error during topology broadcast reading: ", e);
                    entitiesFuture.complete(-1);
                }
            }

            entitiesFuture.complete(entityCount);
        }
    }

    // All hosts with the number one in their display name.
    final Grouping hostsWithOne = Grouping.newBuilder()
            .setId(1234L)
            .setDefinition(GroupDefinition.newBuilder()
                    .setType(GroupType.COMPUTE_HOST_CLUSTER)
                    .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                    .setSearchParametersCollection(
                                            SearchParametersCollection.newBuilder()
                                                    .addSearchParameters(
                                                            SearchParameters.newBuilder()
                                                                    .setStartingFilter(
                                                                            PropertyFilter.newBuilder()
                                                                                    .setPropertyName(
                                                                                            "entityType")
                                                                                    .setNumericFilter(
                                                                                            NumericFilter
                                                                                                    .newBuilder()
                                                                                                    .setComparisonOperator(
                                                                                                            ComparisonOperator.EQ)
                                                                                                    .setValue(
                                                                                                            EntityType.PHYSICAL_MACHINE
                                                                                                                    .getNumber())))
                                                                    .addSearchFilter(
                                                                            SearchFilter.newBuilder()
                                                                                    .setPropertyFilter(
                                                                                            PropertyFilter
                                                                                                    .newBuilder()
                                                                                                    .setPropertyName(
                                                                                                            "displayName")
                                                                                                    .setStringFilter(
                                                                                                            StringFilter
                                                                                                                    .newBuilder()
                                                                                                                    .setStringPropertyRegex(
                                                                                                                            ".*1.*")))))
                                                    .build())))
                    .build())
            .build();

    // All virtual machines consuming from physical machines.
    final Grouping vmsOnHosts = Grouping.newBuilder()
            .setId(1234L)
            .setDefinition(GroupDefinition.newBuilder()
                    .setEntityFilters(EntityFilters.newBuilder()
                            .addEntityFilter(EntityFilter.newBuilder()
                                    .setEntityType(EntityType.PHYSICAL_MACHINE.getNumber())
                                    .setSearchParametersCollection(
                                            SearchParametersCollection.newBuilder()
                                                    .addSearchParameters(
                                                            SearchParameters.newBuilder()
                                                                    .setStartingFilter(
                                                                            PropertyFilter.newBuilder()
                                                                                    .setPropertyName(
                                                                                            "entityType")
                                                                                    .setNumericFilter(
                                                                                            NumericFilter
                                                                                                    .newBuilder()
                                                                                                    .setComparisonOperator(
                                                                                                            ComparisonOperator.EQ)
                                                                                                    .setValue(
                                                                                                            EntityType.PHYSICAL_MACHINE
                                                                                                                    .getNumber())))
                                                                    .addSearchFilter(
                                                                            SearchFilter.newBuilder()
                                                                                    .setTraversalFilter(
                                                                                            TraversalFilter
                                                                                                    .newBuilder()
                                                                                                    .setTraversalDirection(
                                                                                                            TraversalDirection.PRODUCES)
                                                                                                    .setStoppingCondition(
                                                                                                            StoppingCondition
                                                                                                                    .newBuilder()
                                                                                                                    .setStoppingPropertyFilter(
                                                                                                                            PropertyFilter
                                                                                                                                    .newBuilder()
                                                                                                                                    .setPropertyName(
                                                                                                                                            "entityType")
                                                                                                                                    .setNumericFilter(
                                                                                                                                            NumericFilter
                                                                                                                                                    .newBuilder()
                                                                                                                                                    .setComparisonOperator(
                                                                                                                                                            ComparisonOperator.EQ)
                                                                                                                                                    .setValue(
                                                                                                                                                            EntityType.VIRTUAL_MACHINE
                                                                                                                                                                    .getNumber())))))))))))
            .build();

    private static Policy bindToGroup(@Nonnull final Grouping consumerGroup,
                                      @Nonnull final Grouping providerGroup) {
        return Policy.newBuilder()
            .setPolicyInfo(PolicyInfo.newBuilder()
                .setBindToGroup(BindToGroupPolicy.newBuilder()
                    .setConsumerGroupId(consumerGroup.getId())
                    .setProviderGroupId(providerGroup.getId())))
            .build();
    }

    public class PolicyServiceStub extends PolicyServiceImplBase {
        @Override
        public void getAllPolicies(final PolicyDTO.PolicyRequest request,
                                   final StreamObserver<PolicyResponse> responseObserver) {
            responseObserver.onNext(
                PolicyResponse.newBuilder()
                    .setPolicy(bindToGroup(vmsOnHosts, hostsWithOne))
                    .build());
            responseObserver.onCompleted();
        }
    }
}
