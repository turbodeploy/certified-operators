package com.vmturbo.components.test.performance.repository;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Channel;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryNotificationDTO.RepositoryNotification;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetSupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChain;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.BasicServiceHealthCheck;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.api.server.TopologyProcessorKafkaSender;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

/**
 * Performance tests for the repository component.
 */
@Alert({
    "repo_global_supply_chain_duration_seconds_sum",
    "repo_single_source_supply_chain_duration_seconds_sum",
    "repo_update_topology_duration_seconds_sum{topology_type='source'}/10minutes",
    "jvm_memory_bytes_used_max"})
public class RepositoryPerformanceTest {
    private static final Logger logger = LogManager.getLogger();

    private RepositoryNotificationReceiver repository;
    private SupplyChainServiceBlockingStub supplyChainService;
    private KafkaMessageConsumer kafkaConsumer;
    private IMessageReceiver<RepositoryNotification> messageReceiver;
    private ExecutorService threadPool = Executors.newCachedThreadPool();

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
        .withComponentCluster(ComponentCluster.newBuilder()
            .withService(ComponentCluster.newService("arangodb")
                .withMemLimit(2, MetricPrefix.GIGA)
                .withHealthCheck(new BasicServiceHealthCheck())
                .logsToLogger(logger))
            .withService(ComponentCluster.newService("repository")
                .withConfiguration("topologyProcessorHost", ComponentUtils.getDockerHostRoute())
                .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                .withMemLimit(4, MetricPrefix.GIGA)
                .logsToLogger(logger)))
        .withoutStubs()
        .scrapeServicesAndLocalMetricsToInflux("repository");

    @Before
    public void setup() {
        kafkaConsumer = new KafkaMessageConsumer(DockerEnvironment.getKafkaBootstrapServers(),
                "RepositoryPerformanceTest");
        messageReceiver = kafkaConsumer.messageReceiver(RepositoryNotificationReceiver
                .TOPOLOGY_TOPIC, RepositoryNotification::parseFrom);
        repository = new RepositoryNotificationReceiver(messageReceiver, threadPool, 0);
        kafkaConsumer.start();

        final Channel repositoryChannel = componentTestRule.getCluster().newGrpcChannel("repository");
        supplyChainService = SupplyChainServiceGrpc.newBlockingStub(repositoryChannel);
    }

    @After
    public void teardown() {
        try {
            kafkaConsumer.close();
            threadPool.shutdownNow();
            threadPool.awaitTermination(10, TimeUnit.MINUTES);
        } catch (Exception e) {
            logger.error("Failed to tear down in RepositoryPerformanceTest!", e);
        }
    }

    @Test
    public void testStoreAndRetrieve25k() throws Exception {
        testStoreTopologyAndGetSupplyChain(25_000);
    }

    @Test
    public void testStoreAndRetrieve50k() throws Exception {
        testStoreTopologyAndGetSupplyChain(50_000);
    }

    @Test
    public void testStoreAndRetrieve100k() throws Exception {
        testStoreTopologyAndGetSupplyChain(100_000);
    }

    @Test
    public void testStoreAndRetrieve200k() throws Exception {
        testStoreTopologyAndGetSupplyChain(200_000);
    }

    /**
     * Performance test the repository by sending it a topology to store and then
     * fetching the supply chain from it.
     *
     * @param topologySize The size of the topology to send.
     * @throws InterruptedException If interrupted.
     * @throws ExecutionException If the future execution fails.
     * @throws TimeoutException If the topology cannot be stored before a timeout expires.
     * @throws TimeoutException If communication error occurred
     * @throws CommunicationException If communication error occurred.
     * @throws OversizedElementException If an element in the topology is too large.
     */
    private void testStoreTopologyAndGetSupplyChain(final int topologySize)
        throws CommunicationException, InterruptedException, ExecutionException, TimeoutException, OversizedElementException {

        // Register for when the repository finishes storing the topology.
        final CompletableFuture<TopologyAndContext> topologyStoredFuture = new CompletableFuture<>();
        repository.addListener(new TestRepositoryListener(topologyStoredFuture));

        // Send a topology to the repo for storage.
        long datacenterId = sendTopology(topologySize);

        // Get the information about the topology that was just stored.
        final TopologyAndContext topologyAndContext = topologyStoredFuture.get(10, TimeUnit.MINUTES);

        // Request the global supply chain.
        getSupplyChain(Optional.empty(), topologyAndContext.getTopologyContextId(), "Global Supply Chain:\n");

        // Request the single source supply chain.
        getSupplyChain(Optional.of(datacenterId), topologyAndContext.getTopologyContextId(),
            "Single Source Supply Chain from Datacenter:\n");
    }

    private void getSupplyChain(@Nonnull final Optional<Long> startingEntityOid,
                                final long contextId,
                                @Nonnull final String message) {
        final GetSupplyChainRequest.Builder supplyChainRequest = GetSupplyChainRequest.newBuilder()
            .setContextId(contextId);
        startingEntityOid.ifPresent(startingEntity ->
            supplyChainRequest.getScopeBuilder().addStartingEntityOid(startingEntity));

        final SupplyChain supplyChain =
            supplyChainService.getSupplyChain(supplyChainRequest.build()).getSupplyChain();

        logger.info(message + supplyChain.getSupplyChainNodesList().stream()
            .map(supplyChainNode -> RepositoryDTOUtil.getMemberCount(supplyChainNode) + " " + supplyChainNode.getEntityType())
            .collect(Collectors.joining("\n")));
    }

    /**
     * Send a topology via the topologyProcessorStub, returning the ID of a datacenter in the topology.
     *
     * @param topologySize The size of the topology to send.
     * @return The id of the datacenter in the topology sent.
     * @throws InterruptedException If the operation is interrupted.
     * @throws CommunicationException if sending operation failed
     * @throws OversizedElementException If element in topology is too large.
     */
    private long sendTopology(final int topologySize)
        throws CommunicationException, InterruptedException, OversizedElementException {
        final List<TopologyEntityDTO> topoDTOs = TopologyUtils.generateTopology(topologySize);

        final TopologyProcessorNotificationSender topologySender = TopologyProcessorKafkaSender
                .create(threadPool, componentTestRule.getKafkaMessageProducer(), new MutableFixedClock(1_000_000));

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyId(10)
                .setTopologyContextId(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT)
                .setCreationTime(0)
                .build();

        final TopologyBroadcast topologyBroadcast =
                topologySender.broadcastLiveTopology(topologyInfo);
        for (TopologyEntityDTO entity : topoDTOs) {
            topologyBroadcast.append(entity);
        }
        topologyBroadcast.finish();

        return topoDTOs.stream()
            .filter(dto -> dto.getEntityType() == EntityType.DATACENTER.getNumber())
            .map(TopologyEntityDTO::getOid)
            .findFirst()
            .get();
    }

    /**
     * A helper that wraps up a topology id with its topology context id.
     */
    private static class TopologyAndContext {
        private final long topologyId;
        private final long topologyContextId;

        public TopologyAndContext(long topologyId, long topologyContextId) {
            this.topologyId = topologyId;
            this.topologyContextId = topologyContextId;
        }

        public long getTopologyId() {
            return topologyId;
        }

        public long getTopologyContextId() {
            return topologyContextId;
        }
    }

    private class TestRepositoryListener implements RepositoryListener {
        private final CompletableFuture<TopologyAndContext> topologyStoredFuture;

        public TestRepositoryListener(@Nonnull final CompletableFuture<TopologyAndContext> topologyStoredFuture) {
            this.topologyStoredFuture = Objects.requireNonNull(topologyStoredFuture);
        }

        @Override
        public void onSourceTopologyAvailable(long topologyId, long topologyContextId) {
            logger.info("onSourceTopologyAvailable topologyId {} contextId {}",
                topologyId, topologyContextId);
            topologyStoredFuture.complete(new TopologyAndContext(topologyId, topologyContextId));
        }

        @Override
        public void onSourceTopologyFailure(long topologyId, long topologyContextId,
                                            @Nonnull String failureDescription) {
            logger.info("onSourceTopologyFailure: : topologyId {} contextId {} failure: {}",
                topologyId, topologyContextId, failureDescription);
            topologyStoredFuture.completeExceptionally(
                new RuntimeException("Repository storing source topology failed"));
        }
    }
}
