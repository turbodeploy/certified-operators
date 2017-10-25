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
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import io.grpc.Channel;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChain.SupplyChainRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.alert.Alert;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.ServiceHealthCheck.BasicServiceHealthCheck;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.Repository;
import com.vmturbo.repository.api.RepositoryListener;
import com.vmturbo.repository.api.impl.RepositoryMessageReceiver;
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

    private Repository repository;
    private SupplyChainServiceBlockingStub supplyChainService;
    private RepositoryMessageReceiver messageReceiver;
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
        messageReceiver = new RepositoryMessageReceiver(
                componentTestRule.getCluster().getConnectionConfig("repository"), threadPool);
        repository = new RepositoryNotificationReceiver(messageReceiver, threadPool);

        final Channel repositoryChannel = componentTestRule.getCluster().newGrpcChannel("repository");
        supplyChainService = SupplyChainServiceGrpc.newBlockingStub(repositoryChannel);
    }

    @After
    public void teardown() {
        try {
            messageReceiver.close();

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
     */
    private void testStoreTopologyAndGetSupplyChain(final int topologySize)
        throws InterruptedException, ExecutionException, TimeoutException {

        // Register for when the repository finishes storing the topology.
        final CompletableFuture<TopologyAndContext> topologyStoredFuture = new CompletableFuture<>();
        repository.addListener(new TestRepositoryListener(topologyStoredFuture));

        // Send a topology to the repo for storage.
        long datacenterId = sendTopology(topologySize);

        // Get the information about the topology that was just stored.
        final TopologyAndContext topologyAndContext = topologyStoredFuture.get(10, TimeUnit.MINUTES);

        // Request the global supply chain.
        getSupplyChain(Optional.empty(), topologyAndContext.getTopologyContextId());

        // Request the single source supply chain.
        getSupplyChain(Optional.of(datacenterId), topologyAndContext.getTopologyContextId());
    }

    private void getSupplyChain(@Nonnull final Optional<Long> startingEntityOid, final long contextId) {
        try {
            final SupplyChainRequest.Builder supplyChainRequest = SupplyChainRequest.newBuilder()
                .setContextId(contextId);
            startingEntityOid.ifPresent(supplyChainRequest::addStartingEntityOid);

            final Iterable<SupplyChainNode> supplyChain =
                () -> supplyChainService.getSupplyChain(supplyChainRequest.build());

            StreamSupport.stream(supplyChain.spliterator(), false)
                .forEach(supplyChainNode ->
                    logger.info(supplyChainNode.getMemberOidsCount() + " " + supplyChainNode.getEntityType()));
        } catch (RuntimeException e) {
            // TODO: Remove the try/catch when the bug for single-source supply chain is fixed.
            // TODO: https://vmturbo.atlassian.net/browse/OM-19193
            logger.error("Exception while fetching supply chain: ", e);
        }
    }

    /**
     * Send a topology via the topologyProcessorStub, returning the ID of a datacenter in the topology.
     *
     * @param topologySize The size of the topology to send.
     * @return The id of the datacenter in the topology sent.
     * @throws InterruptedException If the operation is interrupted.
     */
    private long sendTopology(final int topologySize) throws InterruptedException {
        final List<TopologyEntityDTO> topoDTOs = TopologyUtils.generateTopology(topologySize);

        final TopologyProcessorNotificationSender topologySender = TopologyProcessorKafkaSender
                .create(threadPool, componentTestRule.getKafkaMessageProducer());
        final TopologyBroadcast topologyBroadcast =
                topologySender.broadcastTopology(ComponentUtils.REALTIME_TOPOLOGY_CONTEXT, 10,
                        TopologyType.REALTIME);
        topoDTOs.forEach(entity -> {
            try {
                topologyBroadcast.append(entity);
            } catch (InterruptedException e) {
                throw new RuntimeException("Broadcast interrupted.", e);
            }
        });
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
