package com.vmturbo.components.test.performance.history;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import com.vmturbo.common.protobuf.stats.Stats.EntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.history.component.api.HistoryComponent;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.api.MarketKafkaSender;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessagePayload;
import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.api.server.TopologyProcessorKafkaSender;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

public abstract class HistoryPerformanceTest {
    protected static final Logger logger = LogManager.getLogger();

    private MarketNotificationSender marketSender;
    private TopologyProcessorNotificationSender tpSender;
    private PriceIndexNotificationSender piSender;

    protected HistoryComponent historyComponent;
    protected WebsocketNotificationReceiver historyMessageReceiver;
    protected StatsHistoryServiceBlockingStub statsService;
    protected ExecutorService threadPool = Executors.newCachedThreadPool();

    protected final long SOURCE_TOPOLOGY_ID = 1234;
    protected final long PROJECTED_TOPOLOGY_ID = 5678;
    protected final long CREATION_TIME = 6789;

    protected static final List<String> STATS_TO_FETCH = Arrays.asList(
        "Mem",
        "CPU",
        "VMem",
        "VCPU",
        "CPUAllocation",
        "MemAllocation",
        "CPUProvisioned",
        "MemProvisioned",
        "VCPUAllocation",
        "VMemAllocation"
    );

    /**
     * Plan/Live depending on what the test is testing.
     *
     * @return Either live or plan depending on the context for the test.
     */
    protected abstract String getTestContextType();

    /**
     * Get the future that can be used to block until stats are available.
     *
     * @return The future that can be used to block until stats are available.
     */
    protected abstract CompletableFuture<Long> getStatsAvailableFuture();

    @Nonnull
    protected abstract KafkaMessageProducer getKafkaMessageProducer();

    public static final long DEFAULT_STATS_TIMEOUT_MINUTES = 10;

    @Before
    public void createSenders() {
        tpSender = TopologyProcessorKafkaSender.create(threadPool, getKafkaMessageProducer());
        marketSender = MarketKafkaSender.createMarketSender(threadPool, getKafkaMessageProducer());
        piSender = MarketKafkaSender.createPriceIndexSender(getKafkaMessageProducer());
    }

    protected void executeTest(final int topologySize, final long topologyContextId) throws Exception {
        // Execute the test with a default timeout of 10 minutes.
        executeTest(topologySize, topologyContextId, DEFAULT_STATS_TIMEOUT_MINUTES);
    }

    protected void executeTest(final int topologySize, final long topologyContextId,
                               final long statsTimeoutMinutes) throws Exception {
        final long startTime = System.currentTimeMillis();
        final List<TopologyEntityDTO> topoDTOs = TopologyUtils.generateTopology(topologySize);

        sendTopology(topoDTOs, topologyContextId);
        sendProjectedTopology(topoDTOs, topologyContextId);
        sendPriceIndex(topoDTOs, topologyContextId);

        // Wait for stats to be available before fetching them.
        getStatsAvailableFuture().get(statsTimeoutMinutes, TimeUnit.MINUTES);
        fetchStats(topologyContextId);

        final long executionTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds to execute {} history performance test for {} entities",
            executionTimeSeconds, getTestContextType(), topologySize);
    }

    protected void sendTopology(@Nonnull final List<TopologyEntityDTO> topoDTOs,
                                final long topologyContextId) throws Exception {
        logger.info("Sending {} entity topology...", topoDTOs.size());

        final TopologyBroadcast topologyBroadcast =
                tpSender.broadcastTopology(topologyContextId, SOURCE_TOPOLOGY_ID,
                        ComponentUtils.topologyType(topologyContextId));
        topoDTOs.forEach(entity -> {
            try {
                topologyBroadcast.append(entity);
            } catch (InterruptedException e) {
                throw new RuntimeException("Broadcast interrupted.", e);
            }
        });
        topologyBroadcast.finish();
    }


    protected void sendProjectedTopology(@Nonnull final List<TopologyEntityDTO> topoDTOs,
                                         final long topologyContextId) throws Exception {
        logger.info("Sending {} entity projected {} topology...", topoDTOs.size(), getTestContextType());

        marketSender.notifyProjectedTopology(
            SOURCE_TOPOLOGY_ID, PROJECTED_TOPOLOGY_ID, topologyContextId,
            ComponentUtils.topologyType(topologyContextId), CREATION_TIME, topoDTOs);
    }

    protected void sendPriceIndex(@Nonnull final List<TopologyEntityDTO> topoDTOs,
                                  final long topologyContextId) throws Exception {
        logger.info("Sending price index for {} entities...", topoDTOs.size());

        final PriceIndexMessage.Builder builder = PriceIndexMessage.newBuilder()
            .setTopologyId(SOURCE_TOPOLOGY_ID)
            .setTopologyContextId(topologyContextId);
        topoDTOs.forEach(entity -> {
            builder.addPayload(PriceIndexMessagePayload.newBuilder()
                    .setOid(entity.getOid())
                    .setPriceindexCurrent(2.0)
                    .setPriceindexProjected(1.0)
            );
        });

        piSender.sendPriceIndex(SOURCE_TOPOLOGY_ID, CREATION_TIME, builder.build());
    }

    protected void fetchStats(final long topologyContextId) throws Exception {
        Iterable<StatSnapshot> fetchedStats = () -> statsService.getAveragedEntityStats(
            EntityStatsRequest.newBuilder()
                .addEntities(topologyContextId)
                .setFilter(StatsFilter.newBuilder()
                    .setStartDate(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
                    .setEndDate(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))
                    .addAllCommodityName(STATS_TO_FETCH))
                .build()
        );

        final AtomicInteger counter = new AtomicInteger(0);
        StreamSupport.stream(fetchedStats.spliterator(), false)
            .forEach(action -> counter.getAndIncrement());

        logger.info("Fetched {} {} stats", counter.get(), getTestContextType());
    }
}
