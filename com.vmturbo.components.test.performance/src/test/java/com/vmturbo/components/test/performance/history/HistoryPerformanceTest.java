package com.vmturbo.components.test.performance.history;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.GetAveragedEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.chunking.OversizedElementException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.utils.TopologyUtils;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.api.MarketKafkaSender;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.server.TopologyProcessorKafkaSender;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

public abstract class HistoryPerformanceTest {
    protected static final Logger logger = LogManager.getLogger();

    protected MarketNotificationSender marketSender;
    protected TopologyProcessorNotificationSender tpSender;

    protected HistoryComponentNotificationReceiver historyComponent;
    protected IMessageReceiver<HistoryComponentNotification> historyMessageReceiver;
    protected StatsHistoryServiceBlockingStub statsService;
    protected ExecutorService threadPool = Executors.newCachedThreadPool();

    protected final long SOURCE_TOPOLOGY_ID = 1234;
    protected final long PROJECTED_TOPOLOGY_ID = 5678;
    protected final long CREATION_TIME = System.currentTimeMillis();
    protected final long ACTION_PLAN_ID = 9123;

    protected final TopologyInfo.Builder TOPOLOGY_INFO = TopologyInfo.newBuilder()
        .setTopologyId(SOURCE_TOPOLOGY_ID)
        .setCreationTime(CREATION_TIME);

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

    private static final List<CommodityRequest> COMMODITY_REQUESTS = STATS_TO_FETCH.stream()
            .map(name -> CommodityRequest.newBuilder()
                    .setCommodityName(name)
                    .build())
        .collect(Collectors.toList());

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

    protected abstract void broadcastSourceTopology(TopologyInfo topologyInfo,
                                                    Collection<TopologyEntityDTO> topoDTOs)
        throws CommunicationException, InterruptedException, OversizedElementException;

    public static final long DEFAULT_STATS_TIMEOUT_MINUTES = 10;

    @Before
    public void createSenders() {
        tpSender = TopologyProcessorKafkaSender.create(threadPool, getKafkaMessageProducer(), new MutableFixedClock(1_000_000));
        marketSender = MarketKafkaSender.createMarketSender(getKafkaMessageProducer());
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

        // Wait for stats to be available before fetching them.
        getStatsAvailableFuture().get(statsTimeoutMinutes, TimeUnit.MINUTES);
        fetchStats(topologyContextId);
        fetchEntityStats(topoDTOs);

        final long executionTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - startTime);
        logger.info("Took {} seconds to execute {} history performance test for {} entities",
            executionTimeSeconds, getTestContextType(), topologySize);
    }

    protected void sendTopology(@Nonnull final List<TopologyEntityDTO> topoDTOs,
            final long topologyContextId) throws Exception {
        logger.info("Sending {} entity topology...", topoDTOs.size());

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(SOURCE_TOPOLOGY_ID)
                .setTopologyType(TopologyType.PLAN)
                .setCreationTime(CREATION_TIME)
                .build();
        broadcastSourceTopology(topologyInfo, topoDTOs);
    }

    protected void sendProjectedTopology(@Nonnull final List<TopologyEntityDTO> topoDTOs,
                                         final long topologyContextId) throws Exception {
        logger.info("Sending {} entity projected {} topology...", topoDTOs.size(), getTestContextType());

        marketSender.notifyProjectedTopology(
                TOPOLOGY_INFO
                    .setTopologyContextId(topologyContextId)
                    .setTopologyType(ComponentUtils.topologyType(topologyContextId))
                    .build(),
                PROJECTED_TOPOLOGY_ID,
                Lists.transform(topoDTOs, entity -> ProjectedTopologyEntity.newBuilder()
                    .setEntity(entity)
                    .setProjectedPriceIndex(1.0)
                    .setOriginalPriceIndex(2.0)
                    .build()),
                ACTION_PLAN_ID);
    }

    protected void fetchStats(final long topologyContextId){
        final GetAveragedEntityStatsRequest.Builder requestBuilder =
            GetAveragedEntityStatsRequest.newBuilder()
                .setFilter(makeStatsFilter());
        if (topologyContextId != ComponentUtils.REALTIME_TOPOLOGY_CONTEXT) {
            requestBuilder.addEntities(topologyContextId);
        }

        final Iterable<StatSnapshot> fetchedStats =
                () -> statsService.getAveragedEntityStats(requestBuilder.build());

        final AtomicInteger counter = new AtomicInteger(0);
        StreamSupport.stream(fetchedStats.spliterator(), false)
            .forEach(action -> counter.getAndIncrement());

        logger.info("Fetched {} {} stats", counter.get(), getTestContextType());
    }

    @Nonnull
    private StatsFilter makeStatsFilter() {
        return StatsFilter.newBuilder()
            .setStartDate(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
            .setEndDate(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))
            .addAllCommodityRequests(COMMODITY_REQUESTS)
            .build();
    }

    protected void fetchEntityStats(@Nonnull final List<TopologyEntityDTO> topology) {
        final PaginationParameters paginationParams = PaginationParameters.newBuilder()
            .setLimit(50)
            .setAscending(false)
            .setOrderBy(OrderBy.newBuilder()
                .setEntityStats(EntityStatsOrderBy.newBuilder()
                    .setStatName("Mem")))
            .build();
        final GetEntityStatsRequest request = GetEntityStatsRequest.newBuilder()
            .setFilter(makeStatsFilter())
            .setPaginationParams(paginationParams)
            .setScope(EntityStatsScope.newBuilder()
                .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE))
            .build();

        final GetEntityStatsResponse response = statsService.getEntityStats(request);

        logger.info("Fetched first page of stats, with {} results.", response.getEntityStatsCount());
    }
}
