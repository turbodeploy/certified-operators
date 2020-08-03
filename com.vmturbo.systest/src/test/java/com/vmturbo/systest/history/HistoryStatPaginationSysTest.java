package com.vmturbo.systest.history;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import io.grpc.Channel;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope.EntityList;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsRequest;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.ComponentUtils;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.history.component.api.HistoryComponentNotifications.HistoryComponentNotification;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.history.component.api.StatsListener;
import com.vmturbo.history.component.api.impl.HistoryComponentNotificationReceiver;
import com.vmturbo.history.component.api.impl.HistoryMessageReceiver;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.api.MarketKafkaSender;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.api.server.TopologyProcessorKafkaSender;
import com.vmturbo.topology.processor.api.server.TopologyProcessorNotificationSender;

public class HistoryStatPaginationSysTest {
    protected static final Logger logger = LogManager.getLogger();

    protected MarketNotificationSender marketSender;
    protected TopologyProcessorNotificationSender tpSender;

    protected HistoryComponentNotificationReceiver historyComponent;
    protected IMessageReceiver<HistoryComponentNotification> historyMessageReceiver;
    protected StatsHistoryServiceBlockingStub statsService;
    protected ExecutorService threadPool = Executors.newCachedThreadPool();
    private final CompletableFuture<Long> statsAvailableFuture = new CompletableFuture<>();
    private StatsListener statsListener = new TestStatsListener(statsAvailableFuture);
    private KafkaMessageConsumer messageConsumer;

    @Rule
    public ComponentTestRule componentTestRule = ComponentTestRule.newBuilder()
            .withComponentCluster(ComponentCluster.newBuilder()
                    .withService(ComponentCluster.newService("history")
                            .withConfiguration("topologyProcessorHost", ComponentUtils.getDockerHostRoute())
                            .withConfiguration("marketHost", ComponentUtils.getDockerHostRoute())
                            .withMemLimit(2048, MetricPrefix.MEGA)
                            .withHealthCheckTimeoutMinutes(15)
                            .logsToLogger(logger)))
            .withoutStubs()
            .noMetricsCollection();

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

        final KafkaMessageProducer messageProducer = componentTestRule.getKafkaMessageProducer();
        tpSender = TopologyProcessorKafkaSender.create(threadPool, messageProducer, new MutableFixedClock(1_000_000));
        marketSender = MarketKafkaSender.createMarketSender(messageProducer);
    }

    private static final double CAPACITY = 10_000.0;


    /**
     * Test paging through lots of stats.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testPaginateThroughStats() throws Exception {
        final long topologyContextId = ComponentUtils.REALTIME_TOPOLOGY_CONTEXT;
        final List<TopologyEntityDTO> entities = Lists.newArrayList(
                createNewEntity(1L, EntityType.PHYSICAL_MACHINE_VALUE,
                        Lists.newArrayList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.MEM_VALUE))
                                .setCapacity(CAPACITY)
                                .setUsed(1.0)
                                .build())),
                createNewEntity(2L, EntityType.PHYSICAL_MACHINE_VALUE,
                        Lists.newArrayList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.MEM_VALUE))
                                .setCapacity(CAPACITY)
                                .setUsed(2.0)
                                .build())),
                createNewEntity(3L, EntityType.PHYSICAL_MACHINE_VALUE,
                        Lists.newArrayList(CommoditySoldDTO.newBuilder()
                                .setCommodityType(CommodityType.newBuilder()
                                        .setType(CommodityDTO.CommodityType.MEM_VALUE))
                                .setCapacity(CAPACITY)
                                .setUsed(3.0)
                                .build())));
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(1L)
                .setTopologyType(TopologyType.REALTIME)
                .setCreationTime(System.currentTimeMillis())
                .build();

        // Send topology
        logger.info("Sending topology...");
        final TopologyBroadcast topologyBroadcast = tpSender.broadcastLiveTopology(topologyInfo);
        for (final TopologyEntityDTO entity: entities) {
            topologyBroadcast.append(entity);
        }
        topologyBroadcast.finish();

        // Send projected topology
        logger.info("Sending projected topology...");
        marketSender.notifyProjectedTopology(topologyInfo, 2L, entities.stream()
            .map(entity -> ProjectedTopologyEntity.newBuilder()
                .setEntity(entity)
                .setOriginalPriceIndex(1.0f)
                // Entity ID = price index.
                .setProjectedPriceIndex(entity.getOid())
                .build())
            .collect(Collectors.toList()),
            4L);


        logger.info("Waiting for stats to be available...");
        statsAvailableFuture.get(1, TimeUnit.MINUTES);

        // Now get the stats by memory.
        paginateThroughStats("Mem", false, entities.stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet()), Arrays.asList(3L, 2L, 1L));
        paginateThroughStats("Mem", true, entities.stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet()), Arrays.asList(1L, 2L, 3L));

        // What about price index?
        paginateThroughStats("priceIndex", false, entities.stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet()), Arrays.asList(3L, 2L, 1L));
        paginateThroughStats("priceIndex", true, entities.stream()
                .map(TopologyEntityDTO::getOid)
                .collect(Collectors.toSet()), Arrays.asList(1L, 2L, 3L));
    }

    private void paginateThroughStats(final String sortStatName,
                                      final boolean ascending,
                                      @Nonnull final Set<Long> entityIds,
                                      @Nonnull final List<Long> expectedOrder) {
        logger.info("Paginating through stats, sorted by {}, in {} order.",
                sortStatName, ascending ? "ascending" : "descending");
        final PaginationParameters paginationParams = PaginationParameters.newBuilder()
                .setLimit(1)
                .setAscending(ascending)
                .setOrderBy(OrderBy.newBuilder()
                        .setEntityStats(EntityStatsOrderBy.newBuilder()
                                .setStatName(sortStatName)))
                .build();
        GetEntityStatsRequest request = GetEntityStatsRequest.newBuilder()
                .setFilter(StatsFilter.newBuilder()
                        .setStartDate(System.currentTimeMillis() - TimeUnit.HOURS.toMillis(1))
                        .setEndDate(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1))
                        .addCommodityRequests(CommodityRequest.newBuilder()
                                .setCommodityName(sortStatName)))
                .setPaginationParams(paginationParams)
                .setScope(EntityStatsScope.newBuilder()
                        .setEntityList(EntityList.newBuilder()
                                .addAllEntities(entityIds)))
                .build();
        int pageNumber = 0;
        final List<Long> retrievedIds = new ArrayList<>(expectedOrder.size());
        String nextCursor = "";
        do {
            pageNumber++;
            final GetEntityStatsResponse response = statsService.getEntityStats(request);
            response.getEntityStatsList().stream()
                    .map(EntityStats::getOid)
                    .forEach(retrievedIds::add);
            nextCursor = response.getPaginationResponse().getNextCursor();
            logger.info("Fetched page {}. Next cursor is: {}", pageNumber, nextCursor);

            final GetEntityStatsRequest.Builder newReqBuilder = request.toBuilder();
            newReqBuilder.getPaginationParamsBuilder().setCursor(nextCursor);
            request = newReqBuilder.build();
        } while (!StringUtils.isEmpty(nextCursor));

        assertThat(retrievedIds, contains(expectedOrder.toArray()));
    }

    private TopologyEntityDTO createNewEntity(final long id,
                                              final int entityType,
                                              final List<CommoditySoldDTO> soldCommodities) {
        final TopologyEntityDTO.Builder entityBuilder = TopologyEntityDTO.newBuilder()
                .setOid(id)
                .setEntityType(entityType)
                .setDisplayName(EntityType.forNumber(entityType) + " - " + id)
                .addAllCommoditySoldList(soldCommodities);
        return entityBuilder.build();
    }

    private static class TestStatsListener implements StatsListener {
        private final CompletableFuture<Long> statsAvailableFuture;

        public TestStatsListener(@Nonnull final CompletableFuture<Long> statsAvailableFuture) {
            this.statsAvailableFuture = Objects.requireNonNull(statsAvailableFuture);
        }

        @Override
        public void onStatsAvailable(@Nonnull final StatsAvailable statsAvailable) {
            statsAvailableFuture.complete(statsAvailable.getTopologyContextId());
        }
    }
}
