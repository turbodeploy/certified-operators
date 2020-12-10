package com.vmturbo.history.ingesters;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.utils.GuestLoadFilters;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.ImmutableTopologyIngesterConfig;
import com.vmturbo.history.ingesters.common.TopologyIngesterConfig;
import com.vmturbo.history.ingesters.live.ProjectedRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.live.SourceRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.live.writers.ClusterStatsWriter;
import com.vmturbo.history.ingesters.live.writers.EntitiesWriter;
import com.vmturbo.history.ingesters.live.writers.EntityStatsWriter;
import com.vmturbo.history.ingesters.live.writers.PriceIndexWriter;
import com.vmturbo.history.ingesters.live.writers.SystemLoadWriter;
import com.vmturbo.history.ingesters.live.writers.TopologyCommoditiesProcessor;
import com.vmturbo.history.ingesters.live.writers.VolumeAttachmentHistoryWriter;
import com.vmturbo.history.ingesters.plan.ProjectedPlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.SourcePlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.writers.PlanStatsWriter;
import com.vmturbo.history.ingesters.plan.writers.ProjectedPlanStatsWriter.Factory;
import com.vmturbo.history.listeners.ImmutableTopologyCoordinatorConfig;
import com.vmturbo.history.listeners.RollupProcessor;
import com.vmturbo.history.listeners.TopologyCoordinator;
import com.vmturbo.history.listeners.TopologyCoordinatorConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Spring configuration for topology ingestion components.
 */
@Configuration
@Import({HistoryApiConfig.class, TopologyProcessorClientConfig.class, GroupClientConfig.class})
public class IngestersConfig {
    @Autowired
    private StatsConfig statsConfig;

    @Autowired
    private HistoryApiConfig historyApiConfig;

    @Autowired
    private HistoryDbConfig historyDbConfig;

    @Autowired
    private HistoryApiConfig apiConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private TopologyProcessorClientConfig topologyClientConfig;

    @Value("${ingest.perChunkCommit:false}")
    private boolean perChunkCommit;

    @Value("${ingest.topologyStatusRetentionSecs:14400}") // 4 hrs
    private int topologyStatusRetentionSecs;

    @Value("${ingest.ingestionTimeoutSecs:1200}") // 20 mins
    private int ingestionTimeoutSecs;

    @Value("${ingest.hourlyRollupTimeoutSecs:3600}") // 1 hour
    private int hourlyRollupTimeoutSecs;

    @Value("${ingest.processingLoopMaxSleepSecs:60}") // 1 minute
    private int processingLoopMaxSleepSecs;

    @Value("${ingest.repartitioningTimeoutSecs:7200}") // 2 hours
    private int repartitioningTimeoutSecs;

    @Value("${ingest.excludedCommodities:#{null}}")
    private Optional<String> excludedCommodities;

    @Value("${ingest.volumeAttachmentHistoryIntervalBetweenInsertsInHours:6}")
    private long volumeAttachmentHistoryIntervalBetweenInsertsInHours;

    /**
     * One or more thread pools for use by ingesters.
     *
     * <p>By default, all ingesters share a common pool capped at 3 active threads. Separate
     * config properties bind each ingester with the thread pool it should use. In a properties
     * file, you can use the SpEL map literal syntax, as shown here: "{name: 'size', ...}", which
     * will pools of the given sizes to be associated with the given names. Don't forget to surround
     * the SpEL expression with double-quotes as a YAML property, else the YAM parser will try to
     * treat it as an inline JSON object.</p>
     */
    @Value("#{${ingest.threadPoolSpecs:{common:'3'}}}")
    private Map<String, Integer> threadPoolSpecs;

    // following four properties specify the name of the thread pool to be used by each ingester
    @Value("${ingest.sourceRealtimeThreadPool:common}")
    private String sourceRealtimePoolName;

    @Value("${ingest.projectedRealtimeThreadPool:common}")
    private String projectedRealtimePoolName;

    @Value("${ingest.sourcePlanThreadPool:common}")
    private String sourcePlanPoolName;

    @Value("${ingest.projectedPlanThreadPool:common}")
    private String projectedPlanPoolName;

    // per-ingester default per-chunk processing time limits, all defaulting to 1 minute
    @Value("${ingest.sourceRealtimeChunkTimeLimitMsec:60000}")
    private long sourceRealtimeChunkTimeLimit;

    @Value("${ingest.projectedRealtimeChunkTimeLimitMsec:60000}")
    private long projectedRealtimeChunkTimeLimit;

    @Value("${ingest.sourcePlanChunkTimeLimitMsec:60000}")
    private long sourcePlanChunkTimeLimit;

    @Value("${ingest.projectedPlanChunkTimeLimitMsec:60000}")
    private long projectedPlanChunkTimeLimit;

    @Value("${ingest.saveGuestLoadEntityStats:false}")
    private boolean saveGuestLoadEntityStats;

    @Bean
    MarketClientConfig marketClientConfig() {
        return new MarketClientConfig();
    }

    /**
     * Set up kafka subscriptions for topics published by topology processor.
     *
     * @return configured {@link TopologyProcessor} instance
     */
    @Bean
    TopologyProcessor liveTopologyProcessor() {
        final TopologyProcessor topologyProcessor = topologyClientConfig.topologyProcessor(
                TopologyProcessorSubscription.forTopic(Topic.LiveTopologies),
                TopologyProcessorSubscription.forTopic(Topic.PlanTopologies),
                TopologyProcessorSubscription.forTopic(Topic.TopologySummaries));
        topologyProcessor.addLiveTopologyListener(topologyCoordinator());
        topologyProcessor.addPlanTopologyListener(topologyCoordinator());
        topologyProcessor.addTopologySummaryListener(topologyCoordinator());
        return topologyProcessor;
    }

    /**
     * Set up subscriptions for kafka topics published by market component.
     *
     * @return A configured projected component listener.
     */
    @Bean
    MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig().marketComponent(
                MarketSubscription.forTopic(MarketSubscription.Topic.ProjectedTopologies),
                MarketSubscription.forTopic(MarketSubscription.Topic.PlanAnalysisTopologies),
                MarketSubscription.forTopic(MarketSubscription.Topic.AnalysisSummary));
        market.addProjectedTopologyListener(topologyCoordinator());
        market.addAnalysisSummaryListener(topologyCoordinator());
        return market;
    }

    /**
     * Create a {@link TopologyCoordinator} instance to manage processing of all topologies.
     *
     * @return topology coordinator instance
     */
    @Bean
    public TopologyCoordinator topologyCoordinator() {
        return new TopologyCoordinator(
                sourceRealtimeTopologyIngester(),
                projectedRealtimeTopologyIngester(),
                sourcePlanTopologyIngester(),
                projectedPlanTopologyIngester(),
                rollupProcessor(),
                historyApiConfig.statsAvailabilityTracker(),
                historyDbConfig.historyDbIO(),
                topologyCoordinatorConfig());
    }

    @Bean
    TopologyCoordinatorConfig topologyCoordinatorConfig() {
        return ImmutableTopologyCoordinatorConfig.builder()
                .topologyRetentionSecs(topologyStatusRetentionSecs)
                .ingestionTimeoutSecs(ingestionTimeoutSecs)
                .hourlyRollupTimeoutSecs(hourlyRollupTimeoutSecs)
                .repartitioningTimeoutSecs(repartitioningTimeoutSecs)
                .processingLoopMaxSleepSecs(processingLoopMaxSleepSecs)
                .build();
    }

    /**
     * Create an ingester to process live topologies from topology processor.
     *
     * @return new ingester
     */
    @Bean
    SourceRealtimeTopologyIngester sourceRealtimeTopologyIngester() {
        return new SourceRealtimeTopologyIngester(
                Arrays.asList(
                        new EntityStatsWriter.Factory(
                                historyDbConfig.historyDbIO(),
                                excludedCommodities(),
                                getEntitiesFilter()
                        ),
                        new SystemLoadWriter.Factory(
                                groupServiceBlockingStub(),
                                historyDbConfig.historyDbIO()
                        ),
                        new EntitiesWriter.Factory(
                                historyDbConfig.historyDbIO()
                        ),
                        new ClusterStatsWriter.Factory(
                                groupServiceBlockingStub()
                        ),
                        new VolumeAttachmentHistoryWriter.Factory(
                            volumeAttachmentHistoryIntervalBetweenInsertsInHours)
                ),
                ingesterConfig(TopologyIngesterType.sourceRealtime),
                bulkLoaderFactorySupplier()
        );
    }

    /**
     * Create an ingester to process projected live topologies from market component.
     *
     * @return new ingester
     */
    @Bean
    ProjectedRealtimeTopologyIngester projectedRealtimeTopologyIngester() {
        return new ProjectedRealtimeTopologyIngester(
                Arrays.asList(
                        new TopologyCommoditiesProcessor.Factory(
                                statsConfig.projectedStatsStore()
                        ),
                        new PriceIndexWriter.Factory(priceIndexVisitorFactory(),
                                getEntitiesFilter())
                ),
                ingesterConfig(TopologyIngesterType.projectedRealtime),
                bulkLoaderFactorySupplier()
        );
    }

    /**
     * Create a new ingester to process plan analysis topologies from market component.
     *
     * @return new ingester
     */
    @Bean
    SourcePlanTopologyIngester sourcePlanTopologyIngester() {
        return new SourcePlanTopologyIngester(
                Collections.singletonList(
                        new PlanStatsWriter.Factory(historyDbConfig.historyDbIO())
                ),
                ingesterConfig(TopologyIngesterType.sourcePlan),
                bulkLoaderFactorySupplier()
        );
    }

    /**
     * Create a new ingester to process projected plan analysis topologies from market component.
     *
     * @return new ingester
     */
    @Bean
    ProjectedPlanTopologyIngester projectedPlanTopologyIngester() {
        return new ProjectedPlanTopologyIngester(
                Collections.singletonList(
                        new Factory(historyDbConfig.historyDbIO())
                ),
                ingesterConfig(TopologyIngesterType.projectedPlan),
                bulkLoaderFactorySupplier()
        );
    }

    /**
     * Create ingester thread pools and associate them with their configured names, for binding to
     * individual ingesters.
     *
     * @return map of pool name to pool
     */
    @Bean
    Map<String, ExecutorService> ingesterThreadPools() {
        return threadPoolSpecs.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(Entry::getKey,
                        e -> createThreadPool(e.getKey(), e.getValue())));
    }

    private static ExecutorService createThreadPool(String name, int size) {
        final ThreadFactory factory =
                new ThreadFactoryBuilder().setNameFormat("ingester-" + name + "-%d").build();
        return Executors.newFixedThreadPool(size, factory);
    }

    /**
     * Get the thread pool for the given ingester.
     *
     * @param type ingester type
     * @return thread pool for ingester
     */
    private ExecutorService ingesterThreadPool(TopologyIngesterType type) {
        final Map<String, ExecutorService> pools = ingesterThreadPools();
        switch (type) {
            case sourceRealtime:
                return pools.get(sourceRealtimePoolName);
            case projectedRealtime:
                return pools.get(projectedRealtimePoolName);
            case sourcePlan:
                return pools.get(sourcePlanPoolName);
            case projectedPlan:
                return pools.get(projectedPlanPoolName);
            default:
                throw new IllegalArgumentException("Unknown TopologyIngesterType: " + type.name());
        }
    }

    /**
     * Partially build an ingester config object to be used as a starting point for all ingesters.
     *
     * @return partially built shared ingester config
     */
    @Bean
    ImmutableTopologyIngesterConfig.Builder ingesterConfigBase() {
        return ImmutableTopologyIngesterConfig.builder()
                .perChunkCommit(perChunkCommit);
    }

    /**
     * Create the ingester config for the given ingester type.
     *
     * @param type ingester type
     * @return ingester config
     */
    private TopologyIngesterConfig ingesterConfig(TopologyIngesterType type) {
        return ingesterConfigBase()
                // add ingester-specific items to the base
                .threadPool(ingesterThreadPool(type))
                .defaultChunkTimeLimitMsec(chunkTimeLimit(type))
                .build();
    }

    /**
     * Get the per-chunk time limit for the given ingester type.
     *
     * @param type ingester type
     * @return per-chunk time limit
     */
    private long chunkTimeLimit(TopologyIngesterType type) {
        switch (type) {
            case sourceRealtime:
                return sourceRealtimeChunkTimeLimit;
            case projectedRealtime:
                return projectedRealtimeChunkTimeLimit;
            case sourcePlan:
                return sourcePlanChunkTimeLimit;
            case projectedPlan:
                return projectedPlanChunkTimeLimit;
            default:
                throw new IllegalArgumentException("Unknown TopologyIngesterType: " + type.name());
        }
    }

    /**
     * Create a rollup processor to perform rollups for topology coordinator.
     *
     * @return new rollup processor
     */
    @Bean
    RollupProcessor rollupProcessor() {
        return new RollupProcessor(
                historyDbConfig.historyDbIO(), historyDbConfig.bulkLoaderThreadPool());
    }

    /**
     * Create a group service access point for use by hte system load writer.
     *
     * @return new group service endpoint
     */
    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Create a string suitable as a value for the `ingest.excludedProperties` config
     * parameter, representing a reasonable baked-in default.
     *
     * @return default excluded properties
     */
    private static String defaultExcludedCommodities() {
        // TODO this should probably be based on UICommodity rather than CommodityTypeUnits
        final CommodityTypeUnits[] commodities = new CommodityTypeUnits[]{
                CommodityTypeUnits.APPLICATION,
                CommodityTypeUnits.CLUSTER,
                CommodityTypeUnits.DATACENTER,
                CommodityTypeUnits.DATASTORE,
                CommodityTypeUnits.DRS_SEGMENTATION,
                CommodityTypeUnits.DSPM_ACCESS,
                CommodityTypeUnits.NETWORK,
                CommodityTypeUnits.SEGMENTATION,
                CommodityTypeUnits.STORAGE_CLUSTER,
                CommodityTypeUnits.VAPP_ACCESS,
                CommodityTypeUnits.VDC,
                CommodityTypeUnits.VMPM_ACCESS,
                CommodityTypeUnits.CONCURRENT_WORKER,
                CommodityTypeUnits.CONCURRENT_SESSION
        };
        return Arrays.stream(commodities)
                .map(CommodityTypeUnits::getMixedCase)
                .collect(Collectors.joining(" "));
    }

    /**
     * Create set of excluded commodities by parsing the string property value, used by the entity
     * stats writer in the live topology ingester.
     *
     * <p>Commodity names appear in mixed case and are separated by whitespace.</p>
     *
     * @return excluded commodities set
     */
    @Bean
    public ImmutableSet<String> excludedCommodityNamesList() {
        return ImmutableSet.copyOf(excludedCommodities.orElse(defaultExcludedCommodities())
                .toLowerCase()
                .split("\\s+"));
    }

    /**
     * Like {@link #excludedCommodityNamesList()}, but the commodities take the form of SDK
     * commodity type enum values.
     *
     * @return the excluded commodities list
     */
    @Bean
    public ImmutableSet<CommodityType> excludedCommodities() {
        ImmutableSet.Builder<CommodityType> builder = ImmutableSet.builder();
        Arrays.stream(excludedCommodities.orElse(defaultExcludedCommodities()).split("\\s+"))
                .map(CommodityTypeUnits::fromString)
                .map(CommodityTypeUnits::name)
                .map(CommodityType::valueOf)
                .forEach(builder::add);
        return builder.build();
    }

    /**
     * Create a factory to create price index visitors for use by the price index writer in the
     * projected live topology ingester.
     *
     * @return new factory
     */
    @Bean
    DBPriceIndexVisitorFactory priceIndexVisitorFactory() {
        return new DBPriceIndexVisitorFactory(historyDbConfig.historyDbIO());
    }

    /**
     * Create a source of new bulk loader factories.
     *
     * @return new factory supplier
     */
    @Bean
    Supplier<SimpleBulkLoaderFactory> bulkLoaderFactorySupplier() {
        return () -> new SimpleBulkLoaderFactory(historyDbConfig.historyDbIO(),
                historyDbConfig.bulkLoaderConfig(), historyDbConfig.bulkLoaderThreadPool());
    }

    private Predicate<TopologyDTO.TopologyEntityDTO> getEntitiesFilter() {
        return saveGuestLoadEntityStats ? e -> true : GuestLoadFilters::isNotGuestLoad;
    }

    /** Topology ingester types, used in injector configurations. */
    enum TopologyIngesterType {
        sourceRealtime,
        projectedRealtime,
        sourcePlan,
        projectedPlan
    }
}
