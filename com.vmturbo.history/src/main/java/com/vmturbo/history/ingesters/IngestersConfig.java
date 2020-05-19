package com.vmturbo.history.ingesters;

import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.api.HistoryApiConfig;
import com.vmturbo.history.db.HistoryDbConfig;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.ingesters.common.ImmutableTopologyIngesterConfig;
import com.vmturbo.history.ingesters.common.TopologyIngesterConfig;
import com.vmturbo.history.ingesters.live.LiveTopologyIngester;
import com.vmturbo.history.ingesters.live.ProjectedLiveTopologyIngester;
import com.vmturbo.history.ingesters.live.writers.ClusterStatsWriter;
import com.vmturbo.history.ingesters.live.writers.EntitiesWriter;
import com.vmturbo.history.ingesters.live.writers.EntityStatsWriter;
import com.vmturbo.history.ingesters.live.writers.PriceIndexWriter;
import com.vmturbo.history.ingesters.live.writers.SystemLoadWriter;
import com.vmturbo.history.ingesters.live.writers.TopologyCommoditiesProcessor;
import com.vmturbo.history.ingesters.plan.PlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.ProjectedPlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.writers.PlanStatsWriter;
import com.vmturbo.history.ingesters.plan.writers.ProjectedPlanStatsWriter;
import com.vmturbo.history.listeners.ImmutableTopologyCoordinatorConfig;
import com.vmturbo.history.listeners.RollupProcessor;
import com.vmturbo.history.listeners.TopologyCoordinator;
import com.vmturbo.history.listeners.TopologyCoordinatorConfig;
import com.vmturbo.history.stats.StatsConfig;
import com.vmturbo.history.stats.priceindex.DBPriceIndexVisitor.DBPriceIndexVisitorFactory;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;

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

    @Value("${ingest.defaultChunkTimeLimitMsec:60000}") // 1 minute
    private long defaultChunkTimeLimitMsec;

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
                TopologyProcessorSubscription
                        .forTopic(TopologyProcessorSubscription.Topic.LiveTopologies),
                TopologyProcessorSubscription
                        .forTopic(TopologyProcessorSubscription.Topic.TopologySummaries));
        topologyProcessor.addLiveTopologyListener(topologyCoordinator());
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
        market.addPlanAnalysisTopologyListener(topologyCoordinator());
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
                liveTopologyIngester(),
                projectedLiveTopologyIngester(),
                planTopologyIngester(),
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
                .realtimeTopologyContextId(statsConfig.realtimeTopologyContextId)
                .build();
    }

    /**
     * Create an ingester to process live topologies from topology processor.
     *
     * @return new ingester
     */
    @Bean
    LiveTopologyIngester liveTopologyIngester() {
        return new LiveTopologyIngester(
                Arrays.asList(
                        new EntityStatsWriter.Factory(
                                historyDbConfig.historyDbIO(),
                                excludedCommoditiesList()
                        ),
                        new SystemLoadWriter.Factory(
                                groupServiceBlockingStub(),
                                statsConfig.systemLoadReader(),
                                historyDbConfig.historyDbIO()
                        ),
                        new EntitiesWriter.Factory(
                                historyDbConfig.historyDbIO()
                        ),
                        new ClusterStatsWriter.Factory(
                                historyDbConfig.historyDbIO(),
                                groupServiceBlockingStub()
                        )
                ),
                ingesterThreadPool(),
                ingesterConfig(),
                bulkLoaderFactorySupplier()
        );
    }

    /**
     * Create an ingester to process projected live topologies from market component.
     *
     * @return new ingester
     */
    @Bean
    ProjectedLiveTopologyIngester projectedLiveTopologyIngester() {
        return new ProjectedLiveTopologyIngester(
                Arrays.asList(
                        new TopologyCommoditiesProcessor.Factory(
                                statsConfig.projectedStatsStore()
                        ),
                        new PriceIndexWriter.Factory(priceIndexVisitorFactory())
                ),
                ingesterConfig(),
                bulkLoaderFactorySupplier()
        );
    }

    /**
     * Create a new ingester to process plan analysis topologies from market component.
     *
     * @return new ingester
     */
    @Bean
    PlanTopologyIngester planTopologyIngester() {
        return new PlanTopologyIngester(
                Arrays.asList(
                        new PlanStatsWriter.Factory(historyDbConfig.historyDbIO())
                ),
                ingesterConfig(),
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
                Arrays.asList(
                        new ProjectedPlanStatsWriter.Factory(historyDbConfig.historyDbIO())
                ),
                ingesterConfig(),
                bulkLoaderFactorySupplier()
        );
    }

    @Bean
    ExecutorService ingesterThreadPool() {
        return Executors.newFixedThreadPool(3); // TODO configurize, add thread factory
    }

    /**
     * Create an ingester config object used by all our ingesters.
     *
     * @return shared ingester config
     */
    @Bean
    TopologyIngesterConfig ingesterConfig() {
        return ImmutableTopologyIngesterConfig.builder()
                .perChunkCommit(perChunkCommit)
                .threadPool(ingesterThreadPool())
                .defaultChunkTimeLimitMsec(defaultChunkTimeLimitMsec)
                .build();
    }

    /**
     * Create a rollup processor to perform rollups for topology coordinator.
     *
     * @return new rollup processor
     */
    @Bean
    RollupProcessor rollupProcessor() {
        return new RollupProcessor(
                historyDbConfig.historyDbIO(), bulkLoaderThreadPool());
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
        return String.join(" ", new String[]{
                "ApplicationCommodity",
                "CLUSTERCommodity",
                "DATACENTERCommodity",
                "DATASTORECommodity",
                "DRSSEGMENTATIONCommodity",
                "DSPMAccessCommodity",
                "NETWORKCommodity",
                "SEGMENTATIONCommodity",
                "STORAGECLUSTERCommodity",
                "VAPPAccessCommodity",
                "VDCCommodity",
                "VMPMAccessCommodity"
        });
    }

    /**
     * Create set of excluded commodities by parsing the string property value, used by the
     * entity stats writer in the live topology ingester.
     *
     * <p>Commodity names appear in mixed case and are separated by whitespace.</p>
     *
     * @return excluded commodities set
     */
    @Bean
    ImmutableSet<String> excludedCommoditiesList() {
        return ImmutableSet.copyOf(excludedCommodities.orElse(defaultExcludedCommodities())
                .toLowerCase()
                .split("\\s+"));
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
                historyDbConfig.bulkLoaderConfig(), bulkLoaderThreadPool());
    }

    /**
     * Create a shared thread pool for bulk writers used by all the ingesters.
     *
     * @return new thread pool
     */
    @Bean(destroyMethod = "shutdownNow")
    ExecutorService bulkLoaderThreadPool() {
        ThreadFactory factory = new ThreadFactoryBuilder()
                .setNameFormat(BulkInserter.class.getSimpleName() + "-%d")
                .build();
        return Executors.newFixedThreadPool(historyDbConfig.parallelBatchInserts, factory);
    }
}
