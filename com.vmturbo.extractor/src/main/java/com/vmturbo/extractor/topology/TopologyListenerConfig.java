package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.FILE_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_PATH;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.VOLUME_OID;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.lang3.ObjectUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.DataPacks.LongDataPack;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.cost.api.impl.CostComponentImpl;
import com.vmturbo.cost.api.impl.CostSubscription;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.ExtractorGlobalConfig;
import com.vmturbo.extractor.ExtractorGlobalConfig.ExtractorFeatureFlags;
import com.vmturbo.extractor.action.commodity.ActionCommodityDataRetriever;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.ExportUtils;
import com.vmturbo.extractor.export.ExtractorKafkaSender;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.models.HashedDataManager;
import com.vmturbo.extractor.search.SearchEntityWriter;
import com.vmturbo.extractor.topology.ITopologyWriter.TopologyWriterFactory;
import com.vmturbo.extractor.topology.attributes.HistoricalAttributeWriterFactory;
import com.vmturbo.extractor.topology.fetcher.BottomUpCostFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.CloudSavingsFetcher;
import com.vmturbo.extractor.topology.fetcher.ClusterStatsFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.RICoverageFetcherFactory;
import com.vmturbo.extractor.topology.fetcher.TopDownCostFetcherFactory;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({
        TopologyProcessorClientConfig.class,
        GroupClientConfig.class,
        HistoryClientConfig.class,
        ActionOrchestratorClientConfig.class,
        ExtractorDbConfig.class,
        ExtractorGlobalConfig.class,
        HistoryClientConfig.class,
        CostClientConfig.class
})
public class TopologyListenerConfig {
    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Autowired
    private ExtractorDbConfig dbConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private BaseKafkaProducerConfig kafkaProducerConfig;

    @Autowired
    private ExtractorGlobalConfig extractorGlobalConfig;

    @Autowired
    private HistoryClientConfig historyClientConfig;

    @Autowired
    private CostClientConfig costClientConfig;

    /**
     * Max time to wait for results of COPY FROM command that streams data to postgres, after all
     * records have been sent.
     */
    @Value("${insertTimeoutSeconds:300}") // 5 minutes
    private int insertTimeoutSeconds;

    @Value("${reportingCommodityWhitelistAdded:#{null}}")
    private String[] reportingCommodityWhitelistAdded;

    @Value("${reportingCommodityWhitelistRemoved:#{null}}")
    private String[] reportingCommodityWhitelistRemoved;

    @Value("${reportingActionCommodityWhitelistRemoved:#{null}}")
    private String[] actionCommodityWhitelistAdded;

    @Value("${reportingActionCommodityWhitelistAdded:#{null}}")
    private String[] actionCommodityWhitelistRemoved;

    /**
     * The interval at which we will force-write historical attributes to the database even if
     * they have not changed.
     */
    @Value("${historicalAttributeMaxUpdateIntervalHrs:24}")
    private long historicalAttributeMaxUpdateIntervalHrs;

    /**
     * Interval at which we will check for cluster headroom props.
     */
    @Value("${headroomCheckIntervalHrs:6}")
    private int headroomCheckIntervalHrs;

    /**
     * Max time to wait for an object to be delivered to Kafka, default to 5 min.
     */
    @Value("${kafkaTimeoutSeconds:300}")
    private int kafkaTimeoutSeconds;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${searchBatchSize:1000}")
    private int searchBatchSize;

    /**
     * The interval for extracting entity/group and sending to Kafka. Default to broadcast schedule.
     */
    @Value("${entityExtractionIntervalMins:#{null}}")
    private Long entityExtractionIntervalMins;

    /**
     * Create an instance of our topology listener.
     *
     * @return listener instance
     */
    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        final TopologyEntitiesListener topologyEntitiesListener = new TopologyEntitiesListener(
                writerFactories(), writerConfig(), dataProvider(), extractorGlobalConfig.featureFlags());
        topologyProcessor().addLiveTopologyListener(topologyEntitiesListener);
        return topologyEntitiesListener;
    }

    /**
     * Set up to process cost data availability notifications from cost component.
     *
     * @return listener
     */
    @Bean
    public EntityCostListener entityCostListener() {
        final ExtractorFeatureFlags extractorFeatureFlags = extractorGlobalConfig.featureFlags();
        if (extractorFeatureFlags.isReportingEnabled() || extractorFeatureFlags.isExtractionEnabled()) {
            final EntityCostListener entityCostListener = new EntityCostListener(
                    dataProvider(), dbConfig.ingesterEndpoint(), pool(), writerConfig(),
                    extractorFeatureFlags.isReportingEnabled(), realtimeTopologyContextId);
            costNotificationProcessor().addCostNotificationListener(entityCostListener);
            return entityCostListener;
        } else {
            return null;
        }
    }

    /**
     * Set up billing cost processing listener if relevant flags are enabled.
     *
     * @return listener, or null if flags are not enabled.
     */
    @Bean
    @Nullable
    public AccountExpensesListener accountExpensesListener() {
        final ExtractorFeatureFlags extractorFeatureFlags = extractorGlobalConfig.featureFlags();
        if (extractorFeatureFlags.isExtractionEnabled()
                || extractorFeatureFlags.isBillingCostReportingEnabled()) {
            final AccountExpensesListener aeListener = new AccountExpensesListener(
                    dataProvider(), dbConfig.ingesterEndpoint(), pool(), writerConfig(),
                    extractorFeatureFlags.isBillingCostReportingEnabled());
            costNotificationProcessor().addCostNotificationListener(aeListener);
            return aeListener;
        }
        return null;
    }

    /**
     * Used to retrieve percentile-related data for actions.
     *
     * @return The {@link ActionCommodityDataRetriever}.
     */
    @Bean
    public ActionCommodityDataRetriever actionCommodityDataRetriever() {
        return new ActionCommodityDataRetriever(statsHistoryServiceBlockingStub(),
                settingPolicyServiceBlockingStub(),
                getActionCommodityWhiteList());
    }

    /**
     * Create a {@link WriterConfig} object encapsulating configuration properties for the writer.
     *
     * @return writer config
     */
    @Bean
    public WriterConfig writerConfig() {
        ExtractorFeatureFlags extractorFeatureFlags = extractorGlobalConfig.featureFlags();

        Multimap<CommodityType, EntityType> unaggregatedKeyedCommodityTypes;

        if (extractorFeatureFlags.isIndividualVStoragesEnabled()) {
            unaggregatedKeyedCommodityTypes =
                    ImmutableSetMultimap.<CommodityType, EntityType>builder()
                            .putAll(Constants.UNAGGREGATED_KEYED_COMMODITY_TYPES)
                            .put(CommodityType.VSTORAGE, EntityType.VIRTUAL_MACHINE)
                            .build();
        } else {
            unaggregatedKeyedCommodityTypes = Constants.UNAGGREGATED_KEYED_COMMODITY_TYPES;
        }

        return ImmutableWriterConfig.builder()
                .insertTimeoutSeconds(insertTimeoutSeconds)
                .addAllReportingCommodityWhitelist(getReportingCommodityWhitelist())
                .unaggregatedCommodities(unaggregatedKeyedCommodityTypes)
                .searchBatchSize(searchBatchSize)
                .build();
    }

    /**
     * Collect the commodities which should be persisted for reporting, based on default whitelist
     * and added/removed list provided by user.
     *
     * @return set of commodity types in integer format
     */
    private Set<Integer> getReportingCommodityWhitelist() {
        return getCommodityWhitelist(Constants.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST,
                reportingCommodityWhitelistAdded, reportingCommodityWhitelistRemoved);
    }

    private Set<Integer> getActionCommodityWhiteList() {
        return getCommodityWhitelist(Constants.REPORTING_ACTION_COMMODITY_TYPES_WHITELIST,
                actionCommodityWhitelistAdded, actionCommodityWhitelistRemoved);
    }

    private Set<Integer> getCommodityWhitelist(Set<CommodityType> defaults, String[] added, String[] removed) {
        final Set<CommodityType> retTypes = Sets.newHashSet(defaults);
        // add new commodities to whitelist if provided
        if (added != null) {
            retTypes.addAll(Arrays.stream(added)
                    .map(CommodityType::valueOf)
                    .collect(Collectors.toSet()));
        }
        // remove commodities from whitelist if provided
        if (removed != null) {
            retTypes.removeAll(Arrays.stream(removed)
                    .map(CommodityType::valueOf)
                    .collect(Collectors.toSet()));
        }
        return retTypes.stream()
            .map(CommodityType::getNumber)
            .collect(Collectors.toSet());
    }

    /**
     * Create a topology processor subscribing to realtime source topologies.
     *
     * @return topology processor
     */
    @Bean
    public TopologyProcessor topologyProcessor() {
        // Only listen to REALTIME SOURCE topologies.
        // TODO: Also listen for live topology summaries
        return tpConfig.topologyProcessor(
                TopologyProcessorSubscription.forTopic(Topic.LiveTopologies),
                TopologyProcessorSubscription.forTopic(Topic.Notifications));
    }

    /**
     * Subscribe to cost component's notification kafka topic.
     *
     * @return cost notification processor
     */
    public CostComponentImpl costNotificationProcessor() {
        return costClientConfig.costComponent(
                CostSubscription.forTopic(CostSubscription.Topic.COST_STATUS_NOTIFICATION));
    }

    /**
     * Create a group service endpoint.
     *
     * @return service endpoint
     */
    @Bean
    public GroupServiceBlockingStub groupServiceBlockingStub() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Create a history service endpoint.
     *
     * @return service endpoint
     */
    @Bean
    public StatsHistoryServiceBlockingStub statsHistoryServiceBlockingStub() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyClientConfig.historyChannel());
    }

    /**
     * Creates settings policy service endpoint.
     *
     * @return Blocking stub endpoint to settings policy service.
     */
    @Bean
    public SettingPolicyServiceBlockingStub settingPolicyServiceBlockingStub() {
        return SettingPolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Creates settings service endpoint.
     *
     * @return Blocking stub endpoint to settings service.
     */
    @Bean
    public SettingServiceBlockingStub settingServiceBlockingStub() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * Create list of factories for writers that will participate in topology processing.
     *
     * @return writer factories
     */
    @Bean
    public List<TopologyWriterFactory<?>> writerFactories() {
        List<TopologyWriterFactory<?>> retFactories = new ArrayList<>();
        ExtractorFeatureFlags featureFlags = extractorGlobalConfig.featureFlags();
        if (featureFlags.isSearchEnabled()) {
            retFactories.add(() -> new SearchEntityWriter(dbConfig.ingesterMySqlEndpoint(), pool()));
        }
        if (featureFlags.isExtractionEnabled()) {
            retFactories.add(() -> dataExtractionFactory().newDataExtractionWriter());
        }
        if (featureFlags.isExtractionEnabled() || featureFlags.isReportingActionIngestionEnabled()) {
            retFactories.add(() -> actionCommodityDataRetriever());
        }
        if (featureFlags.isReportingEnabled()) {
            retFactories.add(() -> new EntityMetricWriter(dbConfig.ingesterEndpoint(), entityHashManager(),
                    scopeManager(), fileDataManager(),
                    oidPack(), pool(), dataExtractionFactory()));
            retFactories.add(historicalAttributeWriterFactory());
            retFactories.add(() -> new TopologyStatsWriter(dbConfig.ingesterEndpoint(), pool()));
        }
        return Collections.unmodifiableList(retFactories);
    }

    /**
     * Factory class for entity historical attributes.
     *
     * @return The {@link HistoricalAttributeWriterFactory}.
     */
    @Bean
    public HistoricalAttributeWriterFactory historicalAttributeWriterFactory() {
        return new HistoricalAttributeWriterFactory(dbConfig.ingesterEndpoint(),
                pool(),
                extractorGlobalConfig.clock(),
                historicalAttributeMaxUpdateIntervalHrs, TimeUnit.HOURS);
    }

    /**
     * Entity hash manager to track entity hash evolution across topology broadcasts.
     *
     * @return the hash manager
     */
    @Bean
    public EntityHashManager entityHashManager() {
        return new EntityHashManager(oidPack(), writerConfig());
    }

    /**
     * Scope manager to persist record of changing entity scopes.
     *
     * @return scope manager
     */
    @Bean
    public ScopeManager scopeManager() {
        return new ScopeManager(oidPack(),
                dbConfig.ingesterEndpoint(), writerConfig(), pool());
    }

    /**
     * HashedDataManager for file table.
     * @return file table manager
     */
    public HashedDataManager fileDataManager() {
        return new HashedDataManager(
                FILE_TABLE, FILE_HASH, ImmutableSet.of(VOLUME_OID, FILE_PATH), pool());
    }

    /**
     * EntityIdManager to allow entity and other long ids to be stored as ints.
     *
     * @return entity id manager
     */
    @Bean
    public DataPack<Long> oidPack() {
        return new LongDataPack();
    }

    /**
     * Create a thread pool for use by the writers.
     *
     * @return thread pool
     */
    @Bean
    public ExecutorService pool() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("extractor-%d").build();
        return Executors.newCachedThreadPool(threadFactory);
    }

    /**
     * The {@link ClusterStatsFetcherFactory} for the {@link DataProvider}, only initialized if
     * reporting is enabled.
     *
     * @return The {@link ClusterStatsFetcherFactory}.
     */
    @Bean
    public ClusterStatsFetcherFactory clusterStatsFetcherFactory() {
        if (extractorGlobalConfig.featureFlags().isReportingEnabled()) {
            return new ClusterStatsFetcherFactory(statsHistoryServiceBlockingStub(),
                    dbConfig.ingesterEndpoint(), headroomCheckIntervalHrs, TimeUnit.HOURS);
        }
        return null;
    }

    /**
     * The {@link TopDownCostFetcherFactory} for the {@link DataProvider}.
     *
     * @return The {@link TopDownCostFetcherFactory}.
     */
    @Bean
    public TopDownCostFetcherFactory topDownCostFetcherFactory() {
        return new TopDownCostFetcherFactory(costService(),
                RIAndExpenseUploadServiceGrpc.newBlockingStub(costClientConfig.costChannel()));
    }

    /**
     * The {@link BottomUpCostFetcherFactory} for the {@link DataProvider}.
     *
     * @return the {@link BottomUpCostFetcherFactory}
     */
    @Bean
    public BottomUpCostFetcherFactory bottomUpCostFetcherFactory() {
        return new BottomUpCostFetcherFactory(costService());
    }

    /**
     * Factory for creating fetchers for RI coverage.
     *
     * @return the {@link RICoverageFetcherFactory}
     */
    @Bean
    public RICoverageFetcherFactory riCoverageFetcherFactory() {
        return new RICoverageFetcherFactory(riCoverageService());
    }

    /**
     * The data provider which contains latest topology, supply chain, etc.
     *
     * @return {@link DataProvider}
     */
    @Bean
    public DataProvider dataProvider() {
        return new DataProvider(groupServiceBlockingStub(),
                clusterStatsFetcherFactory(),
                topDownCostFetcherFactory(),
                bottomUpCostFetcherFactory(),
                targetCache(),
                riCoverageFetcherFactory());
    }

    /**
     * The sender used to send objects to Kafka.
     *
     * @return {@link ExtractorKafkaSender}
     */
    @Bean
    public ExtractorKafkaSender extractorKafkaSender() {
        return new ExtractorKafkaSender(kafkaProducerConfig.kafkaMessageSender()
                .bytesSender(ExportUtils.DATA_EXTRACTION_KAFKA_TOPIC), kafkaTimeoutSeconds);
    }

    /**
     * The factory for creating different extractors used in data extraction.
     *
     * @return {@link DataExtractionFactory}
     */
    @Bean
    public DataExtractionFactory dataExtractionFactory() {
        final Long extractionIntervalMins = ObjectUtils.firstNonNull(
                entityExtractionIntervalMins,
                extractorGlobalConfig.globalExtractionIntervalMins);
        return new DataExtractionFactory(dataProvider(), targetCache(), extractorKafkaSender(),
                Optional.ofNullable(extractionIntervalMins).map(TimeUnit.MINUTES::toMillis).orElse(null),
                extractorGlobalConfig.clock());
    }

    /**
     * Thin target cache.
     *
     * @return the bean created
     */
    @Bean
    public ThinTargetCache targetCache() {
        return new ThinTargetCache(topologyProcessor());
    }

    /**
     * Cost service endpoint.
     *
     * @return service endpoint
     */
    @Bean
    public CostServiceBlockingStub costService() {
        return CostServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Grpc endpoint for fetching ri coverage utilization.
     *
     * @return service endpoint
     */
    @Bean
    public ReservedInstanceUtilizationCoverageServiceBlockingStub riCoverageService() {
        return ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    /**
     * Creates instance of Cloud Savings listener.
     *
     * @return Cloud Savings listener.
     */
    @Bean
    public CloudSavingsListener cloudSavingsListener() {
        final CloudSavingsListener ceListener = new CloudSavingsListener(new CloudSavingsFetcher(
                dbConfig.ingesterEndpoint(), costService(), pool(), writerConfig()));
        costNotificationProcessor().addCostNotificationListener(ceListener);
        return ceListener;
    }
}
