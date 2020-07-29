package com.vmturbo.extractor.topology;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.search.SearchEntityWriter;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription.Topic;

/**
 * Configuration for integration with the Topology Processor.
 */
@Configuration
@Import({
        TopologyProcessorClientConfig.class,
        GroupClientConfig.class,
        ActionOrchestratorClientConfig.class,
        ExtractorDbConfig.class
})
public class TopologyListenerConfig {
    @Autowired
    private TopologyProcessorClientConfig tpConfig;

    @Autowired
    private ExtractorDbConfig dbConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private ActionOrchestratorClientConfig actionClientConfig;

    /**
     * How often we update last-seen timestamps for entities whose hash values have not changed.
     *
     * <p>The last-seen timestamps need not be precise, so to optimize ingestion performance we
     * only update the values occasionally.</p>
     */
    @Value("${lastSeenUpdateIntervalMinutes:360}")
    private int lastSeenUpdateIntervalMinutes;

    /**
     * Fuzz factor added to last-seen timestamp estimates to provide added tolerance for delayed
     * ingestions.
     */
    @Value("${lastSeenAdditionalFuzzMinutes:60}")
    private int lastSeenAdditionalFuzzMinutes;

    /**
     * Max time to wait for results of COPY FROM command that streams data to postgres, after all
     * records have been sent.
     */
    @Value("${insertTimeoutSeconds:300}")
    private int insertTimeoutSeconds;

    @Value("${reportingCommodityWhitelistAdded:#{null}}")
    private String[] reportingCommodityWhitelistAdded;

    @Value("${reportingCommodityWhitelistRemoved:#{null}}")
    private String[] reportingCommodityWhitelistRemoved;

    /**
     * Whether or not to enable search data ingestion. This feature flag is needed since XLR may
     * be released first, but we don't want to ingest search data.
     * todo: remove once search is released.
     */
    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    /**
     * Configuration used to enable/disable reporting data ingestion. Disabled by default.
     */
    @Value("${enableReporting:false}")
    private boolean enableReporting;

    /**
     * Create an instance of our topology listener.
     *
     * @return listener instance
     */
    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        final ImmutableList<Supplier<? extends ITopologyWriter>> writerFactories =
                ImmutableList.<Supplier<? extends ITopologyWriter>>builder()
                        .addAll(writerFactories())
                        .build();
        final TopologyEntitiesListener topologyEntitiesListener = new TopologyEntitiesListener(
                writerFactories, writerConfig(), groupServiceBlockingStub(),
                actionServiceBlockingStub());
        topologyProcessor().addLiveTopologyListener(topologyEntitiesListener);
        return topologyEntitiesListener;
    }

    /**
     * Create a {@link WriterConfig} object encapsulating configuration properties for the writer.
     *
     * @return writer config
     */
    @Bean
    public WriterConfig writerConfig() {
        return ImmutableWriterConfig.builder()
                .lastSeenUpdateIntervalMinutes(lastSeenUpdateIntervalMinutes)
                .lastSeenAdditionalFuzzMinutes(lastSeenAdditionalFuzzMinutes)
                .insertTimeoutSeconds(insertTimeoutSeconds)
                .addAllReportingCommodityWhitelist(getReportingCommodityWhitelist())
                .unaggregatedCommodities(ModelDefinitions.UNAGGREGATED_KEYED_COMMODITY_TYPES)
                .build();
    }

    /**
     * Collect the commodities which should be persisted for reporting, based on default whitelist
     * and added/removed list provided by user.
     *
     * @return set of commodity types in integer format
     */
    private Set<Integer> getReportingCommodityWhitelist() {
        // use default whitelist as a basis
        final Set<CommodityType> reportingCommodityTypes = Sets.newHashSet(
                ModelDefinitions.REPORTING_DEFAULT_COMMODITY_TYPES_WHITELIST);
        // add new commodities to whitelist if provided
        if (reportingCommodityWhitelistAdded != null) {
            reportingCommodityTypes.addAll(Arrays.stream(reportingCommodityWhitelistAdded)
                    .map(CommodityType::valueOf)
                    .collect(Collectors.toSet()));
        }
        // remove commodities from whitelist if provided
        if (reportingCommodityWhitelistRemoved != null) {
            reportingCommodityTypes.removeAll(Arrays.stream(reportingCommodityWhitelistRemoved)
                    .map(CommodityType::valueOf)
                    .collect(Collectors.toSet()));
        }
        return reportingCommodityTypes.stream()
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
        return tpConfig.topologyProcessor(TopologyProcessorSubscription.forTopic(Topic.LiveTopologies));
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
     * Create a group service endpoint.
     *
     * @return service endpoint
     */
    @Bean
    public ActionsServiceBlockingStub actionServiceBlockingStub() {
        return ActionsServiceGrpc.newBlockingStub(actionClientConfig.actionOrchestratorChannel());
    }

    /**
     * Create list of factories for writers that will participate in topology processing.
     *
     * @return writer factories
     */
    @Bean
    public List<Supplier<ITopologyWriter>> writerFactories() {
        final DbEndpoint dbEndpoint = dbConfig.ingesterEndpoint();
        ImmutableList.Builder<Supplier<ITopologyWriter>> builder = ImmutableList.builder();
        if (enableSearchApi) {
            builder.add(() -> new SearchEntityWriter(dbEndpoint, pool()));
        }
        if (enableReporting) {
            builder.add(() -> new EntityMetricWriter(dbEndpoint, entityHashManager(), pool()));
        }
        return builder.build();
    }

    /**
     * Entity hash manager to track entity hash evolution across topology broadcasts.
     *
     * @return the hash manager
     */
    @Bean
    public EntityHashManager entityHashManager() {
        return new EntityHashManager(writerConfig());
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
}

