package com.vmturbo.extractor.action;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceStub;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.ExtractorGlobalConfig;
import com.vmturbo.extractor.action.stats.HistoricalPendingCountReceiver;
import com.vmturbo.extractor.topology.TopologyListenerConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;

/**
 * Configuration for action ingestion for reporting.
 */
@Configuration
@Import({
        ActionOrchestratorClientConfig.class,
        GroupClientConfig.class,
        ExtractorDbConfig.class,
        TopologyListenerConfig.class,
        ExtractorGlobalConfig.class,
        HistoryClientConfig.class,
})
public class ActionConfig {

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Autowired
    private ActionOrchestratorClientConfig actionClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private TopologyListenerConfig topologyListenerConfig;

    @Autowired
    private ExtractorGlobalConfig globalConfig;

    /**
     * Max time to wait for results of COPY FROM command that streams data to postgres, after all
     * records have been sent.
     */
    @Value("${insertTimeoutSeconds:300}")
    private int insertTimeoutSeconds;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${actionMetricsWritingIntervalMins:60}")
    private long actionMetricsWritingIntervalMins;

    /**
     * The interval for extracting action information and sending to Kafka. Default to 6 hours.
     */
    @Value("${actionExtractionIntervalMins:360}")
    private long actionExtractionIntervalMins;

    /**
     * The extractor will cache policy information for this long after retrieving it from the group
     * component to populate action descriptions.
     */
    @Value("${cachedPolicyUpdateIntervalMins:10}")
    private long cachedPolicyUpdateIntervalMins;

    /**
     * See {@link ActionAttributeExtractor}.
     *
     * @return The {@link ActionAttributeExtractor}.
     */
    @Bean
    public ActionAttributeExtractor actionAttributeExtractor() {
        return new ActionAttributeExtractor(topologyListenerConfig.actionCommodityDataRetriever());
    }

    /**
     * See {@link HistoricalPendingCountReceiver}.
     *
     * @return The {@link HistoricalPendingCountReceiver}.
     */
    @Bean
    public HistoricalPendingCountReceiver historicalPendingCountReceiver() {
        HistoricalPendingCountReceiver pendingCountReceiver =
                new HistoricalPendingCountReceiver(globalConfig.featureFlags(),
                        extractorDbConfig.ingesterEndpoint(),
                        topologyListenerConfig.pool(),
                        topologyListenerConfig.writerConfig());
        actionClientConfig.actionRollupNotificationReceiver().addListener(pendingCountReceiver);
        return pendingCountReceiver;
    }

    /**
     * See {@link ActionConverter}.
     *
     * @return The {@link ActionConverter}.
     */
    @Bean
    public ActionConverter actionConverter() {
        ObjectMapper objectMapper = new ObjectMapper();
        // serialize all fields even through no getter defined or private
        objectMapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
        return new ActionConverter(actionAttributeExtractor(), cachingPolicyFetcher(),
                topologyListenerConfig.dataProvider(),
                topologyListenerConfig.dataExtractionFactory(),
                objectMapper);
    }

    /**
     * Fetches policy, caching them to avoid fetching multiple times.
     *
     * @return The {@link CachingPolicyFetcher}.
     */
    @Bean
    public CachingPolicyFetcher cachingPolicyFetcher() {
        return new CachingPolicyFetcher(policyService(), globalConfig.clock(),
                cachedPolicyUpdateIntervalMins, TimeUnit.MINUTES);
    }

    /**
     * Service for fetching actions.
     *
     * @return {@link ActionsServiceBlockingStub}
     */
    @Bean
    public ActionsServiceBlockingStub actionServiceBlockingStub() {
        return ActionsServiceGrpc.newBlockingStub(actionClientConfig.actionOrchestratorChannel());
    }

    /**
     * Service for fetching entity severities.
     *
     * @return {@link EntitySeverityServiceStub}
     */
    @Bean
    public EntitySeverityServiceStub entitySeverityServiceStub() {
        return EntitySeverityServiceGrpc.newStub(actionClientConfig.actionOrchestratorChannel());
    }

    /**
     * Service for fetching policies.
     *
     * @return {@link PolicyServiceBlockingStub}
     */
    @Bean
    public PolicyServiceBlockingStub policyService() {
        return PolicyServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    /**
     * The {@link PendingActionWriter}.
     *
     * @return The {@link PendingActionWriter}.
     */
    @Bean
    public PendingActionWriter pendingActionWriter() {
        final PendingActionWriter pendingActionWriter = new PendingActionWriter(
            actionServiceBlockingStub(),
            entitySeverityServiceStub(),
            topologyListenerConfig.dataProvider(),
            globalConfig.featureFlags(),
            realtimeTopologyContextId,
            actionWriterFactory());

        actionClientConfig.actionOrchestratorClient().addListener(pendingActionWriter);
        return pendingActionWriter;
    }

    /**
     * Responsible for writing completed actions to the database.
     *
     * @return The {@link CompletedActionWriter}.
     */
    @Bean
    public CompletedActionWriter completedActionWriter() {
        final CompletedActionWriter completedActionWriter = new CompletedActionWriter(
                extractorDbConfig.ingesterEndpoint(), completedActionExecutor(),
                topologyListenerConfig.writerConfig(), topologyListenerConfig.pool(),
                actionConverter(), topologyListenerConfig.dataProvider(), globalConfig.featureFlags(),
                topologyListenerConfig.extractorKafkaSender(), globalConfig.clock());
        actionClientConfig.actionOrchestratorClient().addListener(completedActionWriter);
        return completedActionWriter;
    }

    /**
     * This threadpool gets used by the {@link CompletedActionWriter} to asynchronously trigger
     * inserts of batches of queued actions.
     *
     * @return The {@link ExecutorService}.
     */
    @Bean
    public ExecutorService completedActionExecutor() {
        final ThreadFactory
                threadFactory = new ThreadFactoryBuilder().setNameFormat("completed-action-recorder-%d").build();
        return Executors.newSingleThreadExecutor(threadFactory);
    }

    /**
     * Bean for {@link ActionWriterFactory}.
     *
     * @return {@link ActionWriterFactory}
     */
    @Bean
    public ActionWriterFactory actionWriterFactory() {
        return new ActionWriterFactory(
                globalConfig.clock(),
                actionConverter(),
                extractorDbConfig.ingesterEndpoint(),
                TimeUnit.MINUTES.toMillis(actionMetricsWritingIntervalMins),
                topologyListenerConfig.writerConfig(),
                topologyListenerConfig.pool(),
                topologyListenerConfig.dataProvider(),
                topologyListenerConfig.extractorKafkaSender(),
                TimeUnit.MINUTES.toMillis(actionExtractionIntervalMins));
    }
}
