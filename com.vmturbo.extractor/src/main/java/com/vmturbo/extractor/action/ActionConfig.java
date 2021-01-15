package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
import com.vmturbo.extractor.topology.TopologyListenerConfig;
import com.vmturbo.group.api.GroupClientConfig;

/**
 * Configuration for action ingestion for reporting.
 */
@Configuration
@Import({
        ActionOrchestratorClientConfig.class,
        GroupClientConfig.class,
        ExtractorDbConfig.class,
        TopologyListenerConfig.class
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
     * Configuration used to enable/disable ingestion.
     *
     * <p/>This is only meaningful if "enableReporting" is true, and is a way to stop action ingestion
     * without disabling the rest of reporting.
     */
    @Value("${enableActionIngestion:true}")
    private boolean enableActionIngestion;

    /**
     * Configuration used to completely enable/disable reporting.
     */
    @Value("${enableReporting:false}")
    private boolean enableReporting;

    /**
     * Whether or not to enable search data ingestion.
     */
    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    /**
     * Configuration used to enable/disable data extraction. Disabled by default.
     */
    @Value("${enableDataExtraction:false}")
    private boolean enableDataExtraction;

    /**
     * The interval for extracting action information and sending to Kafka. Default to 6 hours.
     */
    @Value("${actionExtractionIntervalMins:360}")
    private long actionExtractionIntervalMins;

    /**
     * See {@link ActionConverter}.
     *
     * @return The {@link ActionConverter}.
     */
    @Bean
    public ActionConverter actionConverter() {
        return new ActionConverter();
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
            policyService(),
            topologyListenerConfig.dataProvider(),
            enableReporting && enableActionIngestion,
            enableSearchApi,
            enableDataExtraction,
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
                actionConverter());
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
                Clock.systemUTC(),
                actionConverter(),
                extractorDbConfig.ingesterEndpoint(),
                TimeUnit.MINUTES.toMillis(actionMetricsWritingIntervalMins),
                topologyListenerConfig.writerConfig(),
                topologyListenerConfig.pool(),
                topologyListenerConfig.dataProvider(),
                topologyListenerConfig.extractorKafkaSender(),
                topologyListenerConfig.dataExtractionFactory(),
                TimeUnit.MINUTES.toMillis(actionExtractionIntervalMins)
        );
    }
}
