package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

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
import com.vmturbo.extractor.ExtractorDbConfig;
import com.vmturbo.extractor.topology.TopologyListenerConfig;

/**
 * Configuration for action ingestion for reporting.
 */
@Configuration
@Import({
        ActionOrchestratorClientConfig.class,
        ExtractorDbConfig.class,
        TopologyListenerConfig.class
})
public class ActionConfig {

    @Autowired
    private ExtractorDbConfig extractorDbConfig;

    @Autowired
    private ActionOrchestratorClientConfig actionClientConfig;

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
     * See {@link ActionConverter}.
     *
     * @return The {@link ActionConverter}.
     */
    @Bean
    public ActionConverter actionConverter() {
        return new ActionConverter();
    }

    /**
     * See {@link ActionHashManager}.
     *
     * @return The {@link ActionHashManager}.
     */
    @Bean
    public ActionHashManager actionHashManager() {
        return new ActionHashManager(topologyListenerConfig.writerConfig());
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
     * The {@link ActionWriter}.
     *
     * @return The {@link ActionWriter}.
     */
    @Bean
    public ActionWriter actionWriter() {
        final ActionWriter actionWriter = new ActionWriter(Clock.systemUTC(),
            actionServiceBlockingStub(),
            entitySeverityServiceStub(),
            topologyListenerConfig.dataProvider(),
            TimeUnit.MINUTES.toMillis(actionMetricsWritingIntervalMins),
            enableReporting && enableActionIngestion,
            enableSearchApi,
            realtimeTopologyContextId,
            reportingActionWriterSupplier(),
            searchActionWriterSupplier());

        actionClientConfig.actionOrchestratorClient().addListener(actionWriter);
        return actionWriter;
    }

    /**
     * Supplies a ReportingActionWriter. This is the one that actually writes data!
     * @return supplier of ReportingActionWriter
     */
    @Bean
    public Supplier<ReportingActionWriter> reportingActionWriterSupplier() {
        return () -> new ReportingActionWriter(
                Clock.systemUTC(),
                topologyListenerConfig.pool(),
                extractorDbConfig.ingesterEndpoint(),
                topologyListenerConfig.writerConfig(),
                actionConverter(),
                actionHashManager(),
                TimeUnit.MINUTES.toMillis(actionMetricsWritingIntervalMins));
    }

    /**
     * Supplies a SearchActionWriter.
     * @return supplier of SearchActionWriter
     */
    @Bean
    public Supplier<SearchActionWriter> searchActionWriterSupplier() {
        return () -> new SearchActionWriter(topologyListenerConfig.dataProvider(),
                extractorDbConfig.ingesterEndpoint(),
                topologyListenerConfig.writerConfig(),
                topologyListenerConfig.pool());
    }
}
