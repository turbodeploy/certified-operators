package com.vmturbo.extractor.action;

import java.time.Clock;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
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

    // TODO before checkin - change to 60 by default.
    @Value("${actionMetricsWritingIntervalMins:0}")
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
     * The {@link ActionWriter}. This is the one that actually writes data!
     *
     * @return The {@link ActionWriter}.
     */
    @Bean
    public ActionWriter actionWriter() {
        final ActionWriter actionWriter = new ActionWriter(Clock.systemUTC(),
            topologyListenerConfig.pool(),
            ActionsServiceGrpc.newBlockingStub(actionClientConfig.actionOrchestratorChannel()),
            extractorDbConfig.ingesterEndpoint(), topologyListenerConfig.writerConfig(),
            actionConverter(), actionHashManager(),
            actionMetricsWritingIntervalMins, TimeUnit.MINUTES,
            enableReporting && enableActionIngestion,
            realtimeTopologyContextId);

        actionClientConfig.actionOrchestratorClient().addListener(actionWriter);
        return actionWriter;
    }

}
