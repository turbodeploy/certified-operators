package com.vmturbo.ml.datastore.topology;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.ml.datastore.influx.InfluxConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Configuration for integration with the Topology Processor. The ML datstore receives
 * topology broadcasts from the TP and writes the metrics in those broadcasts to influx.
 */
@Configuration
@Import({
    ActionOrchestratorClientConfig.class,
    InfluxConfig.class
})
public class ActionsListenerConfig {

    @Autowired
    private ActionOrchestratorClientConfig aoConfig;

    @Autowired
    private InfluxConfig influxConfig;

    @Bean
    public ActionMetricsListener topologyActionsListener() {
        final ActionMetricsListener actionMetricsListener =
            new ActionMetricsListener(influxConfig.influxDBConnectionFactory(),
                influxConfig.metricsStoreWhitelist());
        actionsProcessor().addActionsListener(actionMetricsListener);
        return actionMetricsListener;
    }

    @Bean
    public ActionOrchestrator actionsProcessor() {
        // Only listen to actions.
        return aoConfig.actionOrchestratorClient();
    }
}
