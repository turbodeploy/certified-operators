package com.vmturbo.ml.datastore.topology;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.market.component.api.impl.MarketSubscription.Topic;
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
    MarketClientConfig.class,
    InfluxConfig.class
})
public class ActionsListenerConfig {

    @Autowired
    private ActionOrchestratorClientConfig aoConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Autowired
    private InfluxConfig influxConfig;

    @Bean
    public ActionMetricsListener topologyActionsListener() {
        final ActionMetricsListener actionMetricsListener =
            new ActionMetricsListener(influxConfig.influxDBConnectionFactory(),
                influxConfig.metricsStoreWhitelist());
        actionsProcessor().addActionsListener(actionMetricsListener);
        marketComponent().addActionsListener(actionMetricsListener);
        return actionMetricsListener;
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
                MarketSubscription.forTopic(Topic.ActionPlans));
        return market;
    }

    @Bean
    public ActionOrchestrator actionsProcessor() {
        // Only listen to actions.
        return aoConfig.actionOrchestratorClient();
    }
}
