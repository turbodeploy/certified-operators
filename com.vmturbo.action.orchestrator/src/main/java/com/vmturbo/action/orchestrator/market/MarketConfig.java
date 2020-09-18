package com.vmturbo.action.orchestrator.market;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.action.orchestrator.store.pipeline.PlanActionPipelineFactory;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.market.component.api.impl.MarketSubscription.Topic;

/**
 * Configuration for integration with the {@link MarketComponent}.
 */
@Configuration
@Import({
    ActionOrchestratorApiConfig.class,
    ActionStoreConfig.class,
    MarketClientConfig.class,
    TopologyProcessorConfig.class
})
public class MarketConfig {

    @Autowired
    private ActionOrchestratorApiConfig apiConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Autowired
    private TopologyProcessorConfig topologyProcessorConfig;

    /**
     * The maximum allowed age of an action plan in seconds.
     * If we receive a live action plan older than this value, it will be discarded.
     * Plan action plans are not discarded due to age.
     */
    @Value("${maxLiveActionPlanAgeSeconds:600}")
    private long maxLiveActionPlanAgeSeconds;

    @Bean
    public ActionPlanAssessor actionPlanAssessor() {
        return new ActionPlanAssessor(Clock.systemUTC(),
            maxLiveActionPlanAgeSeconds);
    }

    @Bean
    public LiveActionPipelineFactory liveActionPipelineFactory() {
        return new LiveActionPipelineFactory(actionStoreConfig.actionStorehouse(),
            actionStoreConfig.automationManager());
    }

    /**
     * The planActionPipelineFactory.
     *
     * @return The planActionPipelineFactory.
     */
    @Bean
    public PlanActionPipelineFactory planActionPipelineFactory() {
        return new PlanActionPipelineFactory(actionStoreConfig.actionStorehouse());
    }

    /**
     * The orchestrator for actions.
     *
     * @return The orchestrator for actions.
     */
    @Bean
    public ActionOrchestrator orchestrator() {
        return new ActionOrchestrator(liveActionPipelineFactory(), planActionPipelineFactory(),
            apiConfig.actionOrchestratorNotificationSender(), topologyProcessorConfig.realtimeTopologyContextId());
    }

    @Bean
    public MarketActionListener marketActionListener() {
        return new MarketActionListener(orchestrator(), actionPlanAssessor());
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
            MarketSubscription.forTopic(Topic.ActionPlans),
            MarketSubscription.forTopicWithStartFrom(Topic.AnalysisSummary, StartFrom.BEGINNING));
        market.addActionsListener(marketActionListener());
        return market;
    }
}
