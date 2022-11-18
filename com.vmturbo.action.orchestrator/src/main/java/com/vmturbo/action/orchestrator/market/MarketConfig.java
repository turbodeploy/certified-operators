package com.vmturbo.action.orchestrator.market;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.audit.AuditCommunicationConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.stats.ActionStatsConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.store.pipeline.LiveActionPipelineFactory;
import com.vmturbo.action.orchestrator.store.pipeline.PlanActionPipelineFactory;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
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

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private ActionTranslationConfig actionTranslationConfig;

    @Autowired
    private ActionStatsConfig actionStatsConfig;

    @Autowired
    private AuditCommunicationConfig auditCommunicationConfig;

    /**
     * The maximum allowed age of an action plan in seconds.
     * If we receive a live action plan older than this value, it will be discarded.
     * Plan action plans are not discarded due to age.
     */
    @Value("${maxLiveActionPlanAgeSeconds:600}")
    private long maxLiveActionPlanAgeSeconds;

    /**
     * Max time to wait in minutes for the shared live actions lock during live
     * actions pipeline execution before timing out. Must be > 0.
     */
    @Value("${liveActionsLockMaxWaitTimeMinutes:60}")
    private long liveActionsLockMaxWaitTimeMinutes;

    @Bean
    public ActionPlanAssessor actionPlanAssessor() {
        return new ActionPlanAssessor(actionOrchestratorGlobalConfig.actionOrchestratorClock(),
                maxLiveActionPlanAgeSeconds);
    }

    @Bean
    public LiveActionPipelineFactory liveActionPipelineFactory() {
        return new LiveActionPipelineFactory(actionStoreConfig.actionStorehouse(),
            actionStoreConfig.automationManager(), actionStoreConfig.atomicActionFactory(),
            actionStoreConfig.entitySettingsCache(), liveActionsLockMaxWaitTimeMinutes,
            actionExecutionConfig.targetCapabilityCache(), actionStoreConfig.actionHistory(),
            actionStoreConfig.actionFactory(), actionOrchestratorGlobalConfig.actionOrchestratorClock(),
            actionStoreConfig.queryTimeWindowForLastExecutedActionsMins(),
            actionStoreConfig.actionIdentityService(), actionExecutionConfig.actionTargetSelector(),
            actionTranslationConfig.actionTranslator(), actionStatsConfig.actionsStatistician(),
            auditCommunicationConfig.actionAuditSender(), auditCommunicationConfig.auditedActionsManager(),
                                             topologyProcessorConfig.actionTopologyStore(),
                topologyProcessorConfig.realtimeTopologyContextId(),
            actionStatsConfig.telemetryChunkSize());
    }

    /**
     * The planActionPipelineFactory.
     *
     * @return The planActionPipelineFactory.
     */
    @Bean
    public PlanActionPipelineFactory planActionPipelineFactory() {
        return new PlanActionPipelineFactory(actionStoreConfig.actionStorehouse(),
                actionStoreConfig.planAtomicActionFactory(),
                actionOrchestratorGlobalConfig.actionOrchestratorClock());
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
        return new MarketActionListener(orchestrator(), actionPlanAssessor(),
                topologyProcessorConfig.realtimeTopologyContextId());
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
            MarketSubscription.forTopic(Topic.ActionPlans),
            MarketSubscription.forTopicWithStartFrom(Topic.AnalysisSummary, StartFrom.BEGINNING),
            MarketSubscription.forTopic(Topic.ProjectedTopologies));
        market.addActionsListener(marketActionListener());
        market.addProjectedTopologyListener(actionStoreConfig.entitiesSnapshotFactory());
        return market;
    }
}
