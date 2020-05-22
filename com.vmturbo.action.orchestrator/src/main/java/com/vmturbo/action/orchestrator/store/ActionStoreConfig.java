package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorDBConfig;
import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionHistoryDaoImpl;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.stats.ActionStatsConfig;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.topology.TpEntitiesWithNewStateListener;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModel;
import com.vmturbo.action.orchestrator.store.identity.ActionInfoModelCreator;
import com.vmturbo.action.orchestrator.store.identity.IdentityServiceImpl;
import com.vmturbo.action.orchestrator.store.identity.RecommendationIdentityStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Configuration for the ActionStore package.
 */
@Configuration
@Import({ActionOrchestratorDBConfig.class,
    ActionOrchestratorGlobalConfig.class,
    ActionExecutionConfig.class,
    GroupClientConfig.class,
    RepositoryClientConfig.class,
    ActionStatsConfig.class,
    ActionTranslationConfig.class,
    PlanOrchestratorClientConfig.class,
    TopologyProcessorConfig.class,
    UserSessionConfig.class,
    LicenseCheckClientConfig.class})
public class ActionStoreConfig {

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;

    @Autowired
    private ActionStatsConfig actionStatsConfig;

    @Autowired
    private ActionTranslationConfig actionTranslationConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Value("${entityTypeRetryIntervalMillis}")
    private long entityTypeRetryIntervalMillis;

    @Value("${entityTypeMaxRetries}")
    private long entityTypeMaxRetries;

    @Value("${minsToWaitForTopology:60}")
    private long minsToWaitForTopology;

    @Value("${actionExecution.concurrentAutomatedActions:5}")
    private int concurrentAutomatedActions;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory(actionModeCalculator());
    }

    @Bean
    public EntitiesAndSettingsSnapshotFactory entitySettingsCache() {
        return new EntitiesAndSettingsSnapshotFactory(
            groupClientConfig.groupChannel(),
            repositoryClientConfig.repositoryChannel(),
            tpConfig.realtimeTopologyContextId(),
            repositoryClientConfig.topologyAvailabilityTracker(),
            minsToWaitForTopology,
            TimeUnit.MINUTES);
    }

    @Bean
    public ActionModeCalculator actionModeCalculator() {
        return new ActionModeCalculator();
    }

    @Bean
    public ExecutorService automatedActionThreadpool() {
        return Executors.newFixedThreadPool(concurrentAutomatedActions);
    }

    @Bean
    public AutomatedActionExecutor automatedActionExecutor() {
        return new AutomatedActionExecutor(actionExecutionConfig.actionExecutor(),
            automatedActionThreadpool(),
            workflowConfig.workflowStore(),
            actionExecutionConfig.actionTargetSelector(),
            entitySettingsCache());
    }

    /**
     * Identity store for market recommendations.
     *
     * @return identity store
     */
    @Bean
    public RecommendationIdentityStore recommendationIdentityStore() {
        return new RecommendationIdentityStore(databaseConfig.dsl());
    }

    /**
     * Identity service for market recommendations.
     *
     * @return identity service
     */
    @Bean
    public IdentityServiceImpl<ActionInfo, ActionInfoModel> actionIdentityService() {
        return new IdentityServiceImpl<>(recommendationIdentityStore(),
                new ActionInfoModelCreator(), Clock.systemUTC(), 24 * 3600 * 1000);
    }

    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return new ActionStoreFactory(actionFactory(),
            tpConfig.realtimeTopologyContextId(),
            databaseConfig.dsl(),
            actionHistory(),
            actionExecutionConfig.actionTargetSelector(),
            actionExecutionConfig.targetCapabilityCache(),
            entitySettingsCache(),
            actionStatsConfig.actionsStatistician(),
            actionTranslationConfig.actionTranslator(),
            actionOrchestratorGlobalConfig.actionOrchestratorClock(),
            userSessionConfig.userSessionContext(),
            licenseCheckClientConfig.licenseCheckClient(),
            actionIdentityService());
    }

    @Bean
    public IActionStoreLoader actionStoreLoader() {
        // For now, only plan action stores (kept in PersistentImmutableActionStores)
        // need to be re-loaded at startup.
        return new PlanActionStore.StoreLoader(databaseConfig.dsl(),
            actionFactory(),
            actionModeCalculator(),
            entitySettingsCache(),
            actionTranslationConfig.actionTranslator(),
            realtimeTopologyContextId,
            actionExecutionConfig.actionTargetSelector());
    }

    @Bean
    public ActionStorehouse actionStorehouse() {
        ActionStorehouse actionStorehouse = new ActionStorehouse(actionStoreFactory(),
            automatedActionExecutor(),
                actionStoreLoader(), actionModeCalculator());
        tpConfig.topologyProcessor()
            .addEntitiesWithNewStatesListener(new TpEntitiesWithNewStateListener(actionStorehouse,
                tpConfig.realtimeTopologyContextId()));
        return actionStorehouse;
    }

    /**
     * Cleans up stale plan data in the action orchestrator.
     *
     * @return The {@link PlanGarbageDetector}.
     */
    @Bean
    public PlanGarbageDetector actionPlanGarbageDetector() {
        ActionPlanGarbageCollector collector = new ActionPlanGarbageCollector(actionStorehouse());
        return planOrchestratorClientConfig.newPlanGarbageDetector(collector);
    }

    @Bean
    public ActionHistoryDao actionHistory() {
        return new ActionHistoryDaoImpl(databaseConfig.dsl(), actionModeCalculator());
    }
}
