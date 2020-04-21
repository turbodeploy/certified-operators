package com.vmturbo.action.orchestrator.store;

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
import com.vmturbo.action.orchestrator.translation.ActionTranslationConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
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
    UserSessionConfig.class})
public class ActionStoreConfig {

    @Autowired
    private ActionOrchestratorDBConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

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
            actionOrchestratorGlobalConfig.realtimeTopologyContextId(),
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
     * Returns the {@link ActionStoreFactory} bean.
     *
     * @return the {@link ActionStoreFactory} bean.
     */
    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return ActionStoreFactory.newBuilder()
            .withActionFactory(actionFactory())
            .withRealtimeTopologyContextId(actionOrchestratorGlobalConfig.realtimeTopologyContextId())
            .withDatabaseDslContext(databaseConfig.dsl())
            .withActionHistoryDao(actionHistory())
            .withActionTargetSelector(actionExecutionConfig.actionTargetSelector())
            .withProbeCapabilityCache(actionExecutionConfig.targetCapabilityCache())
            .withEntitySettingsCache(entitySettingsCache())
            .withActionsStatistician(actionStatsConfig.actionsStatistician())
            .withActionTranslator(actionTranslationConfig.actionTranslator())
            .withClock(actionOrchestratorGlobalConfig.actionOrchestratorClock())
            .withUserSessionContext(userSessionConfig.userSessionContext())
            .withSupplyChainService(SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()))
            .withRepositoryService(RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()))
            .build();
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
            SupplyChainServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()),
            RepositoryServiceGrpc.newBlockingStub(repositoryClientConfig.repositoryChannel()));
    }

    @Bean
    public ActionStorehouse actionStorehouse() {
        return new ActionStorehouse(actionStoreFactory(), automatedActionExecutor(),
            actionStoreLoader(), actionModeCalculator());
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
