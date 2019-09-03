package com.vmturbo.action.orchestrator.store;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

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
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for the ActionStore package.
 */
@Configuration
@Import({SQLDatabaseConfig.class,
    ActionOrchestratorGlobalConfig.class,
    ActionExecutionConfig.class,
    GroupClientConfig.class,
    RepositoryClientConfig.class,
    ActionStatsConfig.class,
    ActionTranslationConfig.class,
    UserSessionConfig.class})
public class ActionStoreConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

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

    @Value("${entityRetrievalRetryIntervalMillis:2000}")
    private int entityRetrievalRetryIntervalMillis;

    @Value("${entityRetrievalMaxRetries:900}")
    private int entityRetrievalMaxRetries;

    @Value("${actionExecution.concurrentAutomatedActions:1}")
    private int concurrentAutomatedActions;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory(actionModeCalculator());
    }

    @Bean
    public EntitiesAndSettingsSnapshotFactory entitySettingsCache() {
        return new EntitiesAndSettingsSnapshotFactory(
            groupClientConfig.groupChannel(),
            repositoryClientConfig.repositoryChannel(),
            entityRetrievalRetryIntervalMillis,
            entityRetrievalMaxRetries,
            actionOrchestratorGlobalConfig.realtimeTopologyContextId());
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
                actionExecutionConfig.actionTargetSelector());
    }

    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return new ActionStoreFactory(actionFactory(),
            actionOrchestratorGlobalConfig.realtimeTopologyContextId(),
            databaseConfig.dsl(),
            actionHistory(),
            actionExecutionConfig.actionTargetSelector(),
            actionExecutionConfig.targetCapabilityCache(),
            entitySettingsCache(),
            actionStatsConfig.actionsStatistician(),
            actionTranslationConfig.actionTranslator(),
            actionOrchestratorGlobalConfig.actionOrchestratorClock(),
            userSessionConfig.userSessionContext());
    }

    @Bean
    public IActionStoreLoader actionStoreLoader() {
        // For now, only plan action stores (kept in PersistentImmutableActionStores)
        // need to be re-loaded at startup.
        return new PlanActionStore.StoreLoader(databaseConfig.dsl(),
            actionFactory(),
            actionModeCalculator(),
            entitySettingsCache(),
            actionTranslationConfig.actionTranslator());
    }

    @Bean
    public ActionStorehouse actionStorehouse() {
        return new ActionStorehouse(actionStoreFactory(), automatedActionExecutor(),
                actionStoreLoader(), actionModeCalculator());
    }

    @Bean
    public ActionHistoryDao actionHistory() {
        return new ActionHistoryDaoImpl(databaseConfig.dsl(), actionModeCalculator());
    }
}
