package com.vmturbo.action.orchestrator.store;

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
    ActionTranslationConfig.class})
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

    @Value("${entityTypeRetryIntervalMillis}")
    private long entityTypeRetryIntervalMillis;

    @Value("${entityTypeMaxRetries}")
    private long entityTypeMaxRetries;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory(actionModeCalculator());
    }

    @Bean
    public EntitiesCache entitySettingsCache() {
        return new EntitiesCache(groupClientConfig.groupChannel(), repositoryClientConfig.repositoryChannel());
    }

    @Bean
    public ActionModeCalculator actionModeCalculator() {
        return new ActionModeCalculator(actionTranslationConfig.actionTranslator());
    }

    @Bean
    public AutomatedActionExecutor automatedActionExecutor() {
        return new AutomatedActionExecutor(actionExecutionConfig.actionExecutor(),
                Executors.newSingleThreadExecutor(),
                actionTranslationConfig.actionTranslator(),
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
            actionStatsConfig.actionsStatistician());
    }

    @Bean
    public IActionStoreLoader actionStoreLoader() {
        // For now, only plan action stores (kept in PersistentImmutableActionStores)
        // need to be re-loaded at startup.
        return new PlanActionStore.StoreLoader(databaseConfig.dsl(), actionFactory(), actionModeCalculator());
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
