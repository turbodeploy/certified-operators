package com.vmturbo.action.orchestrator.store;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for the ActionStore package.
 */
@Configuration
@Import({SQLDatabaseConfig.class, ActionOrchestratorGlobalConfig.class})
public class ActionStoreConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory();
    }

    @Bean
    public ActionTranslator actionTranslator() {
        return new ActionTranslator(actionOrchestratorGlobalConfig.topologyProcessorChannel());
    }

    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return new ActionStoreFactory(actionFactory(),
            actionTranslator(),
            actionOrchestratorGlobalConfig.realtimeTopologyContextId(),
            databaseConfig.dsl(),
            actionOrchestratorGlobalConfig.topologyProcessorChannel(),
            actionOrchestratorGlobalConfig.groupChannel(),
            actionOrchestratorGlobalConfig.topologyProcessor());
    }

    @Bean
    public IActionStoreLoader actionStoreLoader() {
        // For now, only plan action stores (kept in PersistentImmutableActionStores)
        // need to be re-loaded at startup.
        return new PlanActionStore.StoreLoader(databaseConfig.dsl(), actionFactory());
    }

    @Bean
    public ActionStorehouse actionStorehouse() {
        return new ActionStorehouse(actionStoreFactory(), actionStoreLoader());
    }
}
