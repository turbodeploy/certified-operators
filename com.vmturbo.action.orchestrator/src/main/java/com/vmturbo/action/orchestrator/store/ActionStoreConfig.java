package com.vmturbo.action.orchestrator.store;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.execution.ActionTranslator;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

/**
 * Configuration for the ActionStore package.
 */
@Configuration
@Import({SQLDatabaseConfig.class, ActionOrchestratorGlobalConfig.class,
        ActionExecutionConfig.class})
public class ActionStoreConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private ActionOrchestratorGlobalConfig actionOrchestratorGlobalConfig;

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;


    @Value("${repositoryHost}")
    private String repositoryHost;

    @Value("${server.port}")
    private int httpPort;

    @Value("${entityTypeRetryIntervalMillis}")
    private long entityTypeRetryIntervalMillis;

    @Value("${entityTypeMaxRetries}")
    private long entityTypeMaxRetries;

    @Bean
    public IActionFactory actionFactory() {
        return new ActionFactory();
    }

    @Bean
    public ActionTranslator actionTranslator() {
        return new ActionTranslator(actionOrchestratorGlobalConfig.topologyProcessorChannel());
    }

    @Bean
    public EntitySettingsCache entitySettingsCache() {
        return new EntitySettingsCache(actionOrchestratorGlobalConfig.groupChannel(),
                serviceRestTemplate(), repositoryHost, httpPort, Executors.newSingleThreadExecutor(),
                entityTypeMaxRetries, entityTypeRetryIntervalMillis);
    }

    @Bean
    public RestTemplate serviceRestTemplate() {
        RestTemplate restTemplate;
        // for communication with repository component
        restTemplate = new RestTemplate();
        final Jaxb2RootElementHttpMessageConverter msgConverter = new Jaxb2RootElementHttpMessageConverter();
        restTemplate.getMessageConverters().add(msgConverter);
        return restTemplate;
    }

    @Bean
    public AutomatedActionExecutor automatedActionExecutor() {
        return new AutomatedActionExecutor(actionExecutionConfig.actionExecutor(),
                Executors.newSingleThreadExecutor(),
                actionTranslator());
    }

    @Bean
    public IActionStoreFactory actionStoreFactory() {
        return new ActionStoreFactory(actionFactory(),
            actionTranslator(),
            actionOrchestratorGlobalConfig.realtimeTopologyContextId(),
            databaseConfig.dsl(),
            actionOrchestratorGlobalConfig.topologyProcessorChannel(),
            actionOrchestratorGlobalConfig.topologyProcessor(),
            entitySettingsCache());
    }

    @Bean
    public IActionStoreLoader actionStoreLoader() {
        // For now, only plan action stores (kept in PersistentImmutableActionStores)
        // need to be re-loaded at startup.
        return new PlanActionStore.StoreLoader(databaseConfig.dsl(), actionFactory());
    }

    @Bean
    public ActionStorehouse actionStorehouse() {
        return new ActionStorehouse(actionStoreFactory(), automatedActionExecutor(),
                actionStoreLoader());
    }
}
