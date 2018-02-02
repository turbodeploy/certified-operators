package com.vmturbo.action.orchestrator.execution.notifications;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Configuration for integration with the {@link TopologyProcessorClient}.
 */
@Configuration
@Import({ActionStoreConfig.class, ActionOrchestratorGlobalConfig.class, ActionOrchestratorApiConfig.class})
public class NotificationsConfig {

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired ActionOrchestratorApiConfig apiConfig;

    @Bean
    public ActionExecutionListener actionExecutionListener() {
        final ActionExecutionListener executionListener = new ActionStateUpdater(
            actionStoreConfig.actionStorehouse(),
            apiConfig.actionOrchestratorNotificationSender(),
            actionStoreConfig.actionHistory(),
            globalConfig.realtimeTopologyContextId()
        );

        globalConfig.topologyProcessor().addActionListener(executionListener);
        return executionListener;
    }

}
