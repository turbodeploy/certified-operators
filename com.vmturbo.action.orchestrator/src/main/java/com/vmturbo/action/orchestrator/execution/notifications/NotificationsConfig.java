package com.vmturbo.action.orchestrator.execution.notifications;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.api.ActionOrchestratorApiConfig;
import com.vmturbo.action.orchestrator.execution.ActionExecutionConfig;
import com.vmturbo.action.orchestrator.store.ActionStoreConfig;
import com.vmturbo.action.orchestrator.topology.TopologyProcessorConfig;
import com.vmturbo.action.orchestrator.workflow.config.WorkflowConfig;
import com.vmturbo.topology.processor.api.ActionExecutionListener;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClient;

/**
 * Configuration for integration with the {@link TopologyProcessorClient}.
 */
@Configuration
@Import({ActionStoreConfig.class,
    ActionOrchestratorGlobalConfig.class,
    ActionOrchestratorApiConfig.class,
    ActionExecutionConfig.class,
    TopologyProcessorConfig.class,
    WorkflowConfig.class})
public class NotificationsConfig {

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    @Autowired
    private ActionStoreConfig actionStoreConfig;

    @Autowired ActionOrchestratorApiConfig apiConfig;

    @Autowired
    private ActionExecutionConfig actionExecutionConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private TopologyProcessorConfig tpConfig;

    /**
     * Bean for {@link ActionExecutionListener}.
     * @return The {@link ActionExecutionListener}.
     */
    @Bean
    public ActionExecutionListener actionExecutionListener() {
        final ActionExecutionListener executionListener = new ActionStateUpdater(
            actionStoreConfig.actionStorehouse(),
            apiConfig.actionOrchestratorNotificationSender(),
            actionStoreConfig.actionHistory(),
            actionExecutionConfig.actionExecutor(),
            workflowConfig.workflowStore(),
            tpConfig.realtimeTopologyContextId(),
                actionExecutionConfig.failedCloudVMGroupProcessor());
        tpConfig.topologyProcessor().addActionListener(executionListener);
        return executionListener;
    }

}
