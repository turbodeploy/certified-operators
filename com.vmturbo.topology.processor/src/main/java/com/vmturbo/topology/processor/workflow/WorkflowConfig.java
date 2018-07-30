package com.vmturbo.topology.processor.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;

/**
 * Spring configuration for services related to Workflow handling.
 **/
@Configuration
@Import({ActionOrchestratorClientConfig.class})
public class WorkflowConfig {

    @Autowired
    ActionOrchestratorClientConfig actionOrchestratorClientConfig;

    @Bean
    public DiscoveredWorkflowUploader discoveredWorkflowUploader() {
        return new DiscoveredWorkflowUploader(
                actionOrchestratorClientConfig.actionOrchestratorChannel(),
                discoveredWorkflowInterpreter());
    }

    @Bean
    public DiscoveredWorkflowInterpreter discoveredWorkflowInterpreter() {
        return new DiscoveredWorkflowInterpreter();
    }

}
