package com.vmturbo.action.orchestrator.execution;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * Configuration for classes required to execute actions via
 * the {@link TopologyProcessor}.
 */
@Configuration
@Import({ActionOrchestratorGlobalConfig.class})
public class ActionExecutionConfig {

    @Autowired
    private ActionOrchestratorGlobalConfig globalConfig;

    /**
     * Object which is used to resolve which target should execute the action if there a multiple
     * targets which can do it.
     *
     * @return implementation for ActionTargetResolver.
     */
    @Bean
    public ActionTargetResolver actionTargetResolver() {
        return new ActionTargetByProbeCategoryResolver(globalConfig.topologyProcessor());
    }

    @Bean
    public ActionExecutor actionExecutor() {
        return new ActionExecutor(globalConfig.topologyProcessorChannel(), actionTargetResolver());
    }
}
