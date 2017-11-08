package com.vmturbo.action.orchestrator.execution;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
import com.vmturbo.action.orchestrator.store.ActionCapabilitiesStore;
import com.vmturbo.action.orchestrator.store.ProbeActionCapabilitiesStore;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc;
import com.vmturbo.common.protobuf.topology.ProbeActionCapabilitiesServiceGrpc.ProbeActionCapabilitiesServiceBlockingStub;
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

    @Bean
    public ActionCapabilitiesStore actionCapabilitiesStore() {
        return new ProbeActionCapabilitiesStore(actionCapabilitiesService());
    }

    /**
     * Object which is used to resolve which target should execute the action if there a multiple
     * targets which can do it.
     *
     * @return implementation for ActionTargetResolver.
     */
    @Bean
    public ActionTargetResolver actionTargetResolver() {
        return new ActionTargetByProbeCategoryResolver(globalConfig.topologyProcessor(),
                actionCapabilitiesStore());
    }

    /**
     * Returns grpc service to get action capabilities.
     *
     * @return action capabilities grpc service
     */
    @Bean
    public ProbeActionCapabilitiesServiceBlockingStub actionCapabilitiesService() {
        return ProbeActionCapabilitiesServiceGrpc.newBlockingStub(globalConfig
                .topologyProcessorChannel());
    }

    @Bean
    public ActionExecutor actionExecutor() {
        final ActionExecutor executor =
                new ActionExecutor(globalConfig.topologyProcessorChannel(), actionTargetResolver());

        globalConfig.topologyProcessor().addActionListener(executor);

        return executor;
    }
}
