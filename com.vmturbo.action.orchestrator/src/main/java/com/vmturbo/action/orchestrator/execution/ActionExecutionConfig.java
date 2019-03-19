package com.vmturbo.action.orchestrator.execution;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.action.orchestrator.ActionOrchestratorGlobalConfig;
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
    public ProbeCapabilityCache targetCapabilityCache() {
        return new ProbeCapabilityCache(globalConfig.topologyProcessor(), actionCapabilitiesService());
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
    public ActionExecutionEntitySelector actionExecutionTargetEntitySelector() {
        return new EntityAndActionTypeBasedEntitySelector();
    }

    @Bean
    public ActionExecutor actionExecutor() {
        final ActionExecutor executor =
                new ActionExecutor(globalConfig.topologyProcessorChannel());

        globalConfig.topologyProcessor().addActionListener(executor);

        return executor;
    }

    @Bean
    public ActionTargetSelector actionTargetSelector() {
        final ActionTargetSelector actionTargetSelector =
                new ActionTargetSelector(targetCapabilityCache(),
                        actionExecutionTargetEntitySelector(),
                        globalConfig.topologyProcessorChannel());
        return actionTargetSelector;
    }
}
