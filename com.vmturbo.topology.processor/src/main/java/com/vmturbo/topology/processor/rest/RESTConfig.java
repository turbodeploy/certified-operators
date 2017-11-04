package com.vmturbo.topology.processor.rest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.diagnostics.TopologyProcessorDiagnosticsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.scheduling.SchedulerConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the Controllers package in TopologyProcessor.
 */
@Configuration
@Import({
    EntityConfig.class,
    OperationConfig.class,
    TargetConfig.class,
    TopologyConfig.class,
    ProbeConfig.class,
    SchedulerConfig.class,
    GroupConfig.class,
    TopologyProcessorDiagnosticsConfig.class,
})
public class RESTConfig {
    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private OperationConfig operationConfig;

    @Autowired
    private SchedulerConfig schedulerConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private TopologyProcessorDiagnosticsConfig diagnosticsConfig;

    @Bean
    public TopologyController topologyController() {
        return new TopologyController(
            schedulerConfig.scheduler(),
            topologyConfig.topologyHandler(),
            entityConfig.entityStore(),
            groupConfig.policyManager(),
            targetConfig.targetStore()
        );
    }

    @Bean
    public OperationController operationController() {
        return new OperationController(
            operationConfig.operationManager(),
            schedulerConfig.scheduler(),
            targetConfig.targetStore()
        );
    }

    @Bean
    public ProbeController probeController() {
        return new ProbeController(probeConfig.probeStore());
    }

    @Bean
    public TargetController targetController() {
        return new TargetController(
                schedulerConfig.scheduler(),
                targetConfig.targetStore(),
                probeConfig.probeStore(),
                operationConfig.operationManager(),
                topologyConfig.topologyHandler()
        );
    }

    @Bean
    public DiagnosticsController diagnosticsController() {
        return new DiagnosticsController(diagnosticsConfig.diagsHandler());
    }
}
