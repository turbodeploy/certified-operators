package com.vmturbo.topology.processor.operation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.cost.CloudCostConfig;
import com.vmturbo.topology.processor.discoverydumper.ComponentBasedTargetDumpingSettingsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.plan.PlanConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.workflow.WorkflowConfig;

/**
 * Configuration for the Operation package.
 */
@Configuration
@Import({
    EntityConfig.class,
    GroupConfig.class,
    IdentityProviderConfig.class,
    ProbeConfig.class,
    SdkServerConfig.class,
    TargetConfig.class,
    IdentityProviderConfig.class,
    TopologyProcessorApiConfig.class,
    PlanConfig.class,
    ControllableConfig.class,
    WorkflowConfig.class,
    CloudCostConfig.class,
    ComponentBasedTargetDumpingSettingsConfig.class
})
public class OperationConfig {

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private SdkServerConfig sdkServerConfig;

    @Autowired
    private TopologyProcessorApiConfig apiConfig;

    @Autowired
    private ControllableConfig controllableConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private CloudCostConfig cloudCostUploaderConfig;

    @Autowired
    private PlanConfig discoveredTemplateDeploymentProfileConfig;

    @Autowired
    private ComponentBasedTargetDumpingSettingsConfig componentBasedTargetDumpingSettingsConfig;

    @Value("${discoveryTimeoutSeconds}")
    private long discoveryTimeoutSeconds;

    @Value("${validationTimeoutSeconds}")
    private long validationTimeoutSeconds;

    @Value("${actionTimeoutSeconds}")
    private long actionTimeoutSeconds;

    @Value("${maxConcurrentTargetDiscoveriesPerProbeCount}")
    private int maxConcurrentTargetDiscoveriesPerProbeCount;

    @Value("${probeDiscoveryPermitWaitTimeoutMins}")
    private int probeDiscoveryPermitWaitTimeoutMins;

    @Value("${probeDiscoveryPermitWaitTimeoutIntervalMins}")
    private int probeDiscoveryPermitWaitTimeoutIntervalMins;

    @Bean
    public IOperationManager operationManager() {
        return new OperationManager(identityProviderConfig.identityProvider(),
            targetConfig.targetStore(),
            probeConfig.probeStore(),
            sdkServerConfig.remoteMediation(),
            apiConfig.topologyProcessorNotificationSender(),
            entityConfig.entityStore(),
            groupConfig.discoveredGroupUploader(),
            workflowConfig.discoveredWorkflowUploader(),
            cloudCostUploaderConfig.discoveredCloudCostUploader(),
            discoveredTemplateDeploymentProfileConfig.discoveredTemplatesUploader(),
            controllableConfig.entityActionDaoImp(),
            targetConfig.derivedTargetParser(),
            targetConfig.groupScopeResolver(),
            componentBasedTargetDumpingSettingsConfig.componentBasedTargetDumpingSettings(),
            discoveryTimeoutSeconds,
            validationTimeoutSeconds,
            actionTimeoutSeconds,
            maxConcurrentTargetDiscoveriesPerProbeCount,
            probeDiscoveryPermitWaitTimeoutMins,
            probeDiscoveryPermitWaitTimeoutIntervalMins
        );
    }
}
