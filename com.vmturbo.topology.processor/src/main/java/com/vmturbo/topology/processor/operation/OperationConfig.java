package com.vmturbo.topology.processor.operation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.templates.DiscoveredTemplateDeploymentProfileConfig;

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
    DiscoveredTemplateDeploymentProfileConfig.class
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
    private DiscoveredTemplateDeploymentProfileConfig discoveredTemplateDeploymentProfileConfig;

    @Value("${discoveryTimeoutSeconds}")
    private long discoveryTimeoutSeconds;

    @Value("${validationTimeoutSeconds}")
    private long validationTimeoutSeconds;

    @Value("${actionTimeoutSeconds}")
    private long actionTimeoutSeconds;

    @Bean
    public IOperationManager operationManager() {
        return new OperationManager(identityProviderConfig.identityProvider(),
            targetConfig.targetStore(),
            probeConfig.probeStore(),
            sdkServerConfig.remoteMediation(),
            apiConfig.topologyProcessorNotificationSender(),
            entityConfig.entityStore(),
            groupConfig.discoveredGroupUploader(),
            discoveredTemplateDeploymentProfileConfig.discoveredTemplatesUploader(),
            discoveryTimeoutSeconds,
            validationTimeoutSeconds,
            actionTimeoutSeconds
        );
    }
}
