package com.vmturbo.voltron.extensions.tp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.cost.CloudCostConfig;
import com.vmturbo.topology.processor.discoverydumper.BinaryDiscoveryDumperConfig;
import com.vmturbo.topology.processor.discoverydumper.ComponentBasedTargetDumpingSettingsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.ncm.MatrixConfig;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.OperationConfig;
import com.vmturbo.topology.processor.planexport.PlanDestinationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.workflow.WorkflowConfig;

/**
 * Configuration for the IOperationManager that replaces OperationConfig in order to return a
 * CacheOnlyOperationManager for cache only mode.  This class extends OperationConfig so that it
 * can inherit a method but it doesn't really have to if we prefer to change that at some point.
 */
@Configuration
@Primary
public class CacheOnlyOperationConfig extends OperationConfig {

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
    private BinaryDiscoveryDumperConfig binaryDiscoveryDumperConfig;

    @Autowired
    private CacheOnlyDiscoveryDumperConfig cacheOnlyDiscoveryDumperConfig;

    @Autowired
    private SdkServerConfig sdkServerConfig;

    @Autowired
    private TopologyProcessorApiConfig apiConfig;

    @Autowired
    private ControllableConfig controllableConfig;

    @Autowired
    private WorkflowConfig workflowConfig;

    @Autowired
    private PlanDestinationConfig planDestinationConfig;

    @Autowired
    private CloudCostConfig cloudCostUploaderConfig;

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private ComponentBasedTargetDumpingSettingsConfig componentBasedTargetDumpingSettingsConfig;

    @Autowired
    private MatrixConfig matrixConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Value("${discoveryTimeoutSeconds:120}")
    private long discoveryTimeoutSeconds;

    @Value("${validationTimeoutSeconds:60}")
    private long validationTimeoutSeconds;

    @Value("${actionTimeoutSeconds:60}")
    private long actionTimeoutSeconds;

    @Value("${planExportTimeoutSeconds:120}")
    private long planExportTimeoutSeconds;

    @Value("${enableDiscoveryResponsesCaching:true}")
    private boolean enableDiscoveryResponsesCaching;

    @Value("${workflowExecutionTimeoutMillis:60000}")
    private int workflowExecutionTimeoutMillis;

    @Value("${isCacheDiscoveryModeOffline:false}")
    private boolean isCacheDiscoveryModeOffline;

    /**
     * Returns operation manager implementation for cache only mode.
     *
     * @return a {@link CacheOnlyOperationManager} operation manager implementation
     */
    @Bean
    public IOperationManager operationManager() {
        return new CacheOnlyOperationManager(identityProviderConfig.identityProvider(),
                targetConfig.targetStore(),
                probeConfig.probeStore(),
                sdkServerConfig.remoteMediation(),
                apiConfig.topologyProcessorNotificationSender(),
                entityConfig.entityStore(),
                groupConfig.discoveredGroupUploader(),
                workflowConfig.discoveredWorkflowUploader(),
                cloudCostUploaderConfig.discoveredCloudCostUploader(),
                cloudCostUploaderConfig.billedCloudCostUploader(),
                cloudCostUploaderConfig.aliasedOidsUploader(),
                planDestinationConfig.discoveredPlanDestinationUploader(),
                templateConfig.discoveredTemplatesUploader(),
                controllableConfig.entityActionDao(),
                targetConfig.derivedTargetParser(),
                groupConfig.groupScopeResolver(),
                componentBasedTargetDumpingSettingsConfig.componentBasedTargetDumpingSettings(),
                super.systemNotificationProducer(),
                sdkServerConfig.discoveryQueue(),
                discoveryTimeoutSeconds,
                validationTimeoutSeconds,
                actionTimeoutSeconds,
                planExportTimeoutSeconds,
                matrixConfig.matrixInterface(),
                binaryDiscoveryDumperConfig.binaryDiscoveryDumper(),
                cacheOnlyDiscoveryDumperConfig.cacheOnlyDiscoveryDumper(),
                enableDiscoveryResponsesCaching,
                licenseCheckClientConfig.licenseCheckClient(),
                workflowExecutionTimeoutMillis,
                isCacheDiscoveryModeOffline);
    }
}
