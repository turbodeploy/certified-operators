package com.vmturbo.topology.processor.operation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.notification.api.NotificationApiConfig;
import com.vmturbo.notification.api.NotificationSender;
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
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
import com.vmturbo.topology.processor.planexport.PlanDestinationConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
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
    ControllableConfig.class,
    WorkflowConfig.class,
    PlanDestinationConfig.class,
    CloudCostConfig.class,
    TemplateConfig.class,
    ComponentBasedTargetDumpingSettingsConfig.class,
    NotificationApiConfig.class,
    MatrixConfig.class,
    LicenseCheckClientConfig.class,
    BinaryDiscoveryDumperConfig.class
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
    private BinaryDiscoveryDumperConfig binaryDiscoveryDumperConfig;

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
    private NotificationApiConfig notificationApiConfig;

    @Autowired
    private MatrixConfig matrixConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    // the following operation timeouts should be well above the ping/pong one
    @Value("${discoveryTimeoutSeconds:120}")
    private long discoveryTimeoutSeconds;

    @Value("${validationTimeoutSeconds:60}")
    private long validationTimeoutSeconds;

    @Value("${actionTimeoutSeconds:60}")
    private long actionTimeoutSeconds;

    @Value("${planExportTimeoutSeconds:120}")
    private long planExportTimeoutSeconds;

    @Value("${probeDiscoveryPermitWaitTimeoutMins:40}")
    private int probeDiscoveryPermitWaitTimeoutMins;

    @Value("${probeDiscoveryPermitWaitTimeoutIntervalMins:20}")
    private int probeDiscoveryPermitWaitTimeoutIntervalMins;

    @Value("${enableDiscoveryResponsesCaching:true}")
    private boolean enableDiscoveryResponsesCaching;

    @Value("${workflowExecutionTimeoutMillis:60000}")
    private int workflowExecutionTimeoutMillis;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Returns the notification sender implementation used to send notifications
     * to the system like probe failures.
     *
     * @return the configured notification sender.
     */
    @Bean
    public NotificationSender notificationSender() {
        return notificationApiConfig.notificationMessageSender();
    }

    /**
     * Returns the systemNotificationProducer that translates and sends notifications from probes
     * to the system.
     *
     * @return the configured system notification producer.
     */
    @Bean
    public SystemNotificationProducer systemNotificationProducer() {
        return new SystemNotificationProducer(notificationSender());
    }

    /**
     * Choose the right type of OperationManager depending on whether we're applying permits at the
     * container level or probe type level.
     *
     * @return OperationManager of proper type.
     */
    @Bean
    public IOperationManager operationManager() {
        return sdkServerConfig.getApplyPermitsToContainers()
                ? new OperationManagerWithQueue(identityProviderConfig.identityProvider(),
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
                systemNotificationProducer(),
                sdkServerConfig.discoveryQueue(),
                discoveryTimeoutSeconds,
                validationTimeoutSeconds,
                actionTimeoutSeconds,
                planExportTimeoutSeconds,
                matrixConfig.matrixInterface(),
                binaryDiscoveryDumperConfig.binaryDiscoveryDumper(),
                enableDiscoveryResponsesCaching,
                licenseCheckClientConfig.licenseCheckClient(),
                workflowExecutionTimeoutMillis
            )
                : new OperationManager(identityProviderConfig.identityProvider(),
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
            systemNotificationProducer(),
            discoveryTimeoutSeconds,
            validationTimeoutSeconds,
            actionTimeoutSeconds,
            planExportTimeoutSeconds,
            sdkServerConfig.getMaxConcurrentTargetDiscoveriesPerProbeCount(),
            sdkServerConfig.getMaxConcurrentTargetIncrementalDiscoveriesPerProbeCount(),
            probeDiscoveryPermitWaitTimeoutMins,
            probeDiscoveryPermitWaitTimeoutIntervalMins,
            matrixConfig.matrixInterface(),
            binaryDiscoveryDumperConfig.binaryDiscoveryDumper(),
            enableDiscoveryResponsesCaching,
            licenseCheckClientConfig.licenseCheckClient(),
                workflowExecutionTimeoutMillis
        );
    }
}
