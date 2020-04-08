package com.vmturbo.topology.processor.operation;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.notification.api.NotificationApiConfig;
import com.vmturbo.notification.api.NotificationSender;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.communication.SdkServerConfig;
import com.vmturbo.topology.processor.controllable.ControllableConfig;
import com.vmturbo.topology.processor.cost.CloudCostConfig;
import com.vmturbo.topology.processor.discoverydumper.ComponentBasedTargetDumpingSettingsConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.ncm.MatrixConfig;
import com.vmturbo.topology.processor.notification.SystemNotificationProducer;
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
    CloudCostConfig.class,
    TemplateConfig.class,
    ComponentBasedTargetDumpingSettingsConfig.class,
    NotificationApiConfig.class,
    MatrixConfig.class
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
    private TemplateConfig templateConfig;

    @Autowired
    private ComponentBasedTargetDumpingSettingsConfig componentBasedTargetDumpingSettingsConfig;

    @Autowired
    private NotificationApiConfig notificationApiConfig;

    @Autowired
    private MatrixConfig matrixConfig;

    @Value("${discoveryTimeoutSeconds}")
    private long discoveryTimeoutSeconds;

    @Value("${validationTimeoutSeconds}")
    private long validationTimeoutSeconds;

    @Value("${actionTimeoutSeconds}")
    private long actionTimeoutSeconds;

    @Value("${maxConcurrentTargetDiscoveriesPerProbeCount}")
    private int maxConcurrentTargetDiscoveriesPerProbeCount;

    @Value("${maxConcurrentTargetIncrementalDiscoveriesPerProbeCount}")
    private int maxConcurrentTargetIncrementalDiscoveriesPerProbeCount;

    @Value("${probeDiscoveryPermitWaitTimeoutMins}")
    private int probeDiscoveryPermitWaitTimeoutMins;

    @Value("${probeDiscoveryPermitWaitTimeoutIntervalMins}")
    private int probeDiscoveryPermitWaitTimeoutIntervalMins;

    /**
     * Returns the notification sender implementation used to send notifications
     * to the system like probe failures.
     *
     * @return the configured notification sender.
     */
    @Bean
    public NotificationSender notificationSender() {
        return new NotificationSender(
            notificationApiConfig.notificationMessageSender(),
            Clock.systemUTC());
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
            templateConfig.discoveredTemplatesUploader(),
            controllableConfig.entityActionDaoImp(),
            targetConfig.derivedTargetParser(),
            targetConfig.groupScopeResolver(),
            componentBasedTargetDumpingSettingsConfig.componentBasedTargetDumpingSettings(),
            systemNotificationProducer(),
            discoveryTimeoutSeconds,
            validationTimeoutSeconds,
            actionTimeoutSeconds,
            maxConcurrentTargetDiscoveriesPerProbeCount,
            maxConcurrentTargetIncrementalDiscoveriesPerProbeCount,
            probeDiscoveryPermitWaitTimeoutMins,
            probeDiscoveryPermitWaitTimeoutIntervalMins,
            matrixConfig.matrixInterface()
        );
    }
}
