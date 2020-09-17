package com.vmturbo.topology.processor.template;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc;
import com.vmturbo.common.protobuf.plan.DiscoveredTemplateDeploymentProfileServiceGrpc.DiscoveredTemplateDeploymentProfileServiceStub;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.topology.processor.cpucapacity.CpuCapacityConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * All the configuration related to the conversion of templates to TopologyEntityDTO.
 */
@Configuration
@Import({
    IdentityProviderConfig.class,
    EntityConfig.class,
    TargetConfig.class,
    PlanOrchestratorClientConfig.class
})
public class TemplateConfig {

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private CpuCapacityConfig cpuCapacityConfig;

    @Value("${discoveredTemplateUploadTimeoutMin:10}")
    private long discoveredTemplateUploadTimeoutMin;

    /**
     * Bean responsible for converting templates to TopologyEntityDTO.
     *
     * @return the bean responsible for converting templates to TopologyEntityDTO.
     */
    @Bean
    public TemplateConverterFactory templateConverterFactory() {
        return new TemplateConverterFactory(templateServiceBlockingStub(),
                identityProviderConfig.identityProvider(),
                groupConfig.settingPolicyServiceClient(),
                cpuCapacityConfig.cpuCapacityServiceBlockingStub());
    }

    /**
     * gRPC stub to access the template service in the plan orchestrator.
     *
     * @return The gRPC stub.
     */
    @Bean
    public TemplateServiceBlockingStub templateServiceBlockingStub() {
        return TemplateServiceGrpc.newBlockingStub(planOrchestratorClientConfig.planOrchestratorChannel());
    }

    /**
     * Non blocking service stub for streaming messages.
     *
     * @return New non blocking service stub
     */
    @Bean
    public DiscoveredTemplateDeploymentProfileServiceStub nonBlockingtemplateDeploymentProfileRpcService() {
        return DiscoveredTemplateDeploymentProfileServiceGrpc.newStub(
            planOrchestratorClientConfig.planOrchestratorChannel());
    }

    /**
     * Uploader for discovered templates and deployment profiles.
     *
     * @return The {@link DiscoveredTemplateDeploymentProfileUploader}.
     */
    @Bean
    public DiscoveredTemplateDeploymentProfileUploader discoveredTemplatesUploader() {
        return new DiscoveredTemplateDeploymentProfileUploader(entityConfig.entityStore(),
            targetConfig.targetStore(),
            nonBlockingtemplateDeploymentProfileRpcService(),
            discoveredTemplateUploadTimeoutMin, TimeUnit.MINUTES);
    }
}
