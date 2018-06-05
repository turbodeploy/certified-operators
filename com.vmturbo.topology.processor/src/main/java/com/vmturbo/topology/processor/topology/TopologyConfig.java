package com.vmturbo.topology.processor.topology;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.group.discovery.DiscoveredSettingPolicyScanner;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.plan.PlanConfig;
import com.vmturbo.topology.processor.probes.ProbeConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.reservation.ReservationConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.stitching.StitchingGroupFixer;
import com.vmturbo.topology.processor.supplychain.SupplyChainValidationConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;
import com.vmturbo.topology.processor.template.TemplateConfig;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;

/**
 * Configuration for the Topology package in TopologyProcessor.
 */
@Configuration
@Import({
    TopologyProcessorApiConfig.class,
    EntityConfig.class,
    SupplyChainValidationConfig.class,
    IdentityProviderConfig.class,
    GroupConfig.class,
    StitchingConfig.class,
    PlanConfig.class,
    RepositoryConfig.class,
    TemplateConfig.class,
    ClockConfig.class,
    ReservationConfig.class,
    ProbeConfig.class,
    TargetConfig.class
})
public class TopologyConfig {

    @Autowired
    private TopologyProcessorApiConfig apiConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private GroupConfig groupConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private StitchingConfig stitchingConfig;

    @Autowired
    private TemplateConfig templateConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private ReservationConfig reservationConfig;

    @Autowired
    private ProbeConfig probeConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private SupplyChainValidationConfig supplyChainValidationConfig;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public SupplyChainValidationConfig supplyChainValidationConfig() {
        return new SupplyChainValidationConfig();
    }

    @Bean
    public TopologyHandler topologyHandler() {
        return new TopologyHandler(realtimeTopologyContextId(),
                topologyPipelineFactory(),
                identityProviderConfig.identityProvider(),
                entityConfig.entityStore(),
                clockConfig.clock());
    }

    @Bean
    public TopologyEditor topologyEditor() {
        return new TopologyEditor(identityProviderConfig.identityProvider(),
                templateConfig.templateConverterFactory(),
                // we don't use groupResolver cache here because we want
                // up-to-date results.
                groupConfig.groupServiceClient());
    }

    @Bean
    public DiscoveredSettingPolicyScanner discoveredSettingPolicyScanner() {
        return new DiscoveredSettingPolicyScanner(probeConfig.probeStore(), targetConfig.targetStore());
    }

    @Bean
    public StitchingGroupFixer stitchingGroupFixer() {
        return new StitchingGroupFixer();
    }

    @Bean
    public TopologyPipelineFactory topologyPipelineFactory() {
        return new TopologyPipelineFactory(apiConfig.topologyProcessorNotificationSender(),
                groupConfig.policyManager(),
                stitchingConfig.stitchingManager(),
                planConfig.discoveredTemplatesUploader(),
                groupConfig.discoveredGroupUploader(),
                groupConfig.settingsManager(),
                groupConfig.entitySettingsApplicator(),
                topologyEditor(),
                repositoryConfig.repository(),
                groupConfig.topologyFilterFactory(),
                groupConfig.groupServiceClient(),
                reservationConfig.reservationManager(),
                discoveredSettingPolicyScanner(),
                stitchingGroupFixer(),
                entityConfig.entityValidator(),
                supplyChainValidationConfig.supplyChainValidator(),
                groupConfig.discoveredClusterConstraintCache());
    }

    /**
     * Used to identify the topology context of the real-time topology sent to the market component.
     *
     * @return the real-time topology context identifier.
     */
    public long realtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }
}
