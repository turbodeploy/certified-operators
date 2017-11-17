package com.vmturbo.topology.processor.topology;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.TopologyDTOREST;
import com.vmturbo.topology.processor.api.server.TopologyProcessorApiConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.group.GroupConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.plan.PlanConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
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
    IdentityProviderConfig.class,
    GroupConfig.class,
    StitchingConfig.class,
    PlanConfig.class,
    RepositoryConfig.class,
    TemplateConfig.class
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

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Bean
    public TopologyHandler topologyHandler() {
        return new TopologyHandler(realtimeTopologyContextId(),
                topologyPipelineFactory(),
                identityProviderConfig.identityProvider(),
                entityConfig.entityStore(),
                Clock.systemUTC());
    }

    @Bean
    public TopologyEditor topologyEditor() {
        return new TopologyEditor(identityProviderConfig.identityProvider(),
                templateConfig.templateConverterFactory());
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
                groupConfig.topologyFilterFactory());
    }

    @Bean
    public TopologyRpcService topologyRpcService() {
        return new TopologyRpcService(topologyHandler());
    }

    @Bean
    public TopologyDTOREST.TopologyServiceController topologyServiceController() {
        return new TopologyDTOREST.TopologyServiceController(topologyRpcService());
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
