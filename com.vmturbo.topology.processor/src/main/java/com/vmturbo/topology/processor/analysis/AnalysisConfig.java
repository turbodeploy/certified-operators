package com.vmturbo.topology.processor.analysis;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.AnalysisDTOREST.AnalysisServiceController;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.entity.EntityConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.plan.PlanConfig;
import com.vmturbo.topology.processor.repository.RepositoryConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the service to trigger analyses.
 */
@Configuration
@Import({
    TopologyConfig.class,
    EntityConfig.class,
    IdentityProviderConfig.class,
    RepositoryConfig.class,
    StitchingConfig.class,
    ClockConfig.class})
public class AnalysisConfig {

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private EntityConfig entityConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private RepositoryConfig repositoryConfig;

    @Autowired
    private PlanConfig planConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private StitchingConfig stichingConfig;

    @Bean
    public AnalysisRpcService analysisService() {
        return new AnalysisRpcService(topologyConfig.planPipelineFactory(),
                topologyConfig.topologyHandler(),
                identityProviderConfig.identityProvider(),
                entityConfig.entityStore(),
                stichingConfig.stitchingJournalFactory(),
                clockConfig.clock());
    }

    @Bean
    public AnalysisServiceController analysisServiceController() {
        return new AnalysisServiceController(analysisService());
    }

}