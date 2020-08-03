package com.vmturbo.topology.processor.analysis;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.topology.AnalysisDTOREST.AnalysisServiceController;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.identity.IdentityProviderConfig;
import com.vmturbo.topology.processor.stitching.StitchingConfig;
import com.vmturbo.topology.processor.topology.TopologyConfig;

/**
 * Configuration for the service to trigger analyses.
 */
@Configuration
@Import({
    TopologyConfig.class,
    IdentityProviderConfig.class,
    StitchingConfig.class,
    ClockConfig.class})
public class AnalysisConfig {

    @Autowired
    private TopologyConfig topologyConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Autowired
    private StitchingConfig stichingConfig;

    // Used to determine whether the plan Bought RIs (newly recommended RI purchases) should be fed to the market,
    // and the market should perform analysis on them, or if the Buy RI Impact Analysis should be performed.
    // BUY_RI_IMPACT_ANALYSIS is evaluated to true if either it's OCP with RI Buy only, or it's OCP with RI Buy + Market Optimization,
    // and allowBoughtRiInAnalysis is false.
    @Value("${allowBoughtRiInAnalysis:true}")
    private boolean allowBoughtRiInAnalysis;

    /**
     * AnalysisRpcService Bean.
     *
     * @return AnalysisRpcService.
     */
    @Bean
    public AnalysisRpcService analysisService() {
        return new AnalysisRpcService(topologyConfig.pipelineExecutorService(),
                topologyConfig.topologyHandler(),
                identityProviderConfig.identityProvider(),
                stichingConfig.stitchingJournalFactory(),
                clockConfig.clock(),
                allowBoughtRiInAnalysis);
    }

    @Bean
    public AnalysisServiceController analysisServiceController() {
        return new AnalysisServiceController(analysisService());
    }

}