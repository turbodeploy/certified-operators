package com.vmturbo.market.reserved.instance.analysis;

import javax.annotation.PostConstruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.base.MoreObjects;

import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory.DefaultBuyRIImpactAnalysisFactory;
import com.vmturbo.market.topology.TopologyProcessorConfig;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory.DefaultRICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

@Configuration
@Import({TopologyProcessorConfig.class})
public class BuyRIImpactAnalysisConfig {

    private final Logger logger = LogManager.getLogger();

    @Autowired
    private TopologyProcessorConfig topologyProcessorConfig;

    @Value("${buyRIImpactAnalysisValidation:false}")
    private boolean buyRIImpactAnalysisValidation;

    @Value("${concurrentBuyRIImpactAnalysis:true}")
    private boolean concurrentBuyRIImpactAnalysis;

    @Bean
    public CoverageTopologyFactory coverageTopologyFactory() {
        return new CoverageTopologyFactory(topologyProcessorConfig.thinTargetCache());
    }

    @Bean
    public RICoverageAllocatorFactory riCoverageAllocatorFactory() {
        return new DefaultRICoverageAllocatorFactory();
    }

    @Bean
    public BuyRIImpactAnalysisFactory buyRIImpactAnalysisFactory() {
        return new DefaultBuyRIImpactAnalysisFactory(
                riCoverageAllocatorFactory(),
                coverageTopologyFactory(),
                concurrentBuyRIImpactAnalysis,
                buyRIImpactAnalysisValidation);
    }

    @PostConstruct
    public void logConfiguration() {
        logger.info(MoreObjects.toStringHelper(this)
                .add("buyRIImpactAnalysisValidation", buyRIImpactAnalysisValidation)
                .add("concurrentBuyRIImpactAnalysis", concurrentBuyRIImpactAnalysis)
                .toString());
    }
}
