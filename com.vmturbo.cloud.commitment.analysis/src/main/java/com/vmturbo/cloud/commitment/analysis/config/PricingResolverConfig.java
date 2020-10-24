package com.vmturbo.cloud.commitment.analysis.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingAnalyzer.CloudCommitmentPricingAnalyzerFactory;
import com.vmturbo.cloud.commitment.analysis.pricing.DefaultPricingAnalyzer.DefaultPricingAnalyzerFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.pricing.PricingResolverStage.PricingResolverStageFactory;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.PricingResolver;
import com.vmturbo.cost.calculation.pricing.CloudRateExtractor.CloudRateExtractorFactory;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;

/**
 * Spring configuration for creating a {@link PricingResolverStageFactory} instance.
 */
@Lazy
@Configuration
public class PricingResolverConfig {

    // The component-specific resolver should be setup in a component config.
    @Autowired
    PricingResolver<TopologyEntityDTO> pricingResolver;

    /**
     * Bean for getting the topology info extractor.
     *
     * @return The topology info extractor.
     */
    @Bean
    public TopologyEntityInfoExtractor topologyEntityInfoExtractor() {
        return new TopologyEntityInfoExtractor();
    }

    /**
     * Bean for getting the cloud rate extractor factory.
     *
     * @return The cloud rate extractor factory.
     */
    @Bean
    public CloudRateExtractorFactory cloudRateExtractorFactory() {
        return new CloudRateExtractorFactory();
    }

    /**
     * Bean for creating a {@link CloudCommitmentPricingAnalyzerFactory} instance.
     * @return The {@link CloudCommitmentPricingAnalyzerFactory} instance.
     */
    @Bean
    public CloudCommitmentPricingAnalyzerFactory cloudCommitmentPricingAnalyzerFactory() {
        return new DefaultPricingAnalyzerFactory(
                pricingResolver,
                topologyEntityInfoExtractor(),
                cloudRateExtractorFactory());
    }

    /**
     * Creates an instance of the PricingResolverStageFactory.
     *
     * @return The Pricing resolver stage factory.
     */
    @Bean
    public PricingResolverStageFactory pricingResolverStageFactory() {
        return new PricingResolverStageFactory(cloudCommitmentPricingAnalyzerFactory());
    }
}
