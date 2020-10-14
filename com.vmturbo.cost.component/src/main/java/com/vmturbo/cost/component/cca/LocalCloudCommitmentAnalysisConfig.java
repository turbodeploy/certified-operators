package com.vmturbo.cost.component.cca;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.commitment.analysis.pricing.CloudCommitmentPricingAnalyzer;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecResolver;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.DiscountApplicator;
import com.vmturbo.cost.calculation.DiscountApplicator.DiscountApplicatorFactory;
import com.vmturbo.cost.calculation.integration.PricingResolver;
import com.vmturbo.cost.calculation.topology.TopologyEntityInfoExtractor;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.discount.DiscountConfig;
import com.vmturbo.cost.component.pricing.PricingConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.topology.LocalCostPricingResolver;

/**
 * Configures "local" instances of stores required for CCA analysis.
 */
@Configuration
@Import({DiscountConfig.class,
        IdentityProviderConfig.class, ReservedInstanceSpecConfig.class,
        })
public class LocalCloudCommitmentAnalysisConfig {

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private PricingConfig pricingConfig;

    @Autowired
    private DiscountConfig discountConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;


    /**
     * Bean for RI implementation of cloud commitment spec resolver.
     *
     * @return An instance of {@link LocalReservedInstanceSpecResolver}
     */
    @Bean
    public CloudCommitmentSpecResolver<ReservedInstanceSpec> reservedInstanceSpecResolver() {
        return new LocalReservedInstanceSpecResolver(reservedInstanceSpecConfig.reservedInstanceSpecStore());
    }

    /**
     * Bean for implementation of Cloud Commitment Bought Resolver.
     *
     * @return An instance of the Cloud Commitment Bought Resolver.
     */
    @Bean
    public CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver() {
        return new LocalCloudCommitmentBoughtResolver(reservedInstanceConfig.reservedInstanceBoughtStore(), reservedInstanceSpecConfig.reservedInstanceSpecStore());
    }


    /**
     * Bean for creating the CCA Pricing Analyzer.
     *
     * @return The CCA pricing analyzer/
     */
    @Bean
    public CloudCommitmentPricingAnalyzer cloudCommitmentPricingAnalyzer() {
        return new LocalCloudCommitmentPricingAnalyzer(localCostPricingResolver(),
                pricingConfig.businessAccountPriceTableKeyStore(), pricingConfig.priceTableStore(), topologyEntityInfoExtractor());
    }

    /**
     * Bean for creating an instance of the discount applicator factory.
     *
     * @return The discount applicator factory.
     */
    @Bean
    public DiscountApplicatorFactory<TopologyEntityDTO> discountApplicatorFactory() {
        return DiscountApplicator.newFactory();
    }

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
     * Creates the pricing resolver.
     *
     * @return The pricing resolver.
     */
    public PricingResolver localCostPricingResolver() {
        return new LocalCostPricingResolver(pricingConfig.priceTableStore(), pricingConfig.businessAccountPriceTableKeyStore(), identityProviderConfig.identityProvider(),
                discountConfig.discountStore(), discountApplicatorFactory(), topologyEntityInfoExtractor());
    }
}

