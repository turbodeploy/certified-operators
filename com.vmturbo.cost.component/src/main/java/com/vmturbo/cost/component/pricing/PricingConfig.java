package com.vmturbo.cost.component.pricing;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.common.protobuf.cost.PricingREST.PricingServiceController;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;

@Configuration
public class PricingConfig {

    @Bean
    public PriceTableMergeFactory priceTableMergeFactory() {
        return PriceTableMerge.newFactory();
    }

    @Bean
    public PriceTableStore priceTableStore() {
        return new InMemoryPriceTableStore(priceTableMergeFactory());
    }

    @Bean
    public PricingRpcService pricingRpcService() {
        return new PricingRpcService(priceTableStore());
    }

    @Bean
    public PricingServiceController pricingServiceController() {
        return new PricingServiceController(pricingRpcService());
    }
}
