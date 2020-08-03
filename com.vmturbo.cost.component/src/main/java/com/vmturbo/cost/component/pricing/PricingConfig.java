package com.vmturbo.cost.component.pricing;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.PricingREST.PricingServiceController;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.identity.PriceTableKeyIdentityStore;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;

@Configuration
@Import({CostDBConfig.class,
        ReservedInstanceSpecConfig.class})
public class PricingConfig {

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public PriceTableMergeFactory priceTableMergeFactory() {
        return PriceTableMerge.newFactory();
    }

    @Bean
    public PriceTableStore priceTableStore() {
        return new SQLPriceTableStore(Clock.systemUTC(),
                databaseConfig.dsl(),
                priceTableKeyIdentityStore(),
                priceTableMergeFactory());
    }

    @Bean
    public BusinessAccountPriceTableKeyStore businessAccountPriceTableKeyStore() {
        return new BusinessAccountPriceTableKeyStore(databaseConfig.dsl(),
                priceTableKeyIdentityStore());
    }

    /**
     * Bean for PriceTableKeyIdentityStore.
     *
     * @return {@link PriceTableKeyIdentityStore }
     */
    @Bean
    public PriceTableKeyIdentityStore priceTableKeyIdentityStore() {
        return new PriceTableKeyIdentityStore(
                databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    /**
     * Get the pricing rpc service.
     *
     * @return the Pricing rpc service.
     */
    @Bean
    public PricingRpcService pricingRpcService() {
        return new PricingRpcService(priceTableStore(), reservedInstanceSpecConfig
                .reservedInstanceSpecStore(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                businessAccountPriceTableKeyStore());
    }

    @Bean
    public PricingServiceController pricingServiceController() {
        return new PricingServiceController(pricingRpcService());
    }
}
