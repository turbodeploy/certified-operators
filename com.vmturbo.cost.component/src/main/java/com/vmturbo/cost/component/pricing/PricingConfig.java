package com.vmturbo.cost.component.pricing;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.PricingREST.PricingServiceController;
import com.vmturbo.cost.component.pricing.PriceTableMerge.PriceTableMergeFactory;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class})
public class PricingConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Bean
    public PriceTableMergeFactory priceTableMergeFactory() {
        return PriceTableMerge.newFactory();
    }

    @Bean
    public PriceTableStore priceTableStore() {
        return new SQLPriceTableStore(Clock.systemUTC(),
                databaseConfig.dsl(),
                priceTableMergeFactory());
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
