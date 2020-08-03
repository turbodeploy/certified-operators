package com.vmturbo.cost.component.discount;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;

@Configuration
@Import({CostDBConfig.class, IdentityProviderConfig.class})
public class DiscountConfig {

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public DiscountStore discountStore() {
        return new SQLDiscountStore(databaseConfig.dsl(), identityProviderConfig.identityProvider());
    }
}
