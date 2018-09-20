package com.vmturbo.cost.component.discount;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import jdk.nashorn.internal.ir.annotations.Immutable;

import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.identity.IdentityProvider;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, IdentityProviderConfig.class})
public class DiscountConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public DiscountStore discountStore() {
        return new SQLDiscountStore(databaseConfig.dsl(), identityProviderConfig.identityProvider());
    }
}
