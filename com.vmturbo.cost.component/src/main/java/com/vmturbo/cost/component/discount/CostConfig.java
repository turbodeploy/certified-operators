package com.vmturbo.cost.component.discount;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.rpc.CostRpcService;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, IdentityProviderConfig.class})
public class CostConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public DiscountStore discountStore() {
        return new SQLDiscountStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public CostRpcService costRpcService() {
        return new CostRpcService(discountStore());
    }
}
