package com.vmturbo.cost.component.discount;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.expenses.SqlAccountExpensesStore;
import com.vmturbo.cost.component.rpc.CostRpcService;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({SQLDatabaseConfig.class, IdentityProviderConfig.class})
public class CostConfig {

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Value("${persistEntityCostChunkSize}")
    private int persistEntityCostChunkSize;


    @Bean
    public DiscountStore discountStore() {
        return new SQLDiscountStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public AccountExpensesStore accountExpensesStore() {
        return new SqlAccountExpensesStore(databaseConfig.dsl(),
                Clock.systemUTC(),
                persistEntityCostChunkSize);
    }

    @Bean
    public CostRpcService costRpcService() {
        return new CostRpcService(discountStore(), accountExpensesStore());
    }
}
