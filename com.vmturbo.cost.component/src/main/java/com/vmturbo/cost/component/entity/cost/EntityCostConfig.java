package com.vmturbo.cost.component.entity.cost;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.google.common.collect.Sets;

import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketClientConfig.Subscription;
import com.vmturbo.market.component.api.impl.MarketComponentNotificationReceiver;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({MarketClientConfig.class,
        SQLDatabaseConfig.class})
public class EntityCostConfig {

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Value("${persistEntityCostChunkSize}")
    private int persistEntityCostChunkSize;

    @Bean
    public EntityCostStore entityCostStore() {
        return new SQLEntityCostStore(databaseConfig.dsl(), Clock.systemUTC(), persistEntityCostChunkSize);
    }

    @Bean
    public ProjectedEntityCostStore projectedEntityCostStore() {
        return new ProjectedEntityCostStore();
    }

    @Bean
    public CostComponentProjectedEntityCostListener projectedEntityCostListener() {
        final CostComponentProjectedEntityCostListener projectedEntityCostListener =
                new CostComponentProjectedEntityCostListener(projectedEntityCostStore());
        marketComponent().addProjectedEntityCostsListener(projectedEntityCostListener);
        return projectedEntityCostListener;
    }

    @Bean
    public MarketComponent marketComponent() {
        return marketClientConfig.marketComponent(Sets.newHashSet(Subscription.ProjectedEntityCosts));
    }
}
