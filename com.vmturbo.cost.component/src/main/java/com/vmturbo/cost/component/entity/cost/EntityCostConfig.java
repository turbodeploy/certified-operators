package com.vmturbo.cost.component.entity.cost;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.cost.component.notification.CostNotificationConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;

@Configuration
@Import({MarketClientConfig.class,
        MarketListenerConfig.class,
        CostDBConfig.class,
        CostNotificationConfig.class})
public class EntityCostConfig {

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private CostNotificationConfig costNotificationConfig;

    @Value("${persistEntityCostChunkSize}")
    private int persistEntityCostChunkSize;

    @Autowired
    private MarketComponent marketComponent;

    @Bean
    public EntityCostStore entityCostStore() {
        return new SqlEntityCostStore(databaseConfig.dsl(), Clock.systemUTC(), persistEntityCostChunkSize);
    }

    @Bean
    public ProjectedEntityCostStore projectedEntityCostStore() {
        return new ProjectedEntityCostStore();
    }

    @Bean
    public PlanProjectedEntityCostStore planProjectedEntityCostStore() {
        return new PlanProjectedEntityCostStore(databaseConfig.dsl(), persistEntityCostChunkSize);
    }

    @Bean
    public CostComponentProjectedEntityCostListener projectedEntityCostListener() {
        final CostComponentProjectedEntityCostListener projectedEntityCostListener =
                new CostComponentProjectedEntityCostListener(projectedEntityCostStore(),
                        planProjectedEntityCostStore(),
                        costNotificationConfig.costNotificationSender());
        marketComponent.addProjectedEntityCostsListener(projectedEntityCostListener);
        return projectedEntityCostListener;
    }
}
