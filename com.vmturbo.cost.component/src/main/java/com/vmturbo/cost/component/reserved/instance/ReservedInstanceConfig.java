package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceBoughtServiceController;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceSpecServiceController;
import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceUtilizationCoverageServiceController;
import com.vmturbo.cost.component.CostServiceConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.MarketListenerConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({IdentityProviderConfig.class,
        MarketClientConfig.class,
        MarketListenerConfig.class,
        SQLDatabaseConfig.class})
public class ReservedInstanceConfig {

    @Value("${numRetainedMinutes}")
    private int numRetainedMinutes;

    @Value("${numRetainedHours}")
    private int numRetainedHours;

    @Value("${numRetainedDays}")
    private int numRetainedDays;

    @Value("${riCoverageCacheExpireMinutes:120}")
    private int riCoverageCacheExpireMinutes;

    @Autowired
    private MarketComponent marketComponent;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public ReservedInstanceBoughtStore reservedInstanceBoughtStore() {
        return new ReservedInstanceBoughtStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public ReservedInstanceSpecStore reservedInstanceSpecStore() {
        return new ReservedInstanceSpecStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public EntityReservedInstanceMappingStore entityReservedInstanceMappingStore() {
        return new EntityReservedInstanceMappingStore(databaseConfig.dsl(),
                reservedInstanceBoughtStore());
    }

    @Bean
    public ReservedInstanceUtilizationStore reservedInstanceUtilizationStore() {
        return new ReservedInstanceUtilizationStore(databaseConfig.dsl(),
                reservedInstanceBoughtStore(), reservedInstanceSpecStore(),
                entityReservedInstanceMappingStore());
    }

    @Bean
    public ReservedInstanceCoverageStore reservedInstanceCoverageStore() {
        return new ReservedInstanceCoverageStore(databaseConfig.dsl());
    }

    @Bean
    public ReservedInstanceBoughtRpcService reservedInstanceBoughtRpcService() {
        return new ReservedInstanceBoughtRpcService(reservedInstanceBoughtStore(),
                entityReservedInstanceMappingStore());
    }

    @Bean
    public ReservedInstanceSpecRpcService reservedInstanceSpecRpcService() {
        return new ReservedInstanceSpecRpcService(reservedInstanceSpecStore(), databaseConfig.dsl());
    }

    @Bean
    public ProjectedRICoverageStore projectedEntityRICoverageStore() {
        return new ProjectedRICoverageStore();
    }

    @Bean
    public TimeFrameCalculator timeFrameCalculator() {
        return new TimeFrameCalculator(Clock.systemUTC(), numRetainedMinutes, numRetainedHours, numRetainedDays);
    }

    @Bean
    public ReservedInstanceUtilizationCoverageRpcService reservedInstanceUtilizationCoverageRpcService() {
        return new ReservedInstanceUtilizationCoverageRpcService(reservedInstanceUtilizationStore(),
                reservedInstanceCoverageStore(), projectedEntityRICoverageStore(), timeFrameCalculator());
    }

    @Bean
    public ReservedInstanceBoughtServiceController reservedInstanceBoughtServiceController() {
        return new ReservedInstanceBoughtServiceController(reservedInstanceBoughtRpcService());
    }

    @Bean
    public ReservedInstanceSpecServiceController reservedInstanceSpecServiceController() {
        return new ReservedInstanceSpecServiceController(reservedInstanceSpecRpcService());
    }

    @Bean
    public ReservedInstanceUtilizationCoverageServiceController reservedInstanceUtilizationCoverageServiceController() {
        return new ReservedInstanceUtilizationCoverageServiceController(
                reservedInstanceUtilizationCoverageRpcService());
    }

    @Bean
    public ReservedInstanceCoverageUpdate reservedInstanceCoverageUpload() {
        return new ReservedInstanceCoverageUpdate(databaseConfig.dsl(), entityReservedInstanceMappingStore(),
                reservedInstanceUtilizationStore(), reservedInstanceCoverageStore(),
                riCoverageCacheExpireMinutes);
    }


    @Bean
    public ProjectedRICoverageListener projectedRICoverageListener() {
        final ProjectedRICoverageListener projectedRICoverageListener =
                new ProjectedRICoverageListener(projectedEntityRICoverageStore());
        marketComponent.addProjectedEntityRiCoverageListener(projectedRICoverageListener);
        return projectedRICoverageListener;
    }
}
