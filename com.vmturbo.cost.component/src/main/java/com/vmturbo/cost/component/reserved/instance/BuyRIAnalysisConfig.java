package com.vmturbo.cost.component.reserved.instance;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.BuyRIAnalysisServiceController;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({ComputeTierDemandStatsConfig.class,
        ReservedInstanceAnalysisConfig.class})
public class BuyRIAnalysisConfig {

    @Value("${initialBuyRIAnalysisIntervalHours}")
    private long initialBuyRIAnalysisIntervalHours;

    @Value("${normalBuyRIAnalysisIntervalHours}")
    private long normalBuyRIAnalysisIntervalHours;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private ReservedInstanceAnalysisConfig reservedInstanceAnalysisConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Bean
    public BuyRIAnalysisScheduler buyReservedInstanceScheduler() {
        return new BuyRIAnalysisScheduler(Executors.newSingleThreadScheduledExecutor(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                initialBuyRIAnalysisIntervalHours, normalBuyRIAnalysisIntervalHours);
    }

    @Bean
    public BuyRIAnalysisRpcService buyReservedInstanceScheduleRpcService() {
        return new BuyRIAnalysisRpcService(buyReservedInstanceScheduler(),
                reservedInstanceAnalysisConfig.reservedInstanceAnalyzer(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                realtimeTopologyContextId);
    }

    @Bean
    public BuyRIAnalysisServiceController buyReservedInstanceScheduleServiceController() {
        return new BuyRIAnalysisServiceController(buyReservedInstanceScheduleRpcService());
    }

    @Bean
    public BuyReservedInstanceStore buyReservedInstanceStore() {
        return new BuyReservedInstanceStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider());
    }

    @Bean
    public BuyReservedInstanceRpcService buyReservedInstanceRpcService() {
        return new BuyReservedInstanceRpcService(buyReservedInstanceStore());
    }
}