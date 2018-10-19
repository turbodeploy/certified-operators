package com.vmturbo.cost.component.reserved.instance;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.BuyRIAnalysisServiceController;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisConfig;

@Configuration
@Import({ComputeTierDemandStatsConfig.class,
        ReservedInstanceAnalysisConfig.class})
public class BuyRIAnalysisConfig {

    @Value("${initialBuyRIAnalysisIntervalHours}")
    private long initialBuyRIAnalysisIntervalHours;

    @Value("${normalBuyRIAnalysisIntervalHours}")
    private long normalBuyRIAnalysisIntervalHours;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Autowired
    private ReservedInstanceAnalysisConfig reservedInstanceAnalysisConfig;

    @Bean
    public BuyRIAnalysisScheduler buyReservedInstanceScheduler() {
        return new BuyRIAnalysisScheduler(Executors.newSingleThreadScheduledExecutor(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                initialBuyRIAnalysisIntervalHours, normalBuyRIAnalysisIntervalHours);
    }

    @Bean
    public BuyRIAnalysisRpcService buyReservedInstanceScheduleRpcService() {
        return new BuyRIAnalysisRpcService(buyReservedInstanceScheduler(),
                reservedInstanceAnalysisConfig.reservedInstanceAnalyzer());
    }

    @Bean
    public BuyRIAnalysisServiceController buyReservedInstanceScheduleServiceController() {
        return new BuyRIAnalysisServiceController(buyReservedInstanceScheduleRpcService());
    }
}
