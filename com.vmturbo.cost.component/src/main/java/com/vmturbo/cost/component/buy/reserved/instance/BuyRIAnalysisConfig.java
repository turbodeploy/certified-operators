package com.vmturbo.cost.component.buy.reserved.instance;

import java.util.concurrent.Executors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.BuyRIAnalysisServiceController;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsConfig;

@Configuration
@Import(ComputeTierDemandStatsConfig.class)
public class BuyRIAnalysisConfig {

    @Value("${initialBuyRIAnalysisIntervalHours}")
    private long initialBuyRIAnalysisIntervalHours;

    @Value("${normalBuyRIAnalysisIntervalHours}")
    private long normalBuyRIAnalysisIntervalHours;

    @Autowired
    private ComputeTierDemandStatsConfig computeTierDemandStatsConfig;

    @Bean
    public BuyRIAnalysisScheduler buyReservedInstanceScheduler() {
        return new BuyRIAnalysisScheduler(Executors.newSingleThreadScheduledExecutor(),
                computeTierDemandStatsConfig.riDemandStatsStore(),
                initialBuyRIAnalysisIntervalHours, normalBuyRIAnalysisIntervalHours);
    }

    @Bean
    public BuyRIAnalysisRpcService buyReservedInstanceScheduleRpcService() {
        return new BuyRIAnalysisRpcService(buyReservedInstanceScheduler());
    }

    @Bean
    public BuyRIAnalysisServiceController buyReservedInstanceScheduleServiceController() {
        return new BuyRIAnalysisServiceController(buyReservedInstanceScheduleRpcService());
    }
}
