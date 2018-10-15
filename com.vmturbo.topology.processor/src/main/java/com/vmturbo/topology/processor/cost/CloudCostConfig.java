package com.vmturbo.topology.processor.cost;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.topology.processor.ClockConfig;
import com.vmturbo.topology.processor.targets.TargetConfig;

/**
 * Configuration for a Cost Component client.
 */
@Configuration
@Import({CostClientConfig.class, TargetConfig.class, ClockConfig.class})
public class CloudCostConfig {

    @Autowired
    private CostClientConfig costClientConfig;

    @Autowired
    private TargetConfig targetConfig;

    @Autowired
    private ClockConfig clockConfig;

    @Value("${minimumAccountExpensesUploadIntervalMins}")
    private int minimumAccountExpensesUploadIntervalMins;

    @Value("${minimumRIDataUploadIntervalMins}")
    private int minimumRIDataUploadIntervalMins;

    @Bean
    public RIAndExpenseUploadServiceBlockingStub costServiceClient() {
        return RIAndExpenseUploadServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public PricingServiceBlockingStub priceServiceClient() {
        return PricingServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public PriceTableUploader priceTableUploader() {
        return new PriceTableUploader(priceServiceClient(), clockConfig.clock());
    }

    @Bean
    public RICostDataUploader riDataUploader() {
        return new RICostDataUploader(costServiceClient(), minimumRIDataUploadIntervalMins,
                clockConfig.clock());
    }

    @Bean
    public AccountExpensesUploader accountExpensesUploader() {
        return new AccountExpensesUploader(costServiceClient(), minimumAccountExpensesUploadIntervalMins,
                clockConfig.clock());
    }

    @Bean
    public DiscoveredCloudCostUploader discoveredCloudCostUploader() {
        return new DiscoveredCloudCostUploader(riDataUploader(), accountExpensesUploader(), priceTableUploader(),
                targetConfig.targetStore());
    }
}
