package com.vmturbo.topology.processor.cost;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.PricingServiceGrpc.PricingServiceStub;
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

    @Value("${minimumAccountExpensesUploadIntervalMins:60}")
    private int minimumAccountExpensesUploadIntervalMins;

    @Value("${minimumRIDataUploadIntervalMins:5}")
    private int minimumRIDataUploadIntervalMins;

    @Value("${riSpecPriceChunkSize:10000}")
    private int riSpecPriceChunkSize;

    @Value("${fullAzureEARIDiscovery:false}")
    private boolean fullAzureEARIDiscovery;

    @Value("${riSupportInPartialCloudEnvironment:true}")
    private boolean riSupportInPartialCloudEnvironment;

    @Bean
    public RIAndExpenseUploadServiceBlockingStub costServiceClient() {
        return RIAndExpenseUploadServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public PricingServiceStub priceServiceClient() {
        return PricingServiceGrpc.newStub(costClientConfig.costChannel());
    }

    @Bean
    public PriceTableUploader priceTableUploader() {
        return new PriceTableUploader(priceServiceClient(), clockConfig.clock(), riSpecPriceChunkSize,
                targetConfig.targetStore(), spotPriceTableConverter());
    }

    @Bean
    public RICostDataUploader riDataUploader() {
        return new RICostDataUploader(costServiceClient(), minimumRIDataUploadIntervalMins,
                clockConfig.clock(), fullAzureEARIDiscovery, riSupportInPartialCloudEnvironment);
    }

    @Bean
    public CloudCommitmentCostUploader cloudCommitmentCostUploader() {
        return new CloudCommitmentCostUploader(CloudCommitmentUploadServiceGrpc.newBlockingStub(costClientConfig.costChannel()));
    }

    @Bean
    public AccountExpensesUploader accountExpensesUploader() {
        return new AccountExpensesUploader(costServiceClient(), minimumAccountExpensesUploadIntervalMins,
                clockConfig.clock());
    }

    @Bean
    public DiscoveredCloudCostUploader discoveredCloudCostUploader() {
        return new DiscoveredCloudCostUploader(riDataUploader(),  cloudCommitmentCostUploader(),
            accountExpensesUploader(), priceTableUploader(), businessAccountPriceTableKeyUploader());
    }

    @Bean
    public BusinessAccountPriceTableKeyUploader businessAccountPriceTableKeyUploader() {
        PricingServiceBlockingStub pricingServiceBlockingStub = PricingServiceGrpc
                .newBlockingStub(costClientConfig.costChannel());
        return new BusinessAccountPriceTableKeyUploader(pricingServiceBlockingStub,
                targetConfig.targetStore());
    }

    @Bean
    public SpotPriceTableConverter spotPriceTableConverter() {
        return new SpotPriceTableConverter();
    }
}
