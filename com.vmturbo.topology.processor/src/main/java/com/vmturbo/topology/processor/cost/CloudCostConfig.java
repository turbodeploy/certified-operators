package com.vmturbo.topology.processor.cost;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.RIAndExpenseUploadServiceGrpc.RIAndExpenseUploadServiceStub;
import com.vmturbo.cost.api.CostClientConfig;

/**
 * Configuration for a Cost Component client.
 */
@Configuration
@Import({CostClientConfig.class})
public class CloudCostConfig {

    @Autowired
    CostClientConfig costClientConfig;

    @Bean
    public RIAndExpenseUploadServiceBlockingStub costServiceClient() {
        return RIAndExpenseUploadServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public DiscoveredCloudCostUploader discoveredCloudCostUploader() {
        return new DiscoveredCloudCostUploader(costServiceClient());
    }
}
