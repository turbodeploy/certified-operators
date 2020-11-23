package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecConfig;
import com.vmturbo.cost.component.rpc.RIAndExpenseUploadRpcService;

@Configuration
@Import({CostConfig.class,
    ReservedInstanceConfig.class,
    CostDBConfig.class,
    ReservedInstanceSpecConfig.class
})
public class CostServiceConfig {
    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private CostConfig costConfig;

    @Autowired
    private ReservedInstanceSpecConfig reservedInstanceSpecConfig;

    @Value("${riSupportInPartialCloudEnvironment:true}")
    private boolean riSupportInPartialCloudEnvironment;

    @Bean
    public RIAndExpenseUploadRpcService riAndExpenseUploadRpcService() {
        return new RIAndExpenseUploadRpcService(databaseConfig.dsl(),
                costConfig.accountExpensesStore(),
                reservedInstanceSpecConfig.reservedInstanceSpecStore(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                reservedInstanceConfig.reservedInstanceCoverageUpload(),
                riSupportInPartialCloudEnvironment);
    }
}
