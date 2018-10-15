package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.rpc.RIAndExpenseUploadRpcService;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({
        SQLDatabaseConfig.class,
        ReservedInstanceConfig.class,
        CostConfig.class
})
public class CostServiceConfig {
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private CostConfig costConfig;

    @Bean
    public RIAndExpenseUploadRpcService riAndExpenseUploadRpcService() {
        return new RIAndExpenseUploadRpcService(databaseConfig.dsl(),
                costConfig.accountExpensesStore(),
                reservedInstanceConfig.reservedInstanceSpecStore(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                reservedInstanceConfig.reservedInstanceCoverageUpload());
    }
}
