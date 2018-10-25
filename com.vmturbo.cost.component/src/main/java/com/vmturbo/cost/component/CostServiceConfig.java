package com.vmturbo.cost.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostDebugREST.CostDebugServiceController;
import com.vmturbo.cost.component.discount.CostConfig;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.rpc.CostDebugRpcService;
import com.vmturbo.cost.component.rpc.RIAndExpenseUploadRpcService;
import com.vmturbo.cost.component.topology.TopologyListenerConfig;
import com.vmturbo.sql.utils.SQLDatabaseConfig;

@Configuration
@Import({
        SQLDatabaseConfig.class,
        ReservedInstanceConfig.class,
        CostConfig.class,
        TopologyListenerConfig.class
})
public class CostServiceConfig {
    @Autowired
    private SQLDatabaseConfig databaseConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private CostConfig costConfig;

    @Autowired
    private TopologyListenerConfig topologyListenerConfig;

    @Bean
    public RIAndExpenseUploadRpcService riAndExpenseUploadRpcService() {
        return new RIAndExpenseUploadRpcService(databaseConfig.dsl(),
                costConfig.accountExpensesStore(),
                reservedInstanceConfig.reservedInstanceSpecStore(),
                reservedInstanceConfig.reservedInstanceBoughtStore(),
                reservedInstanceConfig.reservedInstanceCoverageUpload());
    }

    @Bean
    public CostDebugRpcService costDebugRpcService() {
        return new CostDebugRpcService(topologyListenerConfig.costJournalRecorder());
    }

    @Bean
    public CostDebugServiceController costDebugServiceController() {
        return new CostDebugServiceController(costDebugRpcService());
    }
}
