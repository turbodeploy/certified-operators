package com.vmturbo.cost.component.discount;

import java.time.Clock;

import com.vmturbo.common.protobuf.cost.CostREST;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.rpc.ReservedInstanceCostRpcService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.expenses.SqlAccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.rpc.CostRpcService;
import com.vmturbo.cost.component.util.BusinessAccountHelper;

@Configuration
@Import({CostDBConfig.class,
        IdentityProviderConfig.class,
        DiscountConfig.class,
        EntityCostConfig.class,
        ReservedInstanceConfig.class})
public class CostConfig {
    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    @Autowired
    private DiscountConfig discountConfig;

    @Autowired
    private EntityCostConfig entityCostConfig;

    @Autowired
    private ReservedInstanceConfig reservedInstanceConfig;

    @Autowired
    private CostComponentGlobalConfig costComponentGlobalConfig;

    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${maxNumberOfInnerStatRecords:43750}")
    private int maxNumberOfInnerStatRecords;

    @Bean
    public AccountExpensesStore accountExpensesStore() {
        return new SqlAccountExpensesStore(databaseConfig.dsl(),
                Clock.systemUTC(),
                persistEntityCostChunkSize);
    }

    @Bean
    public CostRpcService costRpcService() {
        return new CostRpcService(discountConfig.discountStore(),
                accountExpensesStore(),
                entityCostConfig.entityCostStore(),
                entityCostConfig.projectedEntityCostStore(),
                entityCostConfig.planProjectedEntityCostStore(),
                reservedInstanceConfig.timeFrameCalculator(),
                businessAccountHelper(),
                Clock.systemUTC(),
                realtimeTopologyContextId,
                maxNumberOfInnerStatRecords);
    }

    /**
     * Create a bean for the rpc class ReservedInstanceCostRpcService.
     *
     * @return bean of type ReservedInstanceCostRpcService.
     */
    @Bean
    public ReservedInstanceCostRpcService reservedInstanceCostRpcService() {
        return new ReservedInstanceCostRpcService(reservedInstanceConfig.reservedInstanceBoughtStore(),
                        reservedInstanceConfig.buyReservedInstanceStore(), costComponentGlobalConfig.clock());
    }

    /**
     * Create a bean for the rpc controller ReservedInstanceCostServiceController.
     *
     * @return bean of type ReservedInstanceCostServiceController.
     */
    @Bean
    public CostREST.ReservedInstanceCostServiceController reservedInstanceCostServiceController() {
        return new CostREST.ReservedInstanceCostServiceController(reservedInstanceCostRpcService());
    }

    /**
     * Create a bean of type BusinessAccountHelper.
     *
     * @return bean of type BusinessAccountHelper.
     */
    @Bean
    public BusinessAccountHelper businessAccountHelper() {
        return new BusinessAccountHelper();
    }
}
