package com.vmturbo.cost.component.discount;

import java.sql.SQLException;
import java.time.Clock;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST;
import com.vmturbo.cost.component.BilledCostConfig;
import com.vmturbo.cost.component.CostComponentGlobalConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.entity.cost.EntityCostConfig;
import com.vmturbo.cost.component.expenses.AccountExpensesStore;
import com.vmturbo.cost.component.expenses.SqlAccountExpensesStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.rpc.CostRpcService;
import com.vmturbo.cost.component.rpc.ReservedInstanceCostRpcService;
import com.vmturbo.cost.component.savings.EntitySavingsConfig;
import com.vmturbo.cost.component.util.BusinessAccountHelper;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

@Configuration
@Import({DbAccessConfig.class,
        IdentityProviderConfig.class,
        DiscountConfig.class,
        EntityCostConfig.class,
        ReservedInstanceConfig.class,
        EntitySavingsConfig.class,
        BilledCostConfig.class})
public class CostConfig {
    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    @Autowired
    private EntitySavingsConfig entitySavingsConfig;

    @Autowired
    private BilledCostConfig billedCostConfig;

    @Value("${persistEntityCostChunkSize:1000}")
    private int persistEntityCostChunkSize;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${maxNumberOfInnerStatRecords:43750}")
    private int maxNumberOfInnerStatRecords;

    @Bean
    public AccountExpensesStore accountExpensesStore() {
        try {
            return new SqlAccountExpensesStore(dbAccessConfig.dsl(), Clock.systemUTC(),
                    persistEntityCostChunkSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create AccountExpensesStore bean", e);
        }
    }

    @Bean
    public CostRpcService costRpcService() {
        return new CostRpcService(discountConfig.discountStore(),
                accountExpensesStore(),
                entityCostConfig.entityCostStore(),
                billedCostConfig.billedCostStore(),
                entityCostConfig.projectedEntityCostStore(),
                entityCostConfig.planProjectedEntityCostStore(),
                reservedInstanceConfig.timeFrameCalculator(),
                businessAccountHelper(),
                Clock.systemUTC(),
                realtimeTopologyContextId,
                maxNumberOfInnerStatRecords,
                entitySavingsConfig.entitySavingsStore());
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
