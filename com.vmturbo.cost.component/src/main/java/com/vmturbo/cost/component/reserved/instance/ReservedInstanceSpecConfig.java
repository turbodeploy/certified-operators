package com.vmturbo.cost.component.reserved.instance;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceSpecServiceController;
import com.vmturbo.cost.component.CostDBConfig;
import com.vmturbo.cost.component.IdentityProviderConfig;

/**
 * Config class for ReservedInstanceSpec.
 */
@Configuration
@Import({IdentityProviderConfig.class,
        CostDBConfig.class})
public class ReservedInstanceSpecConfig {

    @Autowired
    private CostDBConfig databaseConfig;

    @Autowired
    private IdentityProviderConfig identityProviderConfig;

    /**
     * Max size of a batch to insert into 'reserved_instance_spec'.
     */
    @Value("${riSpecStoreInsertBatch:10000}")
    private int riBatchSize;

    /**
     * Returns an instance of ReservedInstanceSpecStore.
     *
     * @return instance of ReservedInstanceSpecStore.
     */
    @Bean
    public ReservedInstanceSpecStore reservedInstanceSpecStore() {
        return new ReservedInstanceSpecStore(databaseConfig.dsl(),
                identityProviderConfig.identityProvider(), riBatchSize);
    }

    /**
     * Returns an instance of ReservedInstanceSpecRpcService.
     *
     * @return instance of ReservedInstanceSpecRpcService.
     */
    @Bean
    public ReservedInstanceSpecRpcService reservedInstanceSpecRpcService() {
        return new ReservedInstanceSpecRpcService(reservedInstanceSpecStore(),
                databaseConfig.dsl());
    }

    /**
     * Returns an instance of ReservedInstanceSpecServiceController.
     *
     * @return instance of ReservedInstanceSpecServiceController.
     */
    @Bean
    public ReservedInstanceSpecServiceController reservedInstanceSpecServiceController() {
        return new ReservedInstanceSpecServiceController(reservedInstanceSpecRpcService());
    }
}
