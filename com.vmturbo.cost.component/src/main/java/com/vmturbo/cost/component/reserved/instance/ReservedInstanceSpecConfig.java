package com.vmturbo.cost.component.reserved.instance;

import java.sql.SQLException;

import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.common.protobuf.cost.CostREST.ReservedInstanceSpecServiceController;
import com.vmturbo.cost.component.IdentityProviderConfig;
import com.vmturbo.cost.component.db.DbAccessConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Config class for ReservedInstanceSpec.
 */
@Configuration
@Import({IdentityProviderConfig.class,
        DbAccessConfig.class})
public class ReservedInstanceSpecConfig {

    @Autowired
    private DbAccessConfig dbAccessConfig;

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
        try {
            return new ReservedInstanceSpecStore(dbAccessConfig.dsl(), identityProviderConfig.identityProvider(), riBatchSize);
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ReservedInstanceSpecStore bean", e);
        }
    }

    /**
     * Returns an instance of ReservedInstanceSpecRpcService.
     *
     * @return instance of ReservedInstanceSpecRpcService.
     */
    @Bean
    public ReservedInstanceSpecRpcService reservedInstanceSpecRpcService() {
        try {
            return new ReservedInstanceSpecRpcService(reservedInstanceSpecStore(), dbAccessConfig.dsl());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create ReservedInstanceSpecRpcService", e);
        }
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
