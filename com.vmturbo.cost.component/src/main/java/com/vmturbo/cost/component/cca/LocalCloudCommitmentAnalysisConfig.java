package com.vmturbo.cost.component.cca;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentBoughtResolver;
import com.vmturbo.cloud.commitment.analysis.spec.CloudCommitmentSpecResolver;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;

/**
 * Configures "local" instances of stores required for CCA analysis.
 */
@Configuration
public class LocalCloudCommitmentAnalysisConfig {

    @Autowired
    private ReservedInstanceSpecStore reservedInstanceSpecStore;

    @Autowired
    private ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    /**
     * Bean for RI implementation of cloud commitment spec resolver.
     *
     * @return An instance of {@link LocalReservedInstanceSpecResolver}
     */
    @Bean
    public CloudCommitmentSpecResolver<ReservedInstanceSpec> reservedInstanceSpecResolver() {
        return new LocalReservedInstanceSpecResolver(reservedInstanceSpecStore);
    }

    /**
     * Bean for implementation of Cloud Commitment Bought Resolver.
     *
     * @return An instance of the Cloud Commitment Bought Resolver.
     */
    @Bean
    public CloudCommitmentBoughtResolver cloudCommitmentBoughtResolver() {
        return new LocalCloudCommitmentBoughtResolver(reservedInstanceBoughtStore, reservedInstanceSpecStore);
    }
}
