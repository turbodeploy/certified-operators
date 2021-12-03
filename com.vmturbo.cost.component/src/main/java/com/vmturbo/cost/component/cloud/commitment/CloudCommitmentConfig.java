package com.vmturbo.cost.component.cloud.commitment;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentServiceController;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;

/**
 * A configuration file for cloud commitment services.
 */
@Configuration
@Import({CloudCommitmentStatsConfig.class})
public class CloudCommitmentConfig {
    /**
     * A bean for {@link CloudCommitmentRpcService}.
     *
     * @param sourceTopologyCommitmentMappingStore The source topology commitment mapping
     *         store.
     * @param sourceTopologyCommitmentUtilizationStore The source topology commitment
     *         utilization store.
     * @return A bean for {@link CloudCommitmentRpcService}.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public CloudCommitmentRpcService cloudCommitmentRpcService(
            @Nonnull final SingleFieldDataStore<MappingInfo> sourceTopologyCommitmentMappingStore,
            @Nonnull final SingleFieldDataStore<UtilizationInfo> sourceTopologyCommitmentUtilizationStore) {
        return new CloudCommitmentRpcService(sourceTopologyCommitmentMappingStore,
                sourceTopologyCommitmentUtilizationStore);
    }

    /**
     * A bean for {@link CloudCommitmentServiceController}.
     *
     * @param cloudCommitmentRpcService The {@link CloudCommitmentRpcService}.
     * @return A bean for {@link CloudCommitmentServiceController}.
     */
    @Nonnull
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public CloudCommitmentServiceController cloudCommitmentServiceController(
            @Nonnull final CloudCommitmentRpcService cloudCommitmentRpcService) {
        return new CloudCommitmentServiceController(cloudCommitmentRpcService);
    }
}
