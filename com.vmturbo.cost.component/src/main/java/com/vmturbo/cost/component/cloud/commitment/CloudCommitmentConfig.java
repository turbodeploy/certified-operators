package com.vmturbo.cost.component.cloud.commitment;

import java.util.HashMap;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Scope;

import com.vmturbo.cloud.common.commitment.TopologyEntityCommitmentTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServicesREST.CloudCommitmentServiceController;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.TopologyEntityCoverageFilter;
import com.vmturbo.cost.component.cloud.commitment.mapping.CommitmentMappingFilter;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.TopologyCommitmentUtilizationFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * A configuration file for cloud commitment services.
 */
@Configuration
@Import({CloudCommitmentStatsConfig.class,
        RepositoryClientConfig.class,
        GroupClientConfig.class})
public class CloudCommitmentConfig {

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Value("${topologyCommitmentCoverage.isCoverageEstimatorEnabled:true}")
    private boolean isCoverageEstimatorEnabled;

    @Autowired
    private MarketComponent marketComponent;

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
            @Nonnull final SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> sourceTopologyCommitmentMappingStore,
            @Nonnull final SingleFieldDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> sourceTopologyCommitmentUtilizationStore) {
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

    /**
     * A bean for {@link ProjectedCommitmentMappingProcessor}.
     *
     * @return A bean for {@link ProjectedCommitmentMappingProcessor}.
     */
    @Bean
    @Scope(value = ConfigurableBeanFactory.SCOPE_SINGLETON)
    public ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor(
            @Nonnull final SingleFieldDataStore<CoverageInfo, TopologyEntityCoverageFilter> projectedTopologyCommitmentCoverageStore,
            @Nonnull final SingleFieldDataStore<MappingInfo, CommitmentMappingFilter> projectedTopologyCommitmentMappingStore,
            @Nonnull final SingleFieldDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> projectedTopologyCommitmentUtilizationStore) {
        return new ProjectedCommitmentMappingProcessor(new HashMap<>(),
                new HashMap<>(),
                new TopologyCommitmentCoverageWriter.Factory(
                        projectedTopologyCommitmentCoverageStore,
                        projectedTopologyCommitmentMappingStore,
                        projectedTopologyCommitmentUtilizationStore,
                        new TopologyEntityCommitmentTopology.TopologyEntityCommitmentTopologyFactory()),
                repositoryClientConfig.repositoryClient(),
                new DefaultTopologyEntityCloudTopologyFactory(
                        groupClientConfig.groupMemberRetriever()),
                isCoverageEstimatorEnabled);
    }

    /**
     * Returns the projected cloud commitment mapping listener.
     *
     * @return The projected cloud commitment mapping listener.
     */
    @Bean
    public CostComponentProjectedCommitmentMappingListener projectedEntityCommitmentMappingsListener(
            @Nonnull ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor) {
        final CostComponentProjectedCommitmentMappingListener projectedEntityCommitmentMappingsListener =
                new CostComponentProjectedCommitmentMappingListener(projectedCommitmentMappingProcessor);
        marketComponent.addProjectedCommitmentMappingListener(projectedEntityCommitmentMappingsListener);
        return projectedEntityCommitmentMappingsListener;
    }
}
