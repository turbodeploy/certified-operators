package com.vmturbo.cost.component.cloud.commitment;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.cloud.common.commitment.TopologyEntityCommitmentTopology.TopologyEntityCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cost.component.cloud.commitment.TopologyCommitmentCoverageEstimator.CommitmentCoverageEstimatorFactory;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceConfig;
import com.vmturbo.cost.component.reserved.instance.coverage.analysis.SupplementalCoverageAnalysisConfig;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A spring configuration for setting up beans related to topology commitment estimates.
 */
@Import({
        SupplementalCoverageAnalysisConfig.class,
        ReservedInstanceConfig.class,
        CloudCommitmentStatsConfig.class
})
@Configuration
public class TopologyCommitmentConfig {

    @Value("${topologyCommitmentCoverage.isCoverageEstimatorEnabled:true}")
    private boolean isCoverageEstimatorEnabled;

    /**
     * Creates a {@link TopologyCommitmentCoverageWriter.Factory} bean.
     * @param sourceTopologyCommitmentCoverageStore The topology commitment coverage store.
     * @param sourceTopologyCommitmentMappingStore The topology commitment mapping store.
     * @param sourceTopologyCommitmentUtilizationStore The topology commitment utilization.
     * @return The {@link TopologyCommitmentCoverageWriter.Factory} bean.
     */
    @Bean
    public TopologyCommitmentCoverageWriter.Factory topologyCommitmentCoverageWriterFactory(
            // Imported from CloudCommitmentStatsConfig
            @Nonnull SingleFieldDataStore<CoverageInfo, ?> sourceTopologyCommitmentCoverageStore,
            @Nonnull SingleFieldDataStore<MappingInfo, ?> sourceTopologyCommitmentMappingStore,
            @Nonnull SingleFieldDataStore<UtilizationInfo, ?> sourceTopologyCommitmentUtilizationStore) {

        return new TopologyCommitmentCoverageWriter.Factory(
                sourceTopologyCommitmentCoverageStore,
                sourceTopologyCommitmentMappingStore,
                sourceTopologyCommitmentUtilizationStore,
                new TopologyEntityCommitmentTopologyFactory());
    }

    /**
     * Creats a {@link CommitmentCoverageEstimatorFactory} bean.
     * @param entityReservedInstanceMappingStore The entity RI mapping store.
     * @param topologyCommitmentCoverageWriterFactory The topology commitment coverage writer factory.
     * @param coverageAllocatorFactory The coverage allocator factory.
     * @param coverageTopologyFactory The coverage topology factory.
     * @param commitmentAggregatorFactory The commitment aggregator factory.
     * @return The {@link CommitmentCoverageEstimatorFactory} bean.
     */
    @Bean
    public CommitmentCoverageEstimatorFactory commitmentCoverageEstimatorFactory(
            // Imported from ReservedInstanceConfig
            @Nonnull EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull TopologyCommitmentCoverageWriter.Factory topologyCommitmentCoverageWriterFactory,
            // Imported from SupplementalCoverageAnalysisConfig
            @Nonnull CoverageAllocatorFactory coverageAllocatorFactory,
            @Nonnull CoverageTopologyFactory coverageTopologyFactory,
            @Nonnull CloudCommitmentAggregatorFactory commitmentAggregatorFactory) {

        return new CommitmentCoverageEstimatorFactory(
                entityReservedInstanceMappingStore,
                topologyCommitmentCoverageWriterFactory,
                coverageAllocatorFactory,
                coverageTopologyFactory,
                commitmentAggregatorFactory,
                isCoverageEstimatorEnabled);
    }
}
