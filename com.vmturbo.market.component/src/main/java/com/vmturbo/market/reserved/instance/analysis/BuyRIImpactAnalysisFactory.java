package com.vmturbo.market.reserved.instance.analysis;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A factory for {@link BuyRIImpactAnalysis} instances
 */
public interface BuyRIImpactAnalysisFactory {

    /**
     * Creates a new instance of {@link BuyRIImpactAnalysis}
     * @param topologyInfo The {@link TopologyInfo} of {@code cloudTopology}
     * @param cloudTopology The target {@link CloudTopology} of the analysis
     * @param cloudCostData The {@link CloudCostData} associated with the {@code cloudTopology}
     * @param riCoverageByEntityOid The RI coverage of the {@code cloudTopology}
     * @return The newly created {@link BuyRIImpactAnalysis} instance
     */
    @Nonnull
    BuyRIImpactAnalysis createAnalysis(
            @Nonnull TopologyInfo topologyInfo,
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull CloudCostData cloudCostData,
            @Nonnull Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid);

    /**
     * The default instance of {@link BuyRIImpactAnalysisFactory}
     */
    class DefaultBuyRIImpactAnalysisFactory implements BuyRIImpactAnalysisFactory {
        private final RICoverageAllocatorFactory allocatorFactory;
        private final CoverageTopologyFactory coverageTopologyFactory;
        private final boolean allocatorConcurrentProcessing;
        private final boolean validateCoverages;

        /**
         * Construct an instance of {@link DefaultBuyRIImpactAnalysisFactory}.
         * @param allocatorFactory An instance of {@link RICoverageAllocatorFactory}
         * @param coverageTopologyFactory An instance of {@link CoverageTopologyFactory}
         * @param allocatorConcurrentProcessing A flag indicating whether to enable concurrent processing
         *                                      of underlying instances of the RI coverage allocator
         * @param validateCoverages A flag indicating whether underlying instances of the RI coverage
         *                          allocating should validate coverage
         */
        public DefaultBuyRIImpactAnalysisFactory(@Nonnull RICoverageAllocatorFactory allocatorFactory,
                                                 @Nonnull CoverageTopologyFactory coverageTopologyFactory,
                                                 boolean allocatorConcurrentProcessing,
                                                 boolean validateCoverages) {

            this.allocatorFactory = Objects.requireNonNull(allocatorFactory);
            this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
            this.allocatorConcurrentProcessing = allocatorConcurrentProcessing;
            this.validateCoverages = validateCoverages;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public BuyRIImpactAnalysis createAnalysis(
                @Nonnull TopologyInfo topologyInfo,
                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                @Nonnull CloudCostData cloudCostData,
                @Nonnull Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid) {

            Preconditions.checkNotNull(cloudTopology);
            Preconditions.checkNotNull(cloudCostData);

            final Collection<ReservedInstanceData> buyRIData =
                    cloudCostData.getAllBuyRIs();

            final CoverageTopology coverageTopology = coverageTopologyFactory.createCoverageTopology(
                    cloudTopology,
                    buyRIData.stream()
                            .map(ReservedInstanceData::getReservedInstanceSpec)
                            .collect(Collectors.toSet()),
                    buyRIData.stream()
                            .map(ReservedInstanceData::getReservedInstanceBought)
                            .collect(Collectors.toSet()));

            return new BuyRIImpactAnalysis(
                    allocatorFactory,
                    topologyInfo,
                    coverageTopology,
                    riCoverageByEntityOid,
                    allocatorConcurrentProcessing,
                    validateCoverages);
        }
    }

}
