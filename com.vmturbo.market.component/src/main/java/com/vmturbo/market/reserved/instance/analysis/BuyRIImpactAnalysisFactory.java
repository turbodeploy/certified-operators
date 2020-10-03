package com.vmturbo.market.reserved.instance.analysis;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A factory for {@link BuyRIImpactAnalysis} instances.
 */
public interface BuyRIImpactAnalysisFactory {

    /**
     * Creates a new instance of {@link BuyRIImpactAnalysis}.
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
            @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData,
            @Nonnull Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid);

    /**
     * The default instance of {@link BuyRIImpactAnalysisFactory}.
     */
    class DefaultBuyRIImpactAnalysisFactory implements BuyRIImpactAnalysisFactory {

        private final Logger logger = LogManager.getLogger();

        private final CoverageAllocatorFactory allocatorFactory;
        private final CoverageTopologyFactory coverageTopologyFactory;
        private final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory;
        private final boolean allocatorConcurrentProcessing;
        private final boolean validateCoverages;

        /**
         * Construct an instance of {@link DefaultBuyRIImpactAnalysisFactory}.
         *
         * @param allocatorFactory              An instance of {@link CoverageAllocatorFactory}
         * @param coverageTopologyFactory       An instance of {@link CoverageTopologyFactory}
         * @param cloudCommitmentAggregatorFactory A factory instance for creating {@link CloudCommitmentAggregator}
         *                                         instances.
         * @param allocatorConcurrentProcessing A flag indicating whether to enable concurrent processing
         *                                      of underlying instances of the RI coverage allocator
         * @param validateCoverages             A flag indicating whether underlying instances of the RI coverage
         *                                      allocating should validate coverage
         */
        public DefaultBuyRIImpactAnalysisFactory(@Nonnull CoverageAllocatorFactory allocatorFactory,
                                                 @Nonnull CoverageTopologyFactory coverageTopologyFactory,
                                                 @Nonnull CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory,
                                                 boolean allocatorConcurrentProcessing,
                                                 boolean validateCoverages) {

            this.allocatorFactory = Objects.requireNonNull(allocatorFactory);
            this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
            this.cloudCommitmentAggregatorFactory = Objects.requireNonNull(cloudCommitmentAggregatorFactory);
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
                @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData,
                @Nonnull Map<Long, EntityReservedInstanceCoverage> riCoverageByEntityOid) {

            Preconditions.checkNotNull(cloudTopology);
            Preconditions.checkNotNull(cloudCostData);

            final CoverageTopology coverageTopology = coverageTopologyFactory.createCoverageTopology(
                    cloudTopology,
                    resolveCommitmentAggregates(cloudTopology, cloudCostData));

            return new BuyRIImpactAnalysis(
                    allocatorFactory,
                    topologyInfo,
                    coverageTopology,
                    riCoverageByEntityOid,
                    allocatorConcurrentProcessing,
                    validateCoverages);
        }

        private Set<CloudCommitmentAggregate> resolveCommitmentAggregates(
                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
                @Nonnull CloudCostData<TopologyEntityDTO> cloudCostData) {

            // convert to cloud common RI data
            final Set<ReservedInstanceData> buyRIData =
                    cloudCostData.getAllBuyRIs()
                        .stream()
                        .map(riData -> ReservedInstanceData.builder()
                                .commitment(riData.getReservedInstanceBought())
                                .spec(riData.getReservedInstanceSpec())
                                .build())
                        .collect(ImmutableSet.toImmutableSet());

            final CloudCommitmentAggregator aggregator =
                    cloudCommitmentAggregatorFactory.newIdentityAggregator(cloudTopology);
            buyRIData.forEach(buyRI -> {
                try {
                    aggregator.collectCommitment(buyRI);
                } catch (AggregationFailureException e) {
                    logger.error("Error aggregating BuyRI (BuyRI ID={})", buyRI.commitmentId(), e);
                }
            });

            return aggregator.getAggregates();
        }
    }

}
