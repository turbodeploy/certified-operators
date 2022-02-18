package com.vmturbo.cost.component.cloud.commitment;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocationConfig;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * Estimates cloud commitment coverage for a specific topology. The flow of the estimator is similar to
 * supplemental coverage analysis, except it is specific to cloud commitment topology entities.
 */
public class TopologyCommitmentCoverageEstimator {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    private final Supplier<CloudCommitmentCoverageAllocator> coverageAllocatorSupplier;

    private final TopologyCommitmentCoverageWriter commitmentCoverageWriter;

    private final boolean isEstimateEnabled;

    private TopologyCommitmentCoverageEstimator(
            @Nonnull TopologyInfo topologyInfo,
            @Nonnull Supplier<CloudCommitmentCoverageAllocator> coverageAllocatorSupplier,
            @Nonnull TopologyCommitmentCoverageWriter commitmentCoverageWriter,
            boolean isEstimateEnabled) {

        this.topologyInfo = Objects.requireNonNull(topologyInfo);
        this.coverageAllocatorSupplier = Objects.requireNonNull(coverageAllocatorSupplier);
        this.commitmentCoverageWriter = Objects.requireNonNull(commitmentCoverageWriter);
        this.isEstimateEnabled = isEstimateEnabled;
    }

    /**
     * Estimates and persist cloud commitment coverage for the configured topology.
     */
    public void estimateAndPersistCoverage() {

        try {
            if (isEstimateEnabled) {

                logger.info("Estimating topology commitment coverage for {}", topologyInfo);

                final Stopwatch stopwatch = Stopwatch.createStarted();
                final CloudCommitmentCoverageAllocator coverageAllocator = coverageAllocatorSupplier.get();
                final CloudCommitmentCoverageAllocation coverageAllocation = coverageAllocator.allocateCoverage();

                commitmentCoverageWriter.persistCommitmentAllocations(topologyInfo, coverageAllocation.allocatorCoverageTable());

                logger.info("Estimated and persisted {} coverage allocations in {}",
                        coverageAllocation.allocatorCoverageTable().size(), stopwatch);
            } else {
                logger.info("Topology commitment estimates are disabled. Skipping.");
            }

        } catch (Exception e) {
            logger.error("Error while estimating topology commitment coverage", e);
        }
    }

    /**
     * A factory for producing {@link TopologyCommitmentCoverageEstimator} instances.
     */
    public interface Factory {

        /**
         * Creates and returns a new estimator for the supplied topology.
         * @param topologyInfo The topology info.
         * @param cloudTopology The cloud topology.
         * @return The newly constructed estimator.
         */
        @Nonnull
        TopologyCommitmentCoverageEstimator newEstimator(@Nonnull TopologyInfo topologyInfo,
                                                         @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology);
    }

    /**
     * The default implementation of {@link Factory}.
     */
    public static class CommitmentCoverageEstimatorFactory implements Factory {

        private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

        private final TopologyCommitmentCoverageWriter.Factory commitmentCoverageWriterFactory;

        private final CoverageAllocatorFactory coverageAllocatorFactory;

        private final CoverageTopologyFactory coverageTopologyFactory;

        private final CloudCommitmentAggregatorFactory commitmentAggregatorFactory;

        private final boolean isEstimatorEnabled;

        /**
         * Constructs a new coverage estimator factory.
         * @param entityReservedInstanceMappingStore The entity<->RI mapping store, used to query the source coverage for coverage allocation.
         * @param commitmentCoverageWriterFactory The commitment coverage writer factory.
         * @param coverageAllocatorFactory The coverage allocator factory.
         * @param coverageTopologyFactory The coverage topology factory.
         * @param commitmentAggregatorFactory The commitment aggregator factory.
         * @param isEstimatorEnabled A flag indicating whether estimates are enabled.
         */
        public CommitmentCoverageEstimatorFactory(@Nonnull EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
                                                  @Nonnull TopologyCommitmentCoverageWriter.Factory commitmentCoverageWriterFactory,
                                                  @Nonnull CoverageAllocatorFactory coverageAllocatorFactory,
                                                  @Nonnull CoverageTopologyFactory coverageTopologyFactory,
                                                  @Nonnull CloudCommitmentAggregatorFactory commitmentAggregatorFactory,
                                                  boolean isEstimatorEnabled) {

            this.entityReservedInstanceMappingStore = Objects.requireNonNull(entityReservedInstanceMappingStore);
            this.commitmentCoverageWriterFactory = Objects.requireNonNull(commitmentCoverageWriterFactory);
            this.coverageAllocatorFactory = Objects.requireNonNull(coverageAllocatorFactory);
            this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
            this.commitmentAggregatorFactory = Objects.requireNonNull(commitmentAggregatorFactory);
            this.isEstimatorEnabled = isEstimatorEnabled;
        }

        /**
         * {@inheritDoc}.
         */
        @Override
        public TopologyCommitmentCoverageEstimator newEstimator(@Nonnull TopologyInfo topologyInfo,
                                                                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

            final TopologyCommitmentCoverageWriter commitmentCoverageWriter = commitmentCoverageWriterFactory.newWriter(cloudTopology);

            return new TopologyCommitmentCoverageEstimator(topologyInfo,
                    coverageAllocatorSupplier(cloudTopology),
                    commitmentCoverageWriter, isEstimatorEnabled);
        }

        private Supplier<CloudCommitmentCoverageAllocator> coverageAllocatorSupplier(
                @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

            return () -> {

                final Set<CloudCommitmentAggregate> commitmentAggregates = createCommitmentAggregates(cloudTopology);
                final CoverageTopology coverageTopology = coverageTopologyFactory.createCoverageTopology(
                        cloudTopology, commitmentAggregates);

                return coverageAllocatorFactory.createAllocator(
                        CoverageAllocationConfig.builder()
                                .sourceCoverage(resolveSourceCoverage())
                                .coverageTopology(coverageTopology)
                                .build());
            };
        }

        private Set<CloudCommitmentAggregate> createCommitmentAggregates(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

            final CloudCommitmentAggregator cloudCommitmentAggregator =
                    commitmentAggregatorFactory.newIdentityAggregator(cloudTopology);

            final List<CloudCommitmentData> commitmentDataList = cloudTopology.getAllEntitiesOfType(EntityType.CLOUD_COMMITMENT_VALUE)
                    .stream()
                    .map(commitment -> TopologyCommitmentData.builder()
                            .commitment(commitment)
                            .build())
                    .collect(ImmutableList.toImmutableList());

            logger.info("Resolved {} commitments from the topology", commitmentDataList.size());

            for (CloudCommitmentData commitmentData : commitmentDataList) {
                try {
                    cloudCommitmentAggregator.collectCommitment(commitmentData);
                } catch (AggregationFailureException e) {
                    logger.warn("Error aggregating commitment ID {}", commitmentData.commitmentId(), e);
                }
            }

            return cloudCommitmentAggregator.getAggregates();
        }

        private Table<Long, Long, CloudCommitmentAmount> resolveSourceCoverage() {
            return entityReservedInstanceMappingStore.getEntityRiCoverage().values()
                    .stream()
                    .flatMap(entityCoverage -> entityCoverage.getCouponsCoveredByRiMap().entrySet()
                            .stream()
                            .map(riEntry -> ImmutableTriple.of(entityCoverage.getEntityId(), riEntry.getKey(), riEntry.getValue())))
                    .collect(ImmutableTable.toImmutableTable(
                            Triple::getLeft,
                            Triple::getMiddle,
                            entityRITriple -> CloudCommitmentAmount.newBuilder()
                                    .setCoupons(entityRITriple.getRight())
                                    .build()));
        }
    }
}
