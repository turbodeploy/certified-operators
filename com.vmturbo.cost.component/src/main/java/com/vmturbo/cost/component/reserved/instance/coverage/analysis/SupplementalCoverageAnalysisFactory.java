package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.AggregationFailureException;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought.ReservedInstanceBoughtInfo;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A factory class for creating instances of {@link SupplementalCoverageAnalysis}.
 */
public class SupplementalCoverageAnalysisFactory {

    private final CoverageAllocatorFactory allocatorFactory;

    private final CoverageTopologyFactory coverageTopologyFactory;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory;

    private boolean riCoverageAllocatorValidation;

    private boolean concurrentRICoverageAllocation;

    private final Logger logger = LogManager.getLogger();

    /**
     * Constructs a new instance of {@link SupplementalCoverageAnalysis}.
     * @param allocatorFactory An instance of {@link CoverageAllocatorFactory}
     * @param coverageTopologyFactory An instance of {@link CoverageTopologyFactory}
     * @param reservedInstanceBoughtStore An instance of {@link ReservedInstanceSpecStore}
     * @param reservedInstanceSpecStore An instance of {@link ReservedInstanceSpecStore}
     * @param riCoverageAllocatorValidation A boolean flag indicating whether validation through the
*                                      RI coverage allocator should be enabled
     * @param concurrentRICoverageAllocation A boolean flag indicating whether concurrent coverage allocation
     * @param cloudCommitmentAggregatorFactory A factory for creating {@link CloudCommitmentAggregator}
     *                                         instances.
     */
    public SupplementalCoverageAnalysisFactory(
            @Nonnull CoverageAllocatorFactory allocatorFactory,
            @Nonnull CoverageTopologyFactory coverageTopologyFactory,
            @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
            @Nonnull CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory,
            boolean riCoverageAllocatorValidation,
            boolean concurrentRICoverageAllocation) {

        this.allocatorFactory = Objects.requireNonNull(allocatorFactory);
        this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.cloudCommitmentAggregatorFactory = Objects.requireNonNull(cloudCommitmentAggregatorFactory);
        this.riCoverageAllocatorValidation = riCoverageAllocatorValidation;
        this.concurrentRICoverageAllocation = concurrentRICoverageAllocation;
    }

    /**
     * First resolves all available instances of both {@link ReservedInstanceBought} and {@link ReservedInstanceSpec}
     * from the applicable store. After resolving available RIs, a new instance of {@link CoverageTopology}
     * is constructed from the {@code cloudTopology} and resolved RIs/RISpecs. Finally, a new instance of
     * {@link SupplementalCoverageAnalysis} is constructed based on the {@link CoverageTopology} and
     * {@code entityRICoverageUploads}.
     *
     * @param cloudTopology The {@link CloudTopology}, used to create an instance of {@link CoverageTopology}
     * @param entityRICoverageUploads The source {@link EntityRICoverageUpload} records
     * @return A newly created instance of {@link SupplementalCoverageAnalysis}
     */
    public SupplementalCoverageAnalysis createCoverageAnalysis(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull List<EntityRICoverageUpload> entityRICoverageUploads) {

        final CoverageTopology coverageTopology = coverageTopologyFactory.createCoverageTopology(
                cloudTopology,
                resolveCloudCommitmentAggregates(cloudTopology));

        return new SupplementalCoverageAnalysis(
                allocatorFactory,
                coverageTopology,
                entityRICoverageUploads,
                concurrentRICoverageAllocation,
                riCoverageAllocatorValidation);
    }


    private Set<CloudCommitmentAggregate> resolveCloudCommitmentAggregates(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {

        ReservedInstanceBoughtFilter selectAllDiscovered = ReservedInstanceBoughtFilter
                .newBuilder().excludeRIsFromUndiscoveredAcccounts(true).build();
        // Exclude the undiscovered RIs from the reserved instances so as to not run supplementary allocation
        // on undiscovered RIs

        List<ReservedInstanceBought> reservedInstances =
                reservedInstanceBoughtStore
                        .getReservedInstanceBoughtForAnalysis(selectAllDiscovered);

        // Query only for RI specs referenced from reservedInstances
        final Set<Long> riSpecIds = reservedInstances.stream()
                .filter(ReservedInstanceBought::hasReservedInstanceBoughtInfo)
                .map(ReservedInstanceBought::getReservedInstanceBoughtInfo)
                .filter(ReservedInstanceBoughtInfo::hasReservedInstanceSpec)
                .map(ReservedInstanceBoughtInfo::getReservedInstanceSpec)
                .collect(ImmutableSet.toImmutableSet());
        final Map<Long, ReservedInstanceSpec> riSpecsById =
                reservedInstanceSpecStore.getReservedInstanceSpecByIds(riSpecIds)
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                ReservedInstanceSpec::getId,
                                Function.identity()));

        final CloudCommitmentAggregator cloudCommitmentAggregator =
                cloudCommitmentAggregatorFactory.newIdentityAggregator(cloudTopology);

        for (ReservedInstanceBought ri : reservedInstances) {

            final long riSpecId = ri.getReservedInstanceBoughtInfo().getReservedInstanceSpec();
            if (riSpecsById.containsKey(riSpecId)) {

                final ReservedInstanceSpec riSpec = riSpecsById.get(riSpecId);
                final ReservedInstanceData riData = ReservedInstanceData.builder()
                        .commitment(ri)
                        .spec(riSpec)
                        .build();

                try {
                    cloudCommitmentAggregator.collectCommitment(riData);
                } catch (AggregationFailureException e) {
                    logger.error("Error aggregating RI (RI ID={})", ri.getId(), e);
                }
            } else {
                logger.error("Unable to find RI spec for RI (RI ID={}, Spec ID={})",
                        ri.getId(), riSpecId);
            }
        }

        return cloudCommitmentAggregator.getAggregates();
    }
}
