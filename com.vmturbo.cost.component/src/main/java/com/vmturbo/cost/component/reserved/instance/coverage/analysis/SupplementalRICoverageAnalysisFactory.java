package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * A factory class for creating instances of {@link SupplementalRICoverageAnalysis}
 */
public class SupplementalRICoverageAnalysisFactory {

    private final CoverageTopologyFactory coverageTopologyFactory;

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private boolean riCoverageAllocatorValidation;

    private boolean concurrentRICoverageAllocation;

    /**
     * Constructs a new instance of {@link SupplementalRICoverageAnalysis}
     * @param coverageTopologyFactory An instance of {@link CoverageTopologyFactory}
     * @param reservedInstanceBoughtStore An instance of {@link ReservedInstanceSpecStore}
     * @param reservedInstanceSpecStore An instance of {@link ReservedInstanceSpecStore}
     * @param riCoverageAllocatorValidation A boolean flag indicating whether validation through the
     *                                      RI coverage allocator should be enabled
     * @param concurrentRICoverageAllocation A boolean flag indicating whether concurrent coverage allocation
     *                                       should be enabled in the RI coverage allocator
     */
    public SupplementalRICoverageAnalysisFactory(
            @Nonnull CoverageTopologyFactory coverageTopologyFactory,
            @Nonnull ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull ReservedInstanceSpecStore reservedInstanceSpecStore,
            boolean riCoverageAllocatorValidation,
            boolean concurrentRICoverageAllocation) {

        this.coverageTopologyFactory = Objects.requireNonNull(coverageTopologyFactory);
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.riCoverageAllocatorValidation = riCoverageAllocatorValidation;
        this.concurrentRICoverageAllocation = concurrentRICoverageAllocation;
    }

    /**
     * First resolves all available instances of both {@link ReservedInstanceBought} and {@link ReservedInstanceSpec}
     * from the applicable store. After resolving available RIs, a new instance of {@link CoverageTopology}
     * is constructed from the {@code cloudTopology} and resolved RIs/RISpecs. Finally, a new instance of
     * {@link SupplementalRICoverageAnalysis} is constructed based on the {@link CoverageTopology} and
     * {@code entityRICoverageUploads}.
     *
     * @param cloudTopology The {@link CloudTopology}, used to create an instance of {@link CoverageTopology}
     * @param entityRICoverageUploads The source {@link EntityRICoverageUpload} records
     * @return A newly created instance of {@link SupplementalRICoverageAnalysis}
     */
    public SupplementalRICoverageAnalysis createCoverageAnalysis(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull List<EntityRICoverageUpload> entityRICoverageUploads) {

        final List<ReservedInstanceBought> reservedInstances = reservedInstanceBoughtStore
                .getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER);
        final List<ReservedInstanceSpec> riSpecs = reservedInstanceSpecStore.getAllReservedInstanceSpec();
        final CoverageTopology coverageTopology = coverageTopologyFactory.createCoverageTopology(
                cloudTopology,
                riSpecs,
                reservedInstances);

        return new SupplementalRICoverageAnalysis(
                coverageTopology,
                entityRICoverageUploads,
                concurrentRICoverageAllocation,
                riCoverageAllocatorValidation);
    }
}
