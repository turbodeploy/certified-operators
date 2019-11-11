package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Static factory class for creating instances of {@link CoverageTopology}
 */
public class CoverageTopologyFactory {

    private final ThinTargetCache targetCache;

    public CoverageTopologyFactory(@Nonnull ThinTargetCache targetCache) {
        this.targetCache = Objects.requireNonNull(targetCache);
    }

    /**
     * Creates a new instance of {@link CoverageTopology}
     *
     * @param cloudTopology An instance of {@link CloudTopology}, used to populate {@link TopologyEntityDTO}
     *                      data for the created {@link CoverageTopology}
     * @param reservedInstanceSpecs The {@link ReservedInstanceSpec} instances used to populate
     *                              the {@link CoverageTopology}
     * @param reservedInstances The {@link ReservedInstanceBought} instances used to populate the
     *                          {@link CoverageTopology}
     * @return A newly created instance of {@link CoverageTopology}
     */
    @Nonnull
    public CoverageTopology createCoverageTopology(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Collection<ReservedInstanceSpec> reservedInstanceSpecs,
            @Nonnull Collection<ReservedInstanceBought> reservedInstances) {
        return new CoverageTopologyImpl(
                cloudTopology,
                targetCache,
                reservedInstanceSpecs,
                reservedInstances);
    }
}
