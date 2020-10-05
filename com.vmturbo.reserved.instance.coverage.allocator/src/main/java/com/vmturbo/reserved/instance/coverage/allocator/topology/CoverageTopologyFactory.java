package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.topology.processor.api.util.ThinTargetCache;

/**
 * Factory class for creating instances of {@link CoverageTopology}.
 */
public class CoverageTopologyFactory {

    private final ThinTargetCache targetCache;

    /**
     * Constructs a new factory.
     * @param targetCache The thin target cache, used to resolve the cloud service provider for entities.
     */
    public CoverageTopologyFactory(@Nonnull ThinTargetCache targetCache) {
        this.targetCache = Objects.requireNonNull(targetCache);
    }

    /**
     * Creates a new instance of {@link CoverageTopology}.
     *
     * @param cloudTopology An instance of {@link CloudTopology}, used to populate {@link TopologyEntityDTO}
     *                      data for the created {@link CoverageTopology}
     * @param commitmentAggregates The {@link CloudCommitmentAggregate} set to include in the topology.
     * @return A newly created instance of {@link CoverageTopology}
     */
    @Nonnull
    public CoverageTopology createCoverageTopology(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull Set<CloudCommitmentAggregate> commitmentAggregates) {
        return new DelegatingCoverageTopology(
                cloudTopology,
                targetCache,
                commitmentAggregates);
    }
}
