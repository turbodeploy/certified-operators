package com.vmturbo.reserved.instance.coverage.allocator.topology;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregate;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * Factory class for creating instances of {@link CoverageTopology}.
 */
public class CoverageTopologyFactory {

    private final CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory;

    /**
     * Constructs a new coverage topology.
     * @param commitmentTopologyFactory The commitment topology factory.
     */
    public CoverageTopologyFactory(@Nonnull CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory) {
        this.commitmentTopologyFactory = Objects.requireNonNull(commitmentTopologyFactory);
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
                commitmentTopologyFactory.createTopology(cloudTopology),
                commitmentAggregates);
    }
}
