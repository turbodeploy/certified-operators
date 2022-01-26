package com.vmturbo.cloud.common.commitment;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A {@link CloudCommitmentTopology} implementation focused on processing {@link TopologyEntityDTO} instances.
 */
public class TopologyEntityCommitmentTopology implements CloudCommitmentTopology {

    private final CloudTopology<TopologyEntityDTO> cloudTopology;

    private TopologyEntityCommitmentTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
        this.cloudTopology = Objects.requireNonNull(cloudTopology);
    }

    @Override
    public Set<Long> getCoveredCloudServices(long commitmentId) {
        return cloudTopology.getEntity(commitmentId)
                .map(commitmentEntity -> commitmentEntity.getConnectedEntityListList()
                        .stream()
                        .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.NORMAL_CONNECTION
                                && connectedEntity.getConnectedEntityType() == EntityType.CLOUD_SERVICE_VALUE)
                        .map(ConnectedEntity::getConnectedEntityId)
                        .collect(ImmutableSet.toImmutableSet()))
                .orElse(ImmutableSet.of());
    }

    @Override
    public Set<Long> getCoveredAccounts(long commitmentId) {
        return cloudTopology.getEntity(commitmentId)
                .map(commitmentEntity -> commitmentEntity.getConnectedEntityListList()
                        .stream()
                        .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.NORMAL_CONNECTION
                                && connectedEntity.getConnectedEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE)
                        .map(ConnectedEntity::getConnectedEntityId)
                        .collect(ImmutableSet.toImmutableSet()))
                .orElse(ImmutableSet.of());
    }

    /**
     * A {@link com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory} implementation, which
     * created {@link TopologyEntityCommitmentTopology} instances.
     */
    public static class TopologyEntityCommitmentTopologyFactory implements CloudCommitmentTopologyFactory<TopologyEntityDTO> {

        @Override
        public CloudCommitmentTopology createTopology(@Nonnull CloudTopology<TopologyEntityDTO> cloudTopology) {
            return new TopologyEntityCommitmentTopology(cloudTopology);
        }
    }
}
