package com.vmturbo.cloud.common.commitment;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;

/**
 * {@link CloudCommitmentData} instance for a representation of a commitment though the topology.
 */
@HiddenImmutableImplementation
@Immutable
public interface TopologyCommitmentData extends CloudCommitmentData<TopologyEntityDTO> {

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default CloudCommitmentType type() {
        return CloudCommitmentType.TOPOLOGY_COMMITMENT;
    }

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default long commitmentId() {
        return commitment().getOid();
    }

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default CloudCommitmentAmount capacity() {
        return CloudCommitmentUtils.createCapacityAmount(commitment());
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutable {@link TopologyCommitmentData} instances.
     */
    class Builder extends ImmutableTopologyCommitmentData.Builder {}
}
