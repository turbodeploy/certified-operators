package com.vmturbo.cloud.common.commitment;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Auxiliary;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;

/**
 * A wrapper class for pairing a commitment with its specification.
 * @param <CommitmentTypeT> The contained cloud commitment type.
 */
public interface CloudCommitmentData<CommitmentTypeT> {

    /**
     * The cloud commitment.
     * @return The cloud commitment.
     */
    @Auxiliary
    @Nonnull
    CommitmentTypeT commitment();

    /**
     * The commitment ID.
     * @return The commitment ID.
     */
    long commitmentId();

    /**
     * The commitment capacity.
     * @return The commitment capacity.
     */
    @Nonnull
    CloudCommitmentAmount capacity();

    /**
     * The commitment type.
     * @return The commitment type.
     */
    @Nonnull
    CloudCommitmentType type();

    /**
     * Converts this {@link CloudCommitmentData} to {@link ReservedInstanceData}. A {@link ClassCastException}
     * should be expected, if {@link #type()} is not {@link CloudCommitmentType#RESERVED_INSTANCE}.
     * @return This instance, as a {@link ReservedInstanceData}.
     */
    @Nonnull
    default ReservedInstanceData asReservedInstance() {
        return (ReservedInstanceData)this;
    }

    /**
     * Converts this {@link CloudCommitmentData} to {@link TopologyCommitmentData}. A {@link ClassCastException}
     * should be expected, if {@link #type()} is not {@link CloudCommitmentType#TOPOLOGY_COMMITMENT}.
     * @return This instance, as a {@link TopologyCommitmentData}.
     */
    @Nonnull
    default TopologyCommitmentData asTopologyCommitment() {
        return (TopologyCommitmentData)this;
    }
}
