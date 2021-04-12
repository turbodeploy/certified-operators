package com.vmturbo.cloud.common.commitment;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Auxiliary;

import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * A wrapper class for pairing a commitment with its specification.
 * @param <CommitmentTypeT> The contained cloud commitment type.
 * @param <SpecificationTypeT> The contained commitment specification type.
 */
public interface CloudCommitmentData<CommitmentTypeT, SpecificationTypeT> {

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
     * The commitment specification.
     * @return The commitment specification.
     */
    @Auxiliary
    @Nonnull
    SpecificationTypeT spec();

    /**
     * The commitment specification ID.
     * @return The commitment specification ID.
     */
    long specId();

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
}
