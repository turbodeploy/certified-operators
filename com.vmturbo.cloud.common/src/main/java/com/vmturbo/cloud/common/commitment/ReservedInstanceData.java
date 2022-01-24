package com.vmturbo.cloud.common.commitment;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Auxiliary;
import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;

/**
 * A {@link CloudCommitmentData} wrapper class for reserved instances.
 */
@HiddenImmutableImplementation
@Immutable
public interface ReservedInstanceData extends CloudCommitmentData<ReservedInstanceBought> {

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default long commitmentId() {
        return commitment().getId();
    }


    /**
     * The RI specification.
     * @return The RI specification.
     */
    @Auxiliary
    @Nonnull
    ReservedInstanceSpec spec();

    /**
     * The {@link #spec()} ID.
     * @return The {@link #spec()} ID.
     */
    @Derived
    default long specId() {
        return spec().getId();
    }

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default CloudCommitmentType type() {
        return CloudCommitmentType.RESERVED_INSTANCE;
    }

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default CloudCommitmentAmount capacity() {
        return CloudCommitmentAmount.newBuilder()
                .setCoupons(commitment().getReservedInstanceBoughtInfo()
                        .getReservedInstanceBoughtCoupons()
                        .getNumberOfCoupons())
                .build();
    }


    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceData}.
     */
    class Builder extends ImmutableReservedInstanceData.Builder {}
}
