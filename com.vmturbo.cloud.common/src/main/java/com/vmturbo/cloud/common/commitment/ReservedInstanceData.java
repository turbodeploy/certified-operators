package com.vmturbo.cloud.common.commitment;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;

/**
 * A {@link CloudCommitmentData} wrapper class for reserved instances.
 */
@HiddenImmutableImplementation
@Immutable
public interface ReservedInstanceData extends CloudCommitmentData<ReservedInstanceBought, ReservedInstanceSpec> {

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default long commitmentId() {
        return commitment().getId();
    }

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
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
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceData}.
     */
    class Builder extends ImmutableReservedInstanceData.Builder {}
}
