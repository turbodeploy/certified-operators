package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.ReservedInstanceData;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;

/**
 * A {@link CloudCommitmentAggregate} for reserved instances.
 */
@HiddenImmutableImplementation
@Immutable(lazyhash =  true)
public interface ReservedInstanceAggregate extends CloudCommitmentAggregate {

    /**
     * {@inheritDoc}.
     */
    @Override
    ReservedInstanceAggregateInfo aggregateInfo();

    /**
     * {@inheritDoc}.
     */
    @Override
    Set<ReservedInstanceData> commitments();

    /**
     * {@inheritDoc}.
     */
    @Derived
    @Override
    default CloudCommitmentType commitmentType() {
        return CloudCommitmentType.RESERVED_INSTANCE;
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
     * A builder class for {@link ReservedInstanceAggregate} instances.
     */
    class Builder extends ImmutableReservedInstanceAggregate.Builder {}
}
