package com.vmturbo.cloud.common.commitment.aggregator;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope.ComputeTierResourceScope;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CloudCommitmentData.CloudCommitmentStatus;

/**
 * Aggregation info specific to reserved instances.
 */
@HiddenImmutableImplementation
@Immutable(prehash = true)
public interface ReservedInstanceAggregationInfo extends AggregationInfo {

    /**
     * {@inheritDoc}.
     */
    @Override
    ComputeTierResourceScope resourceScope();

    /**
     * The commitment type of the aggregate. This will always return {@link CloudCommitmentType#RESERVED_INSTANCE}.
     * @return The commitment type of the aggregate. This will always return {@link CloudCommitmentType#RESERVED_INSTANCE}.
     */
    @Nonnull
    @Derived
    default CloudCommitmentType commitmentType() {
        return CloudCommitmentType.RESERVED_INSTANCE;
    }

    /**
     * The status of the RI. Currently, only active RIs are discovered.
     * @return The status of the RI.
     */
    @Derived
    @Override
    default CloudCommitmentStatus status() {
        return CloudCommitmentStatus.CLOUD_COMMITMENT_STATUS_ACTIVE;
    }

    /**
     * Constructs and return a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ReservedInstanceAggregationInfo}.
     */
    class Builder extends ImmutableReservedInstanceAggregationInfo.Builder {}
}
