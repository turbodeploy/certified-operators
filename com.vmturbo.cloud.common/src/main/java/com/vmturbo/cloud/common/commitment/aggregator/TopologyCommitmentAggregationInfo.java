package com.vmturbo.cloud.common.commitment.aggregator;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Derived;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.CloudCommitmentResourceScope;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentType;

/**
 * Aggregation info for a topology entity cloud commitment.
 */
@HiddenImmutableImplementation
@Immutable(prehash = true)
public interface TopologyCommitmentAggregationInfo extends AggregationInfo {

    /**
     * The resource scope of the cloud commitment, representing the cloud tier or cloud service
     * this aggregate can cover.
     * @return The resource scope.
     */
    @Nonnull
    CloudCommitmentResourceScope resourceScope();

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Derived
    @Override
    default CloudCommitmentType commitmentType() {
        return CloudCommitmentType.TOPOLOGY_COMMITMENT;
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
     * A builder class for constructing immutable {@link TopologyCommitmentAggregationInfo} instances.
     */
    class Builder extends ImmutableTopologyCommitmentAggregationInfo.Builder {}
}
