package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.TopologyCommitmentData;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A commitment aggregate representing underlying cloud commitment topology entities.
 */
@HiddenImmutableImplementation
@Immutable(lazyhash =  true)
public interface TopologyCommitmentAggregate extends CloudCommitmentAggregate {

    /**
     * {@inheritDoc}.
     */
    @Override
    @Nonnull
    TopologyCommitmentAggregationInfo aggregationInfo();

    /**
     * {@inheritDoc}.
     */
    @Override
    Set<TopologyCommitmentData> commitments();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed {@link Builder} instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing immutabl {@link TopologyCommitmentAggregate} instance.
     */
    class Builder extends ImmutableTopologyCommitmentAggregate.Builder {}
}
