package com.vmturbo.cloud.commitment.analysis.inventory;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * Interface representing the capacity of CCA available in the inventory. The capacity is broken down
 * into time segments.
 */
@HiddenImmutableImplementation
@Immutable
public interface CloudCommitmentCapacity {

    /**
     * A {@link CloudCommitmentCapacity} instance representing zero capacity.
     */
    CloudCommitmentCapacity ZERO_CAPACITY = CloudCommitmentCapacity.builder()
            .capacityAvailable(0.0)
            .build();

    /**
     * The capacity offered by the cloud commitment in the current inventory of the CCA plan scope.
     *
     * @return capacity represented by a double.
     */
    double capacityAvailable();

    /**
     * Constructs and returns a {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link AnalysisTopology}.
     */
    class Builder extends ImmutableCloudCommitmentCapacity.Builder {}
}
