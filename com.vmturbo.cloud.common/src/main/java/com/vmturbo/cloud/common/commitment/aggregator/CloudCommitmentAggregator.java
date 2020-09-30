package com.vmturbo.cloud.common.commitment.aggregator;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;

/**
 * An aggregator of cloud commitments, based on how commitments may be assigned to coverage entities.
 */
public interface CloudCommitmentAggregator {

    /**
     * Processes the {@code commitmentData}, adding it to the set of stored aggregates.
     * @param commitmentData The commitment data to aggregate.
     * @throws AggregationFailureException Thrown if there is a failure in aggregating the commitment.
     * This may happen in cases where the referenced compute tier cannot be found or resolving the
     * associated billing family fails.
     */
    void collectCommitment(@Nonnull CloudCommitmentData commitmentData) throws AggregationFailureException;

    /**
     * Returns an immutable set of the collected commitment aggregates.
     * @return An immutable set of the commitment aggregates.
     */
    @Nonnull
    Set<CloudCommitmentAggregate> getAggregates();

    /**
     * A factory class for {@link CloudCommitmentAggregator}.
     */
    interface CloudCommitmentAggregatorFactory {

        /**
         * Creates and returns the default aggregator, which is aggregate collected commitments based
         * on coverage assignment.
         * @param tierTopology The cloud topology, which must contain the compute tiers referenced
         *                     by collected commitments.
         * @return The newly constructed aggregator.
         */
        @Nonnull
        CloudCommitmentAggregator newAggregator(@Nonnull CloudTopology<TopologyEntityDTO> tierTopology);

        /**
         * Creates an returns an aggregator, which aggregates commitments based on their identity (on the
         * assigned ID).
         * @param tierTopology The cloud topology, which must contain the compute tiers referenced
         *                     by collected commitments.
         * @return The newly constructed aggregator.
         */
        @Nonnull
        CloudCommitmentAggregator newIdentityAggregator(@Nonnull CloudTopology<TopologyEntityDTO> tierTopology);
    }

    /**
     * An exception representing an error in aggregating a cloud commitment.
     */
    class AggregationFailureException extends Exception {

        /**
         * Constructs an {@link AggregationFailureException}, based on an underlying {@code cause}.
         * @param cause The underlying cause of the exception.
         */
        public AggregationFailureException(@Nonnull final Throwable cause) {
            super(cause);
        }
    }
}
