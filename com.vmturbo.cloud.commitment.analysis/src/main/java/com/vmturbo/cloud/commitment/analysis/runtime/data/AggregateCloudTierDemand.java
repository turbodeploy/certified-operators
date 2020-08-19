package com.vmturbo.cloud.commitment.analysis.runtime.data;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierDemand;

/**
 * Represents an aggregate of {@link com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping},
 * grouping all demand across entities with the same scope and demand.
 *
 * @param <CLASSIFICATION_TYPE> The classification type of this demand. The type should be either
 *                             {@link com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification} or
 *                             {@link com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.ProjectedDemandClassification}.
 */
@Immutable
public interface AggregateCloudTierDemand<CLASSIFICATION_TYPE> extends ScopedCloudTierDemand {

    /**
     * The entities OIDs represented by this demand.
     * @return The entities OIDs represented by this demand.
     */
    @Nonnull
    Set<Long> entityOids();

    /**
     * The classification of this demand.
     * @return The classification of this demand.
     */
    @Nonnull
    CLASSIFICATION_TYPE classification();

    /**
     * The amount of this demand. Represents a normalization of demand for entities which may have
     * differing time intervals. The normalization will be based on the time window of the aggregate.
     * For example, if the time window is over a specific hour and each of 3 VMs are up for half an
     * hour within that time window, the demand amount will equal 1.5.
     * @return The amount of this demand, normalized to the parent time window of this aggregate.
     */
    @Nonnull
    double demandAmount();
}
