package com.vmturbo.cloud.commitment.analysis.runtime.data;

import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeriesData;

/**
 * Wraps an aggregate set of classified cloud tier demand.
 */
@Immutable
public interface AggregateDemandSegment extends TimeSeriesData {

    /**
     * The aggregate set of classified cloud tier demand.
     * @return The aggregate set of classified cloud tier demand.
     */
    @Nonnull
    Set<AggregateCloudTierDemand> aggregateCloudTierDemand();
}
