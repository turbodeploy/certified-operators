package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.SetMultimap;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeriesData;

/**
 * Wraps an aggregate set of classified cloud tier demand. The segment corresponds to a single
 * analysis bucket, within the full analysis window.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface AggregateDemandSegment extends TimeSeriesData {

    /**
     * The aggregate set of classified cloud tier demand.
     * @return The aggregate set of classified cloud tier demand.
     */
    @Nonnull
    SetMultimap<ScopedCloudTierInfo, AggregateCloudTierDemand> aggregateCloudTierDemand();

    /**
     * Constructs and returns a new builder instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * Constructs an empty {@link AggregateDemandSegment} for the specific analysis bucket.
     * @param bucket The analysis bucket associated with the empty demand segment.
     * @return The newly created aggregate demand segment.
     */
    @Nonnull
    static AggregateDemandSegment emptySegment(@Nonnull TimeInterval bucket) {

        Preconditions.checkNotNull(bucket);

        return AggregateDemandSegment.builder()
                .timeInterval(bucket)
                .build();
    }

    /**
     * A builder class for {@link AggregateDemandSegment}.
     */
    class Builder extends ImmutableAggregateDemandSegment.Builder {}
}
