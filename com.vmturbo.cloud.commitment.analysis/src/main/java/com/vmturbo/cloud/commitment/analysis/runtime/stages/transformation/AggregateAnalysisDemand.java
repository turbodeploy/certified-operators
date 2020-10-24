package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;

/**
 * The root of aggregated analysis demand, containing bucketed aggregate demand. This is the output
 * of {@link DemandTransformationStage}.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface AggregateAnalysisDemand {

    /**
     * The aggregated demand segments as a time series. Each {@link AggregateDemandSegment} instance
     * will correspond to a single analysis bucket within the analysis window.
     * @return The time series of analysis demand segments.
     */
    @Nonnull
    TimeSeries<AggregateDemandSegment> aggregateDemandSeries();

    /**
     * The analysis timeline, which is each analysis bucket (analyzable segment) within the analysis
     * window.
     * @return A timeline of the analysis window, broken down into buckets.
     */
    @Nonnull
    TimeSeries<TimeInterval> analysisTimeline();

    /**
     * Constructs and returns a builder instance.
     * @return A newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link AggregateAnalysisDemand}.
     */
    class Builder extends ImmutableAggregateAnalysisDemand.Builder {}
}
