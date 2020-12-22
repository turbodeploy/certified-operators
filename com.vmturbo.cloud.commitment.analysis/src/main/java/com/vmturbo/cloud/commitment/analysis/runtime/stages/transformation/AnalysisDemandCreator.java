package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.DemandTransformationJournal.DemandTransformationResult;
import com.vmturbo.cloud.common.data.ImmutableTimeSeries;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;

/**
 * An aggregator responsible for converting all entity demand to an aggregated type, based on the scope,
 * classification, and timestamp of the demand. This class is responsible for producing {@link AggregateAnalysisDemand}.
 */
public class AnalysisDemandCreator {

    private final DemandTransformationResult transformationResult;

    private final TimeInterval analysisWindow;

    private final BoundedDuration analysisBucket;

    private AnalysisDemandCreator(@Nonnull DemandTransformationResult transformationResult,
                                  @Nonnull TimeInterval analysisWindow,
                                  @Nonnull BoundedDuration analysisBucket) {

        this.transformationResult = Objects.requireNonNull(transformationResult);
        this.analysisWindow = Objects.requireNonNull(analysisWindow);
        this.analysisBucket = Objects.requireNonNull(analysisBucket);
    }

    /**
     * Constructs an {@link AggregateAnalysisDemand} instance from the provided demand transformation
     * result and analysis configuration.
     * @param transformationResult THe demand transformation result.
     * @param analysisWindow The full start/end timestamps of the analysis.
     * @param analysisBucketDuration The duration of each segment of the analysis.
     * @return The newly created {@link AggregateAnalysisDemand} instance.
     */
    @Nonnull
    public static AggregateAnalysisDemand createAnalysisDemand(
            @Nonnull DemandTransformationResult transformationResult,
            @Nonnull TimeInterval analysisWindow,
            @Nonnull BoundedDuration analysisBucketDuration) {

        final AnalysisDemandCreator analysisDemandCreator = new AnalysisDemandCreator(
                transformationResult, analysisWindow, analysisBucketDuration);
        return analysisDemandCreator.createAnalysisDemand();
    }

    private AggregateAnalysisDemand createAnalysisDemand() {
        final TimeSeries<TimeInterval> analysisTimeline = calculateAnalysisBuckets();
        final Map<TimeInterval, AggregateDemandSegment> demandByBucket =
                transformationResult.aggregateDemandByBucket();

        return AggregateAnalysisDemand.builder()
                .analysisTimeline(analysisTimeline)
                .aggregateDemandSeries(analysisTimeline.stream()
                        .map(analysisBucket -> demandByBucket.getOrDefault(
                                analysisBucket, AggregateDemandSegment.emptySegment(analysisBucket)))
                        .collect(ImmutableTimeSeries.toImmutableTimeSeries()))
                .build();
    }


    private TimeSeries<TimeInterval> calculateAnalysisBuckets() {

        final ImmutableTimeSeries.Builder<TimeInterval> analysisIntervals = ImmutableTimeSeries.builder();
        final Instant analysisEndTime = analysisWindow.endTime();

        Instant currentStartTime = analysisWindow.startTime();
        while (currentStartTime.isBefore(analysisEndTime)) {

            final Instant endTime = currentStartTime.plus(analysisBucket.duration());
            analysisIntervals.add(
                    TimeInterval.builder()
                            .startTime(currentStartTime)
                            .endTime(endTime)
                            .build());

            currentStartTime = endTime;
        }

        return analysisIntervals.build();
    }
}
