package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;
import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection.ClassifiedEntitySelection;
import com.vmturbo.cloud.commitment.analysis.util.TimeCalculator;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.data.TimeSeries;

/**
 * Responsible for collecting entity demand and grouping it into {@link AggregateCloudTierDemand}
 * instances, in which the scope, classification, recommendation candidacy and time are used to group the demand.
 */
public class AggregateDemandCollector {

    private final Logger logger = LogManager.getLogger();

    private final TimeInterval analysisWindow;

    private final BoundedDuration analysisInterval;

    private DemandTransformationJournal transformationJournal;

    private AggregateDemandCollector(@Nonnull DemandTransformationJournal transformationJournal,
                                     @Nonnull TimeInterval analysisWindow,
                                     @Nonnull BoundedDuration analysisInterval) {

        this.transformationJournal = Objects.requireNonNull(transformationJournal);
        this.analysisWindow = Objects.requireNonNull(analysisWindow);
        this.analysisInterval = Objects.requireNonNull(analysisInterval);
    }

    /**
     * Collects the demand contained within the {@code entitySelection}, breaking it down into analysis
     * buckets and grouping it with similar demand.
     *
     * @param entitySelection The {@link ClassifiedEntitySelection} to collect.
     */
    public void collectEntitySelection(@Nonnull ClassifiedEntitySelection entitySelection) {

        final long entityOid = entitySelection.entityOid();

        try {
            if (!entitySelection.demandTimeline().isEmpty()) {

                final AggregateCloudTierDemand demandScope = AggregateCloudTierDemand.builder()
                        .cloudTierInfo(ScopedCloudTierInfo.builder()
                                .accountOid(entitySelection.accountOid())
                                .billingFamilyId(entitySelection.billingFamilyId())
                                .availabilityZoneOid(entitySelection.availabilityZoneOid())
                                .regionOid(entitySelection.regionOid())
                                .serviceProviderOid(entitySelection.serviceProviderOid())
                                .cloudTierDemand(entitySelection.cloudTierDemand())
                                .build())
                        .isRecommendationCandidate(entitySelection.isRecommendationCandidate())
                        .classification(entitySelection.classification())
                        .build();

                // Suspension/termination is kept in entity info as auxiliary data (not uniquely
                // identifying to aggregate cloud tier demand) as the analysis makes no distinction
                // after selection whether an entity is suspended/terminated or not.
                final EntityInfo entityInfo = EntityInfo.builder()
                        .entityOid(entityOid)
                        .isSuspended(entitySelection.isSuspended())
                        .isTerminated(entitySelection.isTerminated())
                        .build();

                convertDemandIntervalsToBucketDemand(entitySelection.demandTimeline())
                        .forEach((analysisInterval, amount) ->
                                transformationJournal.recordAggregateDemand(
                                        analysisInterval,
                                        demandScope,
                                        entityInfo,
                                        amount));
            }
        } catch (Exception e) {
            logger.warn("Exception in collecting aggregate demand for entity (Entity OID={})",
                    entityOid, e);
        }
    }

    /**
     * A factory class for producing {@link AggregateDemandCollector} instances.
     */
    public static class AggregateDemandCollectorFactory {

        /**
         * Constructs a new {@link AggregateDemandCollector} instance.
         *
         * @param transformationJournal  The demand transformation journal, used to record the
         *                               aggregate demand.
         * @param analysisWindow         The analysis window, which is the full start/end time of the analyzed
         *                               demand.
         * @param analysisBucketDuration The analysis buckets i.e. how to break up the analysis window
         *                               into analyzable segments.
         * @return THe newly created {@link AggregateDemandCollector} instance
         */
        @Nonnull
        public AggregateDemandCollector newCollector(
                @Nonnull DemandTransformationJournal transformationJournal,
                @Nonnull TimeInterval analysisWindow,
                @Nonnull BoundedDuration analysisBucketDuration) {

            return new AggregateDemandCollector(
                    transformationJournal,
                    analysisWindow,
                    analysisBucketDuration);
        }
    }

    /**
     * Converts a timeline representing demand to a map, indicating for each analysis bucket how much
     * the demand timeline overlaps. For example, say there are analysis buckets of 1 pm - 2 pm,
     * 2 pm - 3 pm, and 3 pm - 4 pm. The demand timeline has a record from 1:45 pm - 3:30 pm. The returned
     * bucket demand map would indicate .25 for the first analysis bucket (15 minutes out of an hour
     * for 1 pm - 2 pm), 1.0 for the second analysis bucket, and .5 for the third analysis bucket.
     *
     * @param timeSeries The demand timeline to analyze.
     * @return The bucket demand, where the keys correspond to analysis buckets and the value is the
     * amount of demand that fits within that bucket.
     */
    private Map<TimeInterval, Double> convertDemandIntervalsToBucketDemand(
            @Nonnull TimeSeries<TimeInterval> timeSeries) {

        Map<TimeInterval, Double> demandByTimeInterval = new HashMap<>();

        for (TimeInterval demandInterval : timeSeries) {

            final Duration timeFromAnalysisStart = Duration.between(
                    analysisWindow.startTime(), demandInterval.startTime());
            final long analysisBucketIndex = TimeCalculator.flooredDivision(
                    timeFromAnalysisStart, analysisInterval.duration());
            Instant currentBucketStartTime = analysisWindow.startTime()
                    .plus(analysisInterval.duration().multipliedBy(analysisBucketIndex));

            while (currentBucketStartTime.isBefore(demandInterval.endTime())) {

                final TimeInterval currentBucket = TimeInterval.builder()
                        .startTime(currentBucketStartTime)
                        .endTime(currentBucketStartTime.plus(analysisInterval.duration()))
                        .build();
                final Duration overlap = TimeCalculator.overlap(currentBucket, demandInterval);

                // Do not record the interval, if the overlap is zero
                if (overlap.toMillis() > 0) {
                    final double overlapDemand = TimeCalculator.divide(overlap, currentBucket.duration());

                    demandByTimeInterval.put(
                            currentBucket,
                            demandByTimeInterval.getOrDefault(
                                    currentBucket, 0.0) + overlapDemand);
                }

                currentBucketStartTime = currentBucket.endTime();
            }
        }

        return demandByTimeInterval;
    }
}
