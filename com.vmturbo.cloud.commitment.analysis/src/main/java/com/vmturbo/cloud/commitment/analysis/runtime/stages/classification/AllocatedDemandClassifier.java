package com.vmturbo.cloud.commitment.analysis.runtime.stages.classification;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.cloud.commitment.analysis.demand.CloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;

/**
 * A classifier of allocated {@link CloudTierDemand}. The possible classifications of the demand
 * are enumerated in {@link AllocatedDemandClassification}.
 */
public class AllocatedDemandClassifier {

    private final CloudTierFamilyMatcher familyMatcher;

    private final long minimumUptimeMillis;

    private AllocatedDemandClassifier(@Nonnull CloudTierFamilyMatcher familyMatcher,
                                      long minimumUptimeMillis) {
        this.familyMatcher = Objects.requireNonNull(familyMatcher);
        this.minimumUptimeMillis = minimumUptimeMillis;
    }

    /**
     * Classifies the demand represented in {@code entityDemandSeries}, with the assumption that the
     * demand contained within the time series is associated with a single entity. The classification
     * will be based on the assumption that the last/latest mapping is the current allocation of the entity.
     *
     * @param entityDemandSeries A {@link TimeSeries} of {@link EntityCloudTierMapping} instances, in which
     *                           it is assumed all mappings are associated with a single entity.
     * @return A map of {@link AllocatedDemandClassification} to the set of {@link DemandTimeSeries}. A set
     * is returned, because it is possible to have two distinct {@link CloudTierDemand} instances with
     * the same classification (e.g. two instance sizes may both be classified as
     * {@link AllocatedDemandClassification#FLEXIBLY_ALLOCATED}).
     */
    @Nonnull
    public ClassifiedCloudTierDemand classifyEntityDemand(
            @Nonnull TimeSeries<EntityCloudTierMapping> entityDemandSeries) {

        if (entityDemandSeries.isEmpty()) {
            return ClassifiedCloudTierDemand.EMPTY_CLASSIFICATION;
        }

        final Map<AllocatedDemandClassification, Map<CloudTierDemand, List<TimeInterval>>> classifiedDemand =
                new EnumMap<>(AllocatedDemandClassification.class);
        final EntityCloudTierMapping allocatedMapping = entityDemandSeries.last();

        // Iterate backwards over the mappings, classifying each mapping based on the previous mapping.
        // Once a mapping is classified as stale, any prior mapping will either be classified as either
        // ephemeral or stale (it will be ignored/dropped later on).
        AllocatedDemandClassification priorClassification = AllocatedDemandClassification.ALLOCATED;
        for (EntityCloudTierMapping entityDemand : entityDemandSeries.descendingSet()) {

            final AllocatedDemandClassification demandClassification = classifyEntityMapping(
                    allocatedMapping,
                    priorClassification,
                    entityDemand);

            classifiedDemand.computeIfAbsent(demandClassification, (c) -> Maps.newHashMap())
                    .computeIfAbsent(entityDemand.cloudTierDemand(), (demand) -> Lists.newArrayList())
                        .add(entityDemand.timeInterval());

            // If this demand is classified as ephemeral, we don't carry it forward (backwards) to the next
            // classification. Ephemeral demand is simply ignored.
            priorClassification = (demandClassification != AllocatedDemandClassification.EPHEMERAL)
                    ? demandClassification
                    : priorClassification;
        }


        return ClassifiedCloudTierDemand.builder()
                .classifiedDemand(classifiedDemand.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                (e) -> DemandClassification.builder()
                                        .allocatedClassification(e.getKey())
                                        .build(),
                                (e) -> e.getValue().entrySet()
                                        .stream()
                                        .map(demandSet -> DemandTimeSeries.builder()
                                                .cloudTierDemand(demandSet.getKey())
                                                .demandIntervals(TimeSeries.newTimeline(demandSet.getValue()))
                                                .build())
                                        .collect(ImmutableSet.toImmutableSet()))))
                .allocatedDemand(DemandTimeSeries.builder()
                        .cloudTierDemand(allocatedMapping.cloudTierDemand())
                        .demandIntervals(TimeSeries.singletonTimeline(allocatedMapping.timeInterval()))
                        .build())
                .build();
    }

    private AllocatedDemandClassification classifyEntityMapping(
            @Nonnull EntityCloudTierMapping allocatedMapping,
            @Nonnull AllocatedDemandClassification priorClassification,
            @Nonnull EntityCloudTierMapping targetMapping) {

        // If the mapping does not meet the minimum uptime, it is always classified as ephemeral
        // regardless of the relationship to the latest allocation.
        if (targetMapping.timeInterval().duration().toMillis() >= minimumUptimeMillis) {

            // IF the prior (really the subsequent mapping, based on timestamp) mapping was
            // classified as allocated or flexibly allocated (i.e. the demand has stayed within
            // the scope of a single recommendation), check with the target mapping is
            // also within scope of the same recommendation.
            if (priorClassification == AllocatedDemandClassification.ALLOCATED
                    || priorClassification == AllocatedDemandClassification.FLEXIBLY_ALLOCATED) {

                final CloudTierDemand allocatedDemand = allocatedMapping.cloudTierDemand();
                final CloudTierDemand targetDemand = targetMapping.cloudTierDemand();
                if (targetDemand.equals(allocatedDemand)) {
                    return AllocatedDemandClassification.ALLOCATED;
                } else if (familyMatcher.match(targetMapping, allocatedMapping)) {
                    return AllocatedDemandClassification.FLEXIBLY_ALLOCATED;
                } else {
                    return AllocatedDemandClassification.STALE_ALLOCATED;
                }
            } else {
                return AllocatedDemandClassification.STALE_ALLOCATED;
            }
        } else {
            return AllocatedDemandClassification.EPHEMERAL;
        }
    }

    /**
     * A factory class for creating instances of {@link AllocatedDemandClassifier}.
     */
    public static class AllocatedDemandClassifierFactory {

        /**
         * Creates a new {@link AllocatedDemandClassifier} instance.
         * @param familyMatcher A {@link CloudTierFamilyMatcher}, used to determine whether two distinct
         *                      instances of {@link CloudTierDemand} are flexibly allocated.
         * @param minimumUptimeMillis The minimum uptime required to not be classified as ephemeral demand.
         * @return The newly created instance of {@link AllocatedDemandClassifier}.
         */
        public AllocatedDemandClassifier newClassifier(
                @Nonnull CloudTierFamilyMatcher familyMatcher,
                long minimumUptimeMillis) {
            return new AllocatedDemandClassifier(familyMatcher, minimumUptimeMillis);
        }
    }
}
