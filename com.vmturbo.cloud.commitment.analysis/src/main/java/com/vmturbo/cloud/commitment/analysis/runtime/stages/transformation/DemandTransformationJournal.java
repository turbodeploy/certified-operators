package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.time.Duration;
import java.util.Collections;
import java.util.DoubleSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Table;
import com.google.common.collect.Tables;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.runtime.data.DoubleStatistics;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.ClassifiedEntityDemandAggregate.DemandTimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;

/**
 * A journal for recording aggregated entity demand.
 */
public class DemandTransformationJournal {

    private final Logger logger = LogManager.getLogger();

    private final AtomicLong skippedEntityCount = new AtomicLong(0);

    private final ConcurrentMap<TimeInterval, DoubleSummaryStatistics> demandStatByBucket =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<AggregateCloudTierDemand, DoubleSummaryStatistics>
            demandStatsByScope = new ConcurrentHashMap<>();

    private Table<TimeInterval, AggregateCloudTierDemand, EntityDemand>
            scopedDemandByBucket = Tables.synchronizedTable(HashBasedTable.create());

    private final AtomicReference<Duration> skippedDemandDuration = new AtomicReference<>(Duration.ZERO);

    private final DoubleSummaryStatistics recommendedDemandAmount = new DoubleSummaryStatistics();

    private final DoubleSummaryStatistics analysisDemandAmount = new DoubleSummaryStatistics();

    private final ReadWriteLock demandLock = new ReentrantReadWriteLock();

    /**
     * Constructs a new journal.
     */
    private DemandTransformationJournal() {}

    /**
     * Records entity demand qualified by an analysis bucket and scope.
     * @param analysisBucket The analysis bucket of the entity demand.
     * @param demandScope The scope of the entity demand.
     * @param entityInfo The entity info associated with the demand.
     * @param demandAmount The demand amount.
     */
    public void recordAggregateDemand(@Nonnull TimeInterval analysisBucket,
                                      @Nonnull AggregateCloudTierDemand demandScope,
                                      @Nonnull EntityInfo entityInfo,
                                      double demandAmount) {

        Preconditions.checkArgument(demandAmount >= 0.0);

        demandLock.readLock().lock();
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("Recording entity demand (Entity OID={}, Bucket={}, Scope={}, Amount={})",
                        entityInfo.entityOid(), analysisBucket, demandScope, demandAmount);
            }

            scopedDemandByBucket.row(analysisBucket)
                    .computeIfAbsent(demandScope, (s) -> EntityDemand.newDemand())
                    .addDemand(entityInfo, demandAmount);

            // record stats
            demandStatByBucket.computeIfAbsent(analysisBucket, (b) -> new DoubleSummaryStatistics())
                    .accept(demandAmount);
            demandStatsByScope.computeIfAbsent(demandScope, (s) -> new DoubleSummaryStatistics())
                    .accept(demandAmount);
            analysisDemandAmount.accept(demandAmount);

            if (demandScope.isRecommendationCandidate()) {
                recommendedDemandAmount.accept(demandAmount);
            }
        } finally {
            demandLock.readLock().unlock();
        }
    }

    /**
     * Records a skipped entity. An entity may be skipped for a number of reason, the most common of
     * which would be the entity state (skipping suspended/terminated instances).
     * @param entityAggregate The {@link ClassifiedEntityDemandAggregate} being skipped.
     */
    public void recordSkippedEntity(@Nonnull ClassifiedEntityDemandAggregate entityAggregate) {

        Preconditions.checkNotNull(entityAggregate);

        logger.debug("Skipping entity with empty demand (Entity OID={})", entityAggregate.entityOid());
        skippedEntityCount.incrementAndGet();
    }

    /**
     * Records skipped demand for a specific entity. Demand may be skipped, if the classification
     * is not selected for analysis.
     * @param entityAggregate The entity whose demand is being skipped.
     * @param skippedClassification The skipped demand classification.
     * @param skippedDemand The set of skipped demand series.
     */
    public void recordSkippedDemand(@Nonnull ClassifiedEntityDemandAggregate entityAggregate,
                                    @Nonnull DemandClassification skippedClassification,
                                    @Nonnull Set<DemandTimeSeries> skippedDemand) {

        Preconditions.checkNotNull(entityAggregate);
        Preconditions.checkNotNull(skippedClassification);
        Preconditions.checkNotNull(skippedDemand);

        final Duration aggregateDuration = skippedDemand.stream()
                .map(DemandTimeSeries::duration)
                .reduce(Duration.ZERO, Duration::plus);

        logger.trace("Recording skipped demand for entity (Entity OID={}, Classification={}, Duration={})",
                entityAggregate.entityOid(), skippedClassification, aggregateDuration);
        skippedDemandDuration.updateAndGet(aggregateDuration::plus);
    }

    /**
     * Collects all recorded demand into {@link DemandTransformationResult}.
     * @return A {@link DemandTransformationResult}, containing recorded aggregated demand and stats
     * on the transformation pipeline.
     */
    @Nonnull
    public DemandTransformationResult getTransformationResult() {

        demandLock.writeLock().lock();
        try {
            return DemandTransformationResult.builder()
                    .aggregateDemandByBucket(buildAggregateDemandSeries())
                    .transformationStats(collectTransformationStats())
                    .build();
        } finally {
            demandLock.writeLock().unlock();
        }
    }

    /**
     * Constructs and returns a new {@link DemandTransformationJournal} instance.
     * @return The newly constructed {@link DemandTransformationJournal} instance.
     */
    public static DemandTransformationJournal newJournal() {
        return new DemandTransformationJournal();
    }

    /**
     * The result of demand aggregation.
     */
    @Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
    @Immutable
    public interface DemandTransformationResult {

        /**
         * Statistics about the demand transformation, including the number of entities recorded and
         * aggregate demand records generated.
         * @return The demand aggregation statistics.
         */
        @Nonnull
        DemandTransformationStatistics transformationStats();

        /**
         * The aggregated demand sets, indexed by the analysis buckets.
         * @return The aggregated demand segments, indexed by the analysis buckets.
         */
        @Nonnull
        Map<TimeInterval, AggregateDemandSegment> aggregateDemandByBucket();

        /**
         * Constructs and returns a builder instance.
         * @return A newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for {@link DemandTransformationResult}.
         */
        class Builder extends ImmutableDemandTransformationResult.Builder {}
    }


    private Map<TimeInterval, AggregateDemandSegment> buildAggregateDemandSeries() {
        return scopedDemandByBucket.rowMap().entrySet()
                .stream()
                .map(timeEntry ->
                        AggregateDemandSegment.builder()
                                .timeInterval(timeEntry.getKey())
                                .addAllAggregateCloudTierDemand(timeEntry.getValue().entrySet()
                                        .stream()
                                        // Add all the recorded entity demand to the aggregate scope
                                        .map(scopeEntry ->
                                                scopeEntry.getKey().toBuilder()
                                                        .putAllDemandByEntity(scopeEntry.getValue().demandMap())
                                                        .build())
                                        .collect(Collectors.toList()))
                                .build())
                .collect(ImmutableMap.toImmutableMap(
                        AggregateDemandSegment::timeInterval,
                        Function.identity()));
    }

    private DemandTransformationStatistics collectTransformationStats() {

        final Set<EntityInfo> aggregatedEntities = scopedDemandByBucket.values()
                .stream()
                .map(EntityDemand::demandMap)
                .map(Map::keySet)
                .flatMap(Set::stream)
                .collect(Collectors.toSet());

        return DemandTransformationStatistics.builder()
                .skippedEntitiesCount(skippedEntityCount.get())
                .aggregatedEntitiesCount(aggregatedEntities.size())
                .uniqueAggregateDemand(demandStatsByScope.keySet().size())
                .skippedDemandDuration(skippedDemandDuration.get())
                .analysisAmount(DoubleStatistics.fromDoubleSummary(analysisDemandAmount))
                .recommendationAmount(DoubleStatistics.fromDoubleSummary(recommendedDemandAmount))
                .demandStatsByTierScope(demandStatsByScope.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                Map.Entry::getKey,
                                (e) -> DoubleStatistics.fromDoubleSummary(e.getValue()))))
                .demandStatsByBucket(demandStatByBucket.entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                Map.Entry::getKey,
                                (e) -> DoubleStatistics.fromDoubleSummary(e.getValue()))))
                .build();
    }

    /**
     * A wrapper around an entity demand map, providing utility methods for recording entity demand.
     */
    private static final class EntityDemand {

        private final ConcurrentMap<EntityInfo, Double> demandByEntity =
                new ConcurrentHashMap<>();


        public void addDemand(EntityInfo entityInfo, double demandAmount) {
            demandByEntity.compute(entityInfo, (k, currentAmount) ->
                    currentAmount == null ? demandAmount : currentAmount + demandAmount);
        }

        public Map<EntityInfo, Double> demandMap() {
            return Collections.unmodifiableMap(demandByEntity);
        }

        public static EntityDemand newDemand() {
            return new EntityDemand();
        }
    }
}


