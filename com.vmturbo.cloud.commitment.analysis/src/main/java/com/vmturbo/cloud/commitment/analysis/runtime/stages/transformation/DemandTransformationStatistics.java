package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation;

import java.time.Duration;
import java.util.Map;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.runtime.data.DoubleStatistics;
import com.vmturbo.cloud.common.data.TimeInterval;

/**
 * Contains statistics collected during demand transformation.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface DemandTransformationStatistics {

    /**
     * An empty set of transformation statistics.
     */
    DemandTransformationStatistics EMPTY_STATS = DemandTransformationStatistics.builder()
            .skippedEntitiesCount(0)
            .uniqueAggregateDemand(0)
            .aggregatedEntitiesCount(0)
            .build();

    /**
     * The number of entities skipped, indicating no demand was recorded for the entity.
     * @return The number of entities skipped.
     */
    long skippedEntitiesCount();

    /**
     * The number of unique {@link AggregateCloudTierDemand} instances recorded during demand aggregation.
     * @return The number of unique {@link AggregateCloudTierDemand} instances.
     */
    long uniqueAggregateDemand();

    /**
     * The number of entities aggregated.
     * @return The number of entities aggregated.
     */
    long aggregatedEntitiesCount();

    /**
     * Represents the total duration of skipped demand. Duration may be skipped (dropped from the full
     * analysis) based on it's classification (e.g. ignoring stale demand). Skipped demand does not contain
     * demand from skipped entities.
     * @return The total duration of all skipped demand.
     */
    @Default
    @Nonnull
    default Duration skippedDemandDuration() {
        return Duration.ZERO;
    }

    /**
     * The amount (relative to the analysis bucket duration) of demand considered for coverage analysis.
     * @return The amount (relative to the analysis bucket duration) of demand considered for coverage analysis.
     */
    @Default
    @Nonnull
    default DoubleStatistics analysisAmount() {
        return DoubleStatistics.EMPTY_STATISTICS;
    }

    /**
     * The amount (relative to the analysis bucket duration) of demand considered for recommendation analysis.
     * This is a subset of {@link #analysisAmount()}.
     * @return The amount (relative to the analysis bucket duration) of demand considered for recommendation analysis.
     */
    @Default
    @Nonnull
    default DoubleStatistics recommendationAmount() {
        return DoubleStatistics.EMPTY_STATISTICS;
    }

    /**
     * Demand stats scoped by the unique aggregated cloud tier demand. Demand stats will correspond to the amount
     * of the cloud tier demand contained within {@link AggregateCloudTierDemand}. For example, if an
     * {@link AggregateCloudTierDemand} instance is associated with a t2.medium compute tier and the
     * demand statistics indicate an average of 2.0, this indicates on average there are 2 t2.medium
     * instances powered on for the associated scope over all analysis buckets.
     * @return Demand stats scoped by the unique aggregated cloud tier demand.
     */
    @Nonnull
    Map<AggregateCloudTierDemand, DoubleStatistics> demandStatsByTierScope();

    /**
     * Demand stats scoped by the analysis buckets. Only analysis buckets for which there was aggregated
     * demand will be present in the returned map.
     * @return Demand stats scoped by the analysis buckets.
     */
    @Nonnull
    Map<TimeInterval, DoubleStatistics> demandStatsByBucket();

    /**
     * Checks whether the statistics are empty.
     * @return true, if no unique aggregated demand nor skipped entities were recorded. False, otherwise.
     */
    default boolean isEmpty() {
        return uniqueAggregateDemand() == 0L && skippedEntitiesCount() == 0L;
    }

    /**
     * Constructs and returns a builder instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link DemandTransformationStatistics}.
     */
    class Builder extends ImmutableDemandTransformationStatistics.Builder {}
}
