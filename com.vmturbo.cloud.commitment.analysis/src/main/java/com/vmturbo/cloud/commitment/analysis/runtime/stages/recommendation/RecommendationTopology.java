package com.vmturbo.cloud.commitment.analysis.runtime.stages.recommendation;

import java.util.NavigableSet;

import javax.annotation.Nonnull;

import com.google.common.collect.SetMultimap;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierInfo;
import com.vmturbo.cloud.commitment.analysis.pricing.RateAnnotatedCommitmentContext;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.common.data.TimeSeriesData;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.immutable.TimeSeriesSortedSetEnabled;

/**
 * Topology data (including demand and relevant rates) corresponding to a single cloud commitment
 * recommendation.
 */
@TimeSeriesSortedSetEnabled
@HiddenImmutableImplementation
@Immutable
public interface RecommendationTopology {

    /**
     * The cloud commitment context, containing the commitment data, along with matched demand scopes.
     * @return The cloud commitment context.
     */
    @Nonnull
    RateAnnotatedCommitmentContext commitmentContext();

    /**
     * The demand segments, containing demand in scope of the {@link #commitmentContext()}.
     * @return The demand segments, sorted by timestamp.
     */
    @Nonnull
    NavigableSet<RecommendationDemandSegment> demandSegments();

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructed builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for constructing {@link RecommendationTopology} instances.
     */
    class Builder extends ImmutableRecommendationTopology.Builder {}

    /**
     * A demand segment, containing cloud tier demand for a single time interval. Typically, the time
     * interval of a demand segment with be 1 hour.
     */
    @HiddenImmutableImplementation
    @Immutable
    interface RecommendationDemandSegment extends TimeSeriesData {

        /**
         * A map of cloud tier info the matching {@link AggregateCloudTierDemand} set. Generally,
         * a {@link ScopedCloudTierInfo} instance will map to multiple {@link AggregateCloudTierDemand}
         * if the demand has multiple classifications or if the analysis considers entity state for
         * coverage calculations, but not recommendations.
         * @return An immutable map of tier demand by info.
         */
        @Nonnull
        SetMultimap<ScopedCloudTierInfo, AggregateCloudTierDemand> aggregateCloudTierDemandSet();

        /**
         * Constructs and returns a new {@link Builder} instance.
         * @return The newly constructed builder instance.
         */
        @Nonnull
        static Builder builder() {
            return new Builder();
        }

        /**
         * A builder class for constructing {@link RecommendationDemandSegment} instances.
         */
        class Builder extends ImmutableRecommendationDemandSegment.Builder {}
    }
}
