package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageEntityPreference;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;

/**
 * An implementation of {@link CoverageEntityPreference}, in which recommendation candidate demand
 * is preferred ahead of coverage demand (demand considered for coverage analysis, but not for a
 * potential recommendation). The intent is to be conservative in cases where the application of a
 * cloud commitment is ambiguous. Where cloud provider rules do not stipulate whether a commitment
 * would be applied to demand A or demand B and demand A is a recommendation candidate, this
 * preference logic will choose demand A.
 */
public class AggregateDemandPreference implements CoverageEntityPreference {

    private final BiMap<Long, AggregateCloudTierDemand> aggregateDemandMap;

    private final BiMap<AggregateCloudTierDemand, Long> aggregateDemandIDMap;

    private AggregateDemandPreference(@Nonnull AnalysisCoverageTopology coverageTopology) {
        this.aggregateDemandMap = ImmutableBiMap.<Long, AggregateCloudTierDemand>builder()
                .putAll(coverageTopology.getAggregatedDemandById())
                .build();
        this.aggregateDemandIDMap = aggregateDemandMap.inverse();
    }

    /**
     * {@inheritDoc}.
     */
    @Nonnull
    @Override
    public Iterable<Long> sortEntities(@Nonnull final ReservedInstanceCoverageJournal coverageJournal,
                                        @Nonnull final Set<Long> entityOids) {

        final Collection<AggregateCloudTierDemand> aggregateDemandSet =
                Maps.filterKeys(aggregateDemandMap, entityOids::contains).values();

        final Comparator<AggregateCloudTierDemand> demandSizeComparator =
                Comparator.comparingDouble(AggregateCloudTierDemand::demandAmount)
                        .reversed();
        final Comparator<AggregateCloudTierDemand> demandComparator =
                Comparator.comparing(AggregateCloudTierDemand::isRecommendationCandidate, Boolean::compareTo)
                        .reversed()
                        .thenComparing(demandSizeComparator)
                        .thenComparingLong(AggregateCloudTierDemand::hashCode);

        return aggregateDemandSet.stream()
                .sorted(demandComparator)
                .map(aggregateDemandIDMap::get)
                // Immutable set maintains insertion order
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * A factory class for creating {@link AggregateDemandPreference} instances.
     */
    public static class AggregateDemandPreferenceFactory {

        /**
         * Constucts a new {@link AggregateDemandPreference} instance, based on the provided
         * {@code coverageTopology}.
         * @param coverageTopology The {@link AnalysisCoverageTopology} for the coverage analysis.
         *                         The coverage topology will be responsible for exposing the analyzed
         *                         CCA demand.
         * @return The newly constructed {@link AggregateDemandPreference} instance.
         */
        @Nonnull
        public AggregateDemandPreference newPreference(@Nonnull AnalysisCoverageTopology coverageTopology) {
            return new AggregateDemandPreference(coverageTopology);
        }
    }
}
