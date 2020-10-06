package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AggregateDemandPreference.AggregateDemandPreferenceFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand.EntityInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandClassification;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageJournal;

public class AggregateDemandPreferenceTest {

    private final AnalysisCoverageTopology coverageTopology = mock(AnalysisCoverageTopology.class);

    private final ReservedInstanceCoverageJournal coverageJournal = mock(ReservedInstanceCoverageJournal.class);

    private final AggregateDemandPreferenceFactory preferenceFactory =
            new AggregateDemandPreferenceFactory();

    /**
     * Verifies recommendation candidate demand is preferred over other demand.
     */
    @Test
    public void testRecommendationDemandPreference() {

        // setup the aggregate demand
        final AggregateCloudTierDemand recommendationDemand = AggregateCloudTierDemand.builder()
                .accountOid(1)
                .regionOid(2)
                .serviceProviderOid(3)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(4)
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
                .isRecommendationCandidate(true)
                .build();
        final AggregateCloudTierDemand coverageDemand = recommendationDemand.toBuilder()
                .accountOid(5)
                .isRecommendationCandidate(false)
                .build();

        // setup the coverage topology
        final Map<Long, AggregateCloudTierDemand> aggregateDemandMap = ImmutableMap.of(
                6L, recommendationDemand,
                7L, coverageDemand);
        when(coverageTopology.getAggregatedDemandById()).thenReturn(aggregateDemandMap);

        // create the demand preference
        final AggregateDemandPreference demandPreference = preferenceFactory.newPreference(coverageTopology);

        // invoke the preference
        final Iterable<Long> sortedOids = demandPreference.sortEntities(
                coverageJournal,
                ImmutableSet.of(6L, 7L));

        // ASSERTIONS
        assertThat(Iterables.size(sortedOids), equalTo(2));
        assertThat(sortedOids, IsIterableContainingInOrder.contains(6L, 7L));
    }

    /**
     * Verifies larger demand is preferenced over smaller demand.
     */
    @Test
    public void testDemandSizePreference() {

        // setup the aggregate demand
        final AggregateCloudTierDemand largeDemand = AggregateCloudTierDemand.builder()
                .accountOid(1)
                .regionOid(2)
                .serviceProviderOid(3)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(4)
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .classification(DemandClassification.of(AllocatedDemandClassification.ALLOCATED))
                .isRecommendationCandidate(true)
                .putDemandByEntity(EntityInfo.builder().entityOid(8).build(), 10.0)
                .build();
        final AggregateCloudTierDemand smallDemand = largeDemand.toBuilder()
                .accountOid(5)
                .demandByEntity(ImmutableMap.of(
                        EntityInfo.builder().entityOid(9).build(), 1.0))
                .build();

        // setup the coverage topology
        final Map<Long, AggregateCloudTierDemand> aggregateDemandMap = ImmutableMap.of(
                6L, largeDemand,
                7L, smallDemand);
        when(coverageTopology.getAggregatedDemandById()).thenReturn(aggregateDemandMap);

        // create the demand preference
        final AggregateDemandPreference demandPreference = preferenceFactory.newPreference(coverageTopology);

        // invoke the preference
        final Iterable<Long> sortedOids = demandPreference.sortEntities(
                coverageJournal,
                ImmutableSet.of(6L, 7L));

        // ASSERTIONS
        assertThat(Iterables.size(sortedOids), equalTo(2));
        assertThat(sortedOids, IsIterableContainingInOrder.contains(6L, 7L));
    }
}
