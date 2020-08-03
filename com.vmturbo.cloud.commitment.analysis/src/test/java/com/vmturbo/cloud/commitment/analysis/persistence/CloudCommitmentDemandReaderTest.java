package com.vmturbo.cloud.commitment.analysis.persistence;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.ImmutableTimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.commitment.analysis.demand.store.ImmutableEntityComputeTierAllocationFilter;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope.ComputeTierDemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.DemandSegment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Testing the CloudCommitmentDemandReader class and its methods.
 */
public class CloudCommitmentDemandReaderTest {

    private final ComputeTierAllocationStore computeTierAllocationStore = mock(ComputeTierAllocationStore.class);

    private final CloudCommitmentDemandReader cloudCommitmentDemandReader =
            new CloudCommitmentDemandReaderImpl(computeTierAllocationStore);

    /**
     * Testing multiple demand segments associated with demand.
     */
    @Test
    public void testMultipleDemandSegments() {

        // Setup the input
        final Instant startTime = Instant.now().minus(5, ChronoUnit.DAYS);

        final DemandScope demandScopeA = DemandScope.newBuilder()
                .addAllAccountOid(Sets.newHashSet(1L, 2L))
                .addAllEntityOid(Sets.newHashSet(2L, 3L, 4L))
                .build();

        final DemandSegment demandSegmentA = DemandSegment.newBuilder()
                .setScope(demandScopeA)
                .setDemandType(HistoricalDemandType.ALLOCATION)
                .build();

        final ComputeTierDemandScope computeTierDemandScopeB = ComputeTierDemandScope.newBuilder()
                .addAllComputeTierOid(Sets.newHashSet(8L, 9L))
                .addAllPlatform(Sets.newHashSet(OSType.RHEL, OSType.SUSE))
                .addTenancy(Tenancy.DEDICATED)
                .build();
        final DemandScope demandScopeB = DemandScope.newBuilder()
                .addAllRegionOid(Sets.newHashSet(5L, 6L, 7L))
                .setComputeTierScope(computeTierDemandScopeB)
                .build();

        final DemandSegment demandSegmentB = DemandSegment.newBuilder()
                .setScope(demandScopeB)
                .setDemandType(HistoricalDemandType.ALLOCATION)
                .build();

        // Setup expected invocation args for compute tier store
        final EntityComputeTierAllocationFilter filterA = ImmutableEntityComputeTierAllocationFilter.builder()
                .endTimeFilter(ImmutableTimeFilter.builder()
                        .time(startTime)
                        .comparator(TimeComparator.AFTER_OR_EQUAL_TO)
                        .build())
                .addAllAccountOids(demandScopeA.getAccountOidList())
                .addAllEntityOids(demandScopeA.getEntityOidList())
                .build();

        final EntityComputeTierAllocationFilter filterB = ImmutableEntityComputeTierAllocationFilter.builder()
                .endTimeFilter(ImmutableTimeFilter.builder()
                        .time(startTime)
                        .comparator(TimeComparator.AFTER_OR_EQUAL_TO)
                        .build())
                .addAllRegionOids(demandScopeB.getRegionOidList())
                .addAllComputeTierOids(demandScopeB.getComputeTierScope().getComputeTierOidList())
                .addAllPlatforms(demandScopeB.getComputeTierScope().getPlatformList())
                .addAllTenancies(demandScopeB.getComputeTierScope().getTenancyList())
                .build();

        // Setup demand store mocks
        final EntityComputeTierAllocation allocationA = mock(EntityComputeTierAllocation.class);
        final EntityComputeTierAllocation allocationB = mock(EntityComputeTierAllocation.class);
        when(computeTierAllocationStore.streamAllocations(eq(filterA))).thenAnswer((f) -> Stream.of(allocationA, allocationB));

        final EntityComputeTierAllocation allocationC = mock(EntityComputeTierAllocation.class);
        final EntityComputeTierAllocation allocationD = mock(EntityComputeTierAllocation.class);
        when(computeTierAllocationStore.streamAllocations(eq(filterB))).thenAnswer((f) -> Stream.of(allocationC, allocationD));

        // Invoke the reader
        final Stream<EntityCloudTierMapping> actualDemandStream = cloudCommitmentDemandReader.getDemand(
                CloudTierType.COMPUTE_TIER,
                Lists.newArrayList(demandSegmentA, demandSegmentB),
                startTime);

        final Set<EntityCloudTierMapping> actualDemand = actualDemandStream.collect(Collectors.toSet());

        final ArgumentCaptor<EntityComputeTierAllocationFilter> filterCaptor =
                ArgumentCaptor.forClass(EntityComputeTierAllocationFilter.class);
        verify(computeTierAllocationStore, times(2)).streamAllocations(filterCaptor.capture());

        final List<EntityComputeTierAllocationFilter> actualFilters = filterCaptor.getAllValues();

        assertThat(actualFilters, hasSize(2));
        assertThat(actualFilters, containsInAnyOrder(filterA, filterB));

        assertThat(actualDemand, hasSize(4));
        assertThat(actualDemand, containsInAnyOrder(allocationA, allocationB, allocationC, allocationD));

    }
}
