package com.vmturbo.cloud.commitment.analysis.persistence;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import com.google.common.collect.Sets;

import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.cloud.commitment.analysis.demand.EntityCloudTierMapping;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter;
import com.vmturbo.cloud.commitment.analysis.demand.TimeFilter.TimeComparator;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.EntityComputeTierAllocationFilter;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope.ComputeTierDemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
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
    public void testAllocationDemand() {

        // Setup the input
        final Instant startTime = Instant.now().minus(5, ChronoUnit.DAYS);
        final Instant endTime = startTime.plus(4, ChronoUnit.DAYS);
        final TimeInterval selectionWindow = TimeInterval.builder()
                .startTime(startTime)
                .endTime(endTime)
                .build();

        final DemandScope demandScope = DemandScope.newBuilder()
                .addAllAccountOid(Sets.newHashSet(1L, 2L))
                .addAllEntityOid(Sets.newHashSet(2L, 3L, 4L))
                .addAllCloudTierOid(Sets.newHashSet(8L, 9L))
                .setComputeTierScope(ComputeTierDemandScope.newBuilder()
                        .addAllPlatform(Sets.newHashSet(OSType.RHEL, OSType.SUSE))
                        .addTenancy(Tenancy.DEDICATED))
                .build();

        // Setup expected invocation args for compute tier store
        final EntityComputeTierAllocationFilter allocationFilter = EntityComputeTierAllocationFilter.builder()
                .startTimeFilter(TimeFilter.builder()
                        .time(endTime)
                        .comparator(TimeComparator.BEFORE_OR_EQUAL_TO)
                        .build())
                .endTimeFilter(TimeFilter.builder()
                        .time(startTime)
                        .comparator(TimeComparator.AFTER_OR_EQUAL_TO)
                        .build())
                .addAllAccountOids(demandScope.getAccountOidList())
                .addAllEntityOids(demandScope.getEntityOidList())
                .addAllComputeTierOids(demandScope.getCloudTierOidList())
                .addAllPlatforms(demandScope.getComputeTierScope().getPlatformList())
                .addAllTenancies(demandScope.getComputeTierScope().getTenancyList())
                .build();

        // Setup demand store mocks
        final EntityComputeTierAllocation allocationA = mock(EntityComputeTierAllocation.class);
        final EntityComputeTierAllocation allocationB = mock(EntityComputeTierAllocation.class);

        final List<EntityCloudTierMapping> actualDemandList = new ArrayList<>();
        Consumer<EntityCloudTierMapping> consumer = actualDemandList::add;

        doAnswer(inv -> {
            Consumer<EntityComputeTierAllocation> consumer1 = inv.getArgumentAt(1, Consumer.class);
            consumer1.accept(allocationA);
            consumer1.accept(allocationB);
            return null;
        }).when(computeTierAllocationStore).streamAllocations(eq(allocationFilter), any());

        // Invoke the reader
        cloudCommitmentDemandReader.getAllocationDemand(CloudTierType.COMPUTE_TIER, demandScope,
                selectionWindow, consumer::accept);

        final ArgumentCaptor<EntityComputeTierAllocationFilter> filterCaptor =
                ArgumentCaptor.forClass(EntityComputeTierAllocationFilter.class);
        verify(computeTierAllocationStore).streamAllocations(filterCaptor.capture(), any());
        assertThat(filterCaptor.getValue(), equalTo(allocationFilter));

        assertThat(actualDemandList, hasSize(2));
        assertThat(actualDemandList, containsInAnyOrder(allocationA, allocationB));

    }
}
