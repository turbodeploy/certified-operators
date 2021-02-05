package com.vmturbo.cost.component.cca;

import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.ComputeTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.EntityComputeTierAllocation;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore.EntityComputeTierAllocationSet;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.cloud.common.entity.scope.EntityCloudScope;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ResourceCreationDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventFilter;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventUpdateListener;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

public class CCATopologyEventProviderTest {

    private ComputeTierAllocationStore computeTierAllocationStore = mock(ComputeTierAllocationStore.class);

    private CloudScopeStore cloudScopeStore = mock(CloudScopeStore.class);

    private CCATopologyEventProvider topologyEventProvider;

    @Before
    public void setup() {
        topologyEventProvider = new CCATopologyEventProvider(computeTierAllocationStore, cloudScopeStore);
        when(cloudScopeStore.streamByFilter(any())).thenReturn(Stream.empty());
    }

    @Test
    public void testUpdateListener() {

        final TopologyEventUpdateListener updateListener = mock(TopologyEventUpdateListener.class);

        // Invoke SUT
        topologyEventProvider.registerUpdateListener(updateListener);
        topologyEventProvider.onAllocationUpdate(TopologyInfo.getDefaultInstance());

        // Verify
        verify(updateListener).onTopologyEventUpdate();
    }

    @Test
    public void testBaseComputeAllocation() {

        // setup compute tier allocation store
        final TimeInterval eventWindow = TimeInterval.builder()
                .startTime(Instant.EPOCH)
                .endTime(Instant.EPOCH.plus(1, ChronoUnit.HOURS))
                .build();

        final Instant firstStartTime = eventWindow.startTime().plus(10, ChronoUnit.MINUTES);
        final Instant firstEndTime = eventWindow.startTime().plus(20, ChronoUnit.MINUTES);
        final EntityComputeTierAllocation firstAllocation = EntityComputeTierAllocation.builder()
                .entityOid(1)
                .accountOid(2)
                .regionOid(3)
                .serviceProviderOid(4)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5)
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .timeInterval(TimeInterval.builder()
                        .startTime(firstStartTime)
                        .endTime(firstEndTime)
                        .build())
                .build();

        final Instant secondStartTime = eventWindow.startTime().plus(30, ChronoUnit.MINUTES);
        final Instant secondEndTime = eventWindow.startTime().plus(45, ChronoUnit.MINUTES);
        final EntityComputeTierAllocation secondAllocation = firstAllocation.toBuilder()
                .timeInterval(TimeInterval.builder()
                        .startTime(secondStartTime)
                        .endTime(secondEndTime)
                        .build())
                .build();

        final EntityComputeTierAllocationSet allocationSet = EntityComputeTierAllocationSet.builder()
                .putAllocation(firstAllocation.entityOid(), firstAllocation)
                .putAllocation(secondAllocation.entityOid(), secondAllocation)
                .build();
        when(computeTierAllocationStore.getAllocations(any())).thenReturn(allocationSet);

        // Invoke SUT
        final TopologyEvents topologyEvents =
                topologyEventProvider.getTopologyEvents(eventWindow, TopologyEventFilter.ALL);

        // asserts
        assertThat(topologyEvents.ledgers(), hasKey(1L));
        final TopologyEventLedger eventLedger = topologyEvents.ledgers().get(1L);
        assertThat(eventLedger.events(), containsInRelativeOrder(
                powerOnEvent(firstStartTime), powerOffEvent(firstEndTime),
                powerOnEvent(secondStartTime), powerOffEvent(secondEndTime)));

    }

    @Test
    public void testCreationEvent() {

        // setup compute tier allocation store
        final TimeInterval eventWindow = TimeInterval.builder()
                .startTime(Instant.EPOCH)
                .endTime(Instant.EPOCH.plus(1, ChronoUnit.HOURS))
                .build();

        final Instant firstStartTime = eventWindow.startTime().plus(10, ChronoUnit.MINUTES);
        final Instant firstEndTime = eventWindow.startTime().plus(20, ChronoUnit.MINUTES);
        final EntityComputeTierAllocation firstAllocation = EntityComputeTierAllocation.builder()
                .entityOid(1)
                .accountOid(2)
                .regionOid(3)
                .serviceProviderOid(4)
                .cloudTierDemand(ComputeTierDemand.builder()
                        .cloudTierOid(5)
                        .osType(OSType.LINUX)
                        .tenancy(Tenancy.DEFAULT)
                        .build())
                .timeInterval(TimeInterval.builder()
                        .startTime(firstStartTime)
                        .endTime(firstEndTime)
                        .build())
                .build();

        final EntityComputeTierAllocationSet allocationSet = EntityComputeTierAllocationSet.builder()
                .putAllocation(firstAllocation.entityOid(), firstAllocation)
                .build();
        when(computeTierAllocationStore.getAllocations(any())).thenReturn(allocationSet);

        // setup the cloud scope store
        final Instant creationTime = eventWindow.startTime().plus(5, ChronoUnit.MINUTES);
        final EntityCloudScope entityCloudScope = EntityCloudScope.builder()
                .entityOid(firstAllocation.entityOid())
                .accountOid(firstAllocation.accountOid())
                .regionOid(firstAllocation.regionOid())
                .serviceProviderOid(firstAllocation.regionOid())
                .creationTime(creationTime)
                .build();
        when(cloudScopeStore.streamByFilter(any())).thenReturn(Stream.of(entityCloudScope));

        // Invoke SUT
        final TopologyEvents topologyEvents =
                topologyEventProvider.getTopologyEvents(eventWindow, TopologyEventFilter.ALL);

        // Asserts
        final TopologyEvent expectedCreationEvent = TopologyEvent.newBuilder()
                .setEventTimestamp(creationTime.toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setResourceCreation(ResourceCreationDetails.getDefaultInstance()))
                .build();
        assertThat(topologyEvents.ledgers(), hasKey(1L));
        final TopologyEventLedger eventLedger = topologyEvents.ledgers().get(1L);
        assertThat(eventLedger.events(), containsInRelativeOrder(expectedCreationEvent));
    }

    private TopologyEvent powerOnEvent(@Nonnull Instant timestamp) {
        return TopologyEvent.newBuilder()
                .setEventTimestamp(timestamp.toEpochMilli())
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.UNKNOWN)
                                .setDestinationState(EntityState.POWERED_ON)
                                .build())
                        .build())
                .build();
    }

    private TopologyEvent powerOffEvent(@Nonnull Instant timestamp) {
        return TopologyEvent.newBuilder()
                .setEventTimestamp(timestamp.toEpochMilli())
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_ON)
                                .setDestinationState(EntityState.UNKNOWN)
                                .build())
                        .build())
                .build();
    }
}
