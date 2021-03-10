package com.vmturbo.cost.component.savings;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.cost.component.cca.CCATopologyEventProvider;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventFilter;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

public class TopologyEventsPollerTest {
    private EntityEventsJournal store;
    private TopologyEventsPoller tep;

    private ComputeTierAllocationStore computeTierAllocationStore = mock(ComputeTierAllocationStore.class);

    private CloudScopeStore cloudScopeStore = mock(CloudScopeStore.class);

    private CCATopologyEventProvider topologyEventProvider;

    @Before
    public void setup() throws IOException {
        store = new InMemoryEntityEventsJournal();
        topologyEventProvider = new CCATopologyEventProvider(computeTierAllocationStore, cloudScopeStore);
        tep = new TopologyEventsPoller(topologyEventProvider, store);
        when(cloudScopeStore.streamByFilter(any())).thenReturn(Stream.empty());
    }

    /**
     * Test the processTopologyEvents method() called by the poll method.
     */
    @Test
    public void testProcessTopologyEvents() {
        // setup compute tier allocation store
        final Instant endTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
        final TimeInterval eventWindow = TimeInterval.builder()
                        .endTime(endTime)
                        .startTime(Instant.now().plus(-1, ChronoUnit.HOURS))
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

        // poll calls the processTopologyEvents method, and we are verifying that it populates the
        // store correctly.
        final TopologyEvents topologyEvents = topologyEventProvider.getTopologyEvents(eventWindow, TopologyEventFilter.ALL);
        tep.processTopologyEvents(topologyEvents);

        final Map<Long, TopologyEventLedger> topologyEventLedgers = topologyEvents.ledgers();

        final TopologyEventLedger eventLedger = topologyEvents.ledgers().get(1L);
        final Set<TopologyEvent> entityEvents = eventLedger.events();
        List<SavingsEvent> savingsEvents = store.removeAllEvents();
        entityEvents.forEach(entityEvent -> {
            assert (savingsEvents.contains(createSavingsEvent(1L, entityEvent)));
        });
    }

    /**
     * Create a Savings Event on receiving an Action Event.
     *
     * @param entityId The target entity ID.
     * @param topologyEvent The TopologyEvent from which to create the SavingsEvent.
     * @return The SavingsEvent.
     */
    private static SavingsEvent
            createSavingsEvent(final Long entityId, @Nonnull TopologyEvent topologyEvent) {
        final long eventTimestamp = topologyEvent.getEventTimestamp();
        return new SavingsEvent.Builder()
                        .topologyEvent(topologyEvent)
                        .entityId(entityId)
                        .timestamp(eventTimestamp)
                        .build();
    }
}
