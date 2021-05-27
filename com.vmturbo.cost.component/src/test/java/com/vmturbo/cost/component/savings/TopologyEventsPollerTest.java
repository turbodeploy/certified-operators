package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.cca.CCATopologyEventProvider;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventFilter;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

/**
 * Tests for the topology events poller.
 */
public class TopologyEventsPollerTest {
    private EntityEventsJournal store;
    private TopologyEventsPoller tep;

    private final ComputeTierAllocationStore computeTierAllocationStore = mock(ComputeTierAllocationStore.class);

    private final CloudScopeStore cloudScopeStore = mock(CloudScopeStore.class);

    private  CCATopologyEventProvider topologyEventProvider = mock(CCATopologyEventProvider.class);

    private TopologyInfoTracker topologyInfoTracker = mock(TopologyInfoTracker.class);

    private final Long realTimeTopologyContextId = 777777L;

    private static final long ACTION_EXPIRATION_TIME = TimeUnit.HOURS.toMillis(1L);

    /**
     * Pre-test setup.
     *
     * @throws IOException when something goes wrong.
     */
    @Before
    public void setup() throws IOException {
        store = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
        tep = new TopologyEventsPoller(topologyEventProvider, topologyInfoTracker, store);
    }

    /**
     * Test the isTopologyReady() and processTopologyEvents() methods called by the poll method.
     */
    @Test
    public void testTopologyReadinessAndProcessTopologyEvents() {
        final Instant endTime = Instant.now().truncatedTo(ChronoUnit.HOURS);
        final Instant startTime = Instant.now().minus(1, ChronoUnit.HOURS);
        final TimeInterval eventWindow = TimeInterval.builder()
                        .endTime(endTime)
                        .startTime(startTime)
                        .build();

        final Instant creationTime = eventWindow.startTime().plus(5, ChronoUnit.MINUTES);
        final TopologyEvent powerOffEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(creationTime.toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_ON)
                                .setDestinationState(EntityState.POWERED_OFF)
                                .build()))
                .build();
        final TopologyEvent creationEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.RESOURCE_CREATION)
                .setEventTimestamp(creationTime.toEpochMilli())
                .build();
        final TopologyEvent deletionEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.RESOURCE_CREATION)
                .setEventTimestamp(creationTime.plusSeconds(1).toEpochMilli())
                .build();

        final EntityEvents entityEventsProto = EntityEvents.newBuilder()
                .setEntityOid(1)
                .addEvents(deletionEvent)
                .addEvents(powerOffEvent)
                .addEvents(creationEvent)
                .build();

        final TopologyEvents topologyEvents = TopologyEvents.fromEntityEvents(Sets.newHashSet(entityEventsProto));

        // Latest topology broadcast before event window end time.
        final Instant initialTopologyBroadcastTime = endTime.minus(1, ChronoUnit.MINUTES);
        final TopologyInfo topologyInfo1 = TopologyInfo.newBuilder()
                        .setTopologyType(TopologyType.REALTIME)
                        .setTopologyContextId(realTimeTopologyContextId)
                        .setTopologyId(456L)
                        .setCreationTime(initialTopologyBroadcastTime.toEpochMilli())
                        .build();
        when(topologyInfoTracker.getLatestTopologyInfo()).thenReturn(Optional.of(topologyInfo1));
        // poll calls the processTopologyEventsIfReady method, and we are verifying that it
        // populates the events journal correctly.
        // Test after a topology broadcast has happened, but the creation time is 1 min he end time of the event window.
        // There should be no topology events polled, and hence no Savings events generated.
        assertFalse(tep.isTopologyBroadcasted(LocalDateTime.ofInstant(endTime, ZoneOffset.UTC)));

        // Test post another topology broadcast, with latest topology creation time greater than
        // the event window end time.  Events should be processed and corresponding Savings Events
        // created this time.
        final Instant nextTopologyBroadcastTime = endTime.plus(10, ChronoUnit.MINUTES);
        final TopologyInfo topologyInfo2 = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .setTopologyContextId(realTimeTopologyContextId)
                .setTopologyId(456L)
                .setCreationTime(nextTopologyBroadcastTime.toEpochMilli())
                .build();
        when(topologyInfoTracker.getLatestTopologyInfo()).thenReturn(Optional.of(topologyInfo2));
        when(topologyEventProvider.getTopologyEvents(eventWindow, TopologyEventFilter.ALL)).thenReturn(topologyEvents);
        // If isTopologyBroadcasted(endTime) were to return false, it's called recursively by the
        // EntitySavingsProcessor, and the time range adjusted until it returns true, and hence
        // processTopologyEventsIfReady() can be called without checking the return of isTopologyBroadcasted agin.
        assertTrue(tep.isTopologyBroadcasted(LocalDateTime.ofInstant(endTime, ZoneOffset.UTC)));

        tep.processTopologyEventsIfReady(startTime, endTime);
        final Map<Long, TopologyEventLedger> topologyEventLedgers = topologyEvents.ledgers();
        final TopologyEventLedger eventLedger = topologyEvents.ledgers().get(1L);
        final Set<TopologyEvent> entityEvents = eventLedger.events();
        final List<SavingsEvent> savingsEvents = store.removeAllEvents();

        // Appropriate savings events should be created for each topology event added
        // to the events journal.
        entityEvents.forEach(entityEvent -> {
            SavingsEvent event = createSavingsEvent(1L, entityEvent);
            assert (savingsEvents.contains(event));
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
