package com.vmturbo.cost.component.savings.bottomup;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cloud.commitment.analysis.demand.store.ComputeTierAllocationStore;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.entity.scope.CloudScopeStore;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.cca.CCATopologyEventProvider;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent.EventType;
import com.vmturbo.cost.component.topology.TopologyInfoTracker;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.test.utils.FeatureFlagTestRule;
import com.vmturbo.topology.event.library.TopologyEventProvider.TopologyEventFilter;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

/**
 * Tests for the topology events poller.  Run the tests once with TEM disabled and once with
 * TEM enabled.
 */
@RunWith(Parameterized.class)
public class TopologyEventsPollerTest {

    /**
     * Parameterized test data.
     *
     * @return whether to enable TEM.
     */
    @Parameters(name = "{index}: Test with enable TEM = {0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {{true}, {false}};
        return Arrays.asList(data);
    }

    /**
     * Test parameter.
     */
    @Parameter(0)
    public boolean enableTEM;

    private EntityEventsJournal store;
    private TopologyEventsPoller tep;

    private final ComputeTierAllocationStore computeTierAllocationStore = mock(ComputeTierAllocationStore.class);

    private final CloudScopeStore cloudScopeStore = mock(CloudScopeStore.class);

    private  CCATopologyEventProvider topologyEventProvider = mock(CCATopologyEventProvider.class);

    private TopologyInfoTracker topologyInfoTracker = mock(TopologyInfoTracker.class);

    private final Long realTimeTopologyContextId = 777777L;

    private static final long ACTION_EXPIRATION_TIME = TimeUnit.HOURS.toMillis(1L);

    /**
     * Rule to manage feature flag enablement.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule =
            new FeatureFlagTestRule(FeatureFlags.ENABLE_SAVINGS_TEM);

    /**
     * Pre-test setup.
     *
     * @throws IOException when something goes wrong.
     */
    @Before
    public void setup() throws IOException {
        store = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
        tep = new TopologyEventsPoller(topologyEventProvider, topologyInfoTracker, store);
        if (enableTEM) {
            featureFlagTestRule.enable(FeatureFlags.ENABLE_SAVINGS_TEM);
        } else {
            featureFlagTestRule.disable(FeatureFlags.ENABLE_SAVINGS_TEM);
        }
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
        final TopologyEvent deletionEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.RESOURCE_DELETION)
                .setEventTimestamp(creationTime.plusSeconds(1).toEpochMilli())
                .build();

        final EntityEvents entityEventsProto = EntityEvents.newBuilder()
                .setEntityOid(1)
                .addEvents(deletionEvent)
                .addEvents(powerOffEvent)
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
        // Convert the TEP event to a TEM event
        TopologyEventInfo eventInfo = topologyEvent.getEventInfo();
        com.vmturbo.cost.component.savings.bottomup.TopologyEvent.Builder temEvent =
                new com.vmturbo.cost.component.savings.bottomup.TopologyEvent.Builder()
                        .timestamp(eventTimestamp)
                        .entityType(EntityType.VIRTUAL_MACHINE_VALUE)
                        .entityOid(entityId);
        if (TopologyEventType.STATE_CHANGE.equals(topologyEvent.getType())) {
            EntityState newState = eventInfo.getStateChange().getDestinationState();
            temEvent.eventType(EventType.STATE_CHANGE.getValue()).poweredOn(EntityState.POWERED_ON.equals(newState));
        } else if (TopologyEventType.PROVIDER_CHANGE.equals(topologyEvent.getType())) {
            ProviderChangeDetails changeDetails = eventInfo.getProviderChange();
            if (changeDetails.hasDestinationProviderOid()) {
                // Note that we drop the event when the destination provider OID is unknown.
                temEvent
                        .eventType(EventType.PROVIDER_CHANGE.getValue())
                        .providerOid(changeDetails.getDestinationProviderOid());
            }
        } else if (TopologyEventType.RESOURCE_DELETION.equals(topologyEvent.getType())) {
            temEvent.eventType(EventType.ENTITY_REMOVED.getValue()).entityRemoved(true);
        }

        return new SavingsEvent.Builder()
                .topologyEvent(temEvent.build())
                .entityId(entityId)
                .timestamp(eventTimestamp)
                .build();
    }
}
