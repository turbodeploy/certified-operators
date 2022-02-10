package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.lang3.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionType;
import com.vmturbo.cost.component.savings.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.TopologyEvent.EventType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;

/**
 * All tests related to events store.
 */
public class EntityEventsJournalTest {
    private EntityEventsJournal store;
    private static final long vm1Id = 101L;
    private static final long vm2Id = 201L;
    private static final long vm3Id = 301L;
    private static final long action1Id = 1001L;
    private static final long action2Id = 1002L;

    /**
     * Initializing store.
     */
    @Before
    public void setup() {
        store = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
    }

    @Nonnull
    private static SavingsEvent getPowerStateEvent(long vmId, long timestamp, boolean newStateOn) {
        return new SavingsEvent.Builder()
                .topologyEvent(new TopologyEvent.Builder()
                        .entityOid(vmId)
                        .eventType(EventType.STATE_CHANGE.getValue())
                        .timestamp(timestamp)
                        .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .poweredOn(newStateOn)
                        .build())
                .entityId(vmId)
                .timestamp(timestamp)
                .build();
    }

    @Nonnull
    private static SavingsEvent getResourceDeletedEvent(long vmId, long timestamp) {
        return new SavingsEvent.Builder()
                .topologyEvent(new TopologyEvent.Builder()
                        .entityOid(vmId)
                        .timestamp(timestamp)
                        .eventType(EventType.ENTITY_REMOVED.getValue())
                        .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .build())
                .entityId(vmId)
                .timestamp(timestamp)
                .build();
    }


    @Nonnull
    private static SavingsEvent getActionEvent(long vmId, long timestamp, ActionEventType actionType,
            long actionId, @Nonnull final EntityPriceChange priceChange) {
        return new SavingsEvent.Builder()
                .actionEvent(new ActionEvent.Builder()
                        .actionId(actionId)
                        .eventType(actionType)
                        .description(StringUtils.EMPTY)
                        .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .actionType(ActionType.SCALE_VALUE)
                        .actionCategory(ActionCategory.EFFICIENCY_IMPROVEMENT_VALUE)
                        .build())
                .entityId(vmId)
                .timestamp(timestamp)
                .entityPriceChange(priceChange)
                .build();
    }

    /**
     * Tests event addition and removal.
     */
    @Test
    public void eventsAddAndRemove() {
        assertEquals(0, store.size());
        final EntityPriceChange priceChange = new EntityPriceChange.Builder()
                .sourceOid(1001L)
                .sourceCost(10.518d)
                .destinationOid(2001L)
                .destinationCost(6.23d)
                .build();

        ImmutableSet<SavingsEvent> inputEvents = ImmutableSet.of(
                getPowerStateEvent(vm1Id, 300L, false),
                getPowerStateEvent(vm1Id, 300L, true),
                getActionEvent(vm1Id, 300L, ActionEventType.RECOMMENDATION_ADDED,
                        action1Id, priceChange),
                getPowerStateEvent(vm2Id, 500L, false),
                getActionEvent(vm1Id, 400L, ActionEventType.RECOMMENDATION_ADDED,
                        action1Id, priceChange),
                getActionEvent(vm2Id, 200L, ActionEventType.RECOMMENDATION_REMOVED,
                        action2Id, priceChange),
                getResourceDeletedEvent(vm2Id, 600L)
        );

        // Insert 7 different events into store - with timestamps ranging from 200 to 600.

        // Add 7 events, remove them all and check if counts are good.
        store.addEvents(inputEvents);
        assertEquals(7, store.size());

        Long oldestTime = store.getOldestEventTime();
        assertNotNull(oldestTime);
        assertEquals(200L, (long)oldestTime);
        final List<SavingsEvent> allEvents = store.removeAllEvents();
        assertEquals(0, store.size());
        assertEquals(7, allEvents.size());
        oldestTime = store.getOldestEventTime();
        assertNull(oldestTime);

        // Check if range query works.
        store.addEvents(inputEvents);
        final List<SavingsEvent> events400To601 = store.removeEventsBetween(400L, 601L);
        store.removeAllEvents();

        // Now add them back and check their values after removal.
        store.addEvents(inputEvents);

        // Verify there are a total of 7 events in the store now.
        assertEquals(7, store.size());

        // Remove all events from store with timestamp of 400 (including) and newer.
        final List<SavingsEvent> events400AndUp = store.removeEventsSince(400L);

        // 2 events now left in the store, out of the previous total of 5.
        assertEquals(4, store.size());

        // 3 events removed from the store.
        assertEquals(3, events400AndUp.size());

        // Check 1st event - with lowest timestamp 400. Action recommendation event.
        final SavingsEvent event1 = events400AndUp.get(0);
        assertNotNull(event1);
        assertTrue(event1.hasActionEvent());
        assertFalse(event1.hasTopologyEvent());

        assertTrue(event1.getActionEvent().isPresent());
        final ActionEvent actionAdded = event1.getActionEvent().get();
        assertNotNull(actionAdded);
        assertEquals(vm1Id, event1.getEntityId());
        assertEquals(400, event1.getTimestamp());
        assertEquals(ActionEventType.RECOMMENDATION_ADDED, actionAdded.getEventType());
        assertEquals(action1Id, actionAdded.getActionId());
        assertTrue(event1.hasEntityPriceChange());

        assertTrue(event1.getEntityPriceChange().isPresent());
        final EntityPriceChange priceChange400 = event1.getEntityPriceChange().get();
        assertNotNull(priceChange400);
        assertEquals(priceChange, priceChange400);

        // Check 2nd event - timestamp 500. PowerState (Topology) event. ON -> OFF.
        Iterator<SavingsEvent> iterator = events400AndUp.iterator();
        iterator.next();

        final SavingsEvent event2 = iterator.next();
        assertNotNull(event2);
        assertFalse(event2.hasActionEvent());
        assertTrue(event2.hasTopologyEvent());

        assertEquals(vm2Id, event2.getEntityId());
        assertEquals(500, event2.getTimestamp());

        assertTrue(event2.getTopologyEvent().isPresent());
        final TopologyEvent powerChange = event2.getTopologyEvent().get();
        assertNotNull(powerChange);
        assertEquals(EventType.STATE_CHANGE.getValue(), (int)powerChange.getEventType());
        assertTrue(powerChange.getPoweredOn().isPresent());
        assertFalse(powerChange.getPoweredOn().get());

        // 3rd event - highest timestamp 600
        final SavingsEvent event3 = events400AndUp.get(events400AndUp.size() - 1);
        assertNotNull(event3);
        assertFalse(event3.hasActionEvent());
        assertTrue(event3.hasTopologyEvent());

        assertTrue(event3.getTopologyEvent().isPresent());
        final TopologyEvent entityRemoved = event3.getTopologyEvent().get();
        assertNotNull(entityRemoved);
        assertEquals(vm2Id, event3.getEntityId());
        assertEquals(600, event3.getTimestamp());
        assertEquals(EventType.ENTITY_REMOVED.getValue(), (int)entityRemoved.getEventType());

        // Verify we got 3 same events in range query.
        assertEquals(events400To601, events400AndUp);
    }

    /**
     * Verify that we can extract a subset of events from the event journal.
     */
    @Test
    public void testGetEventsFilteredByUuid() {
        assertEquals(0, store.size());
        final EntityPriceChange priceChange = new EntityPriceChange.Builder()
                .sourceOid(1001L)
                .sourceCost(10.518d)
                .destinationOid(2001L)
                .destinationCost(6.23d)
                .build();

        ImmutableSet<SavingsEvent> inputEvents = ImmutableSet.of(
                getPowerStateEvent(vm1Id, 300L, false),
                getPowerStateEvent(vm1Id, 300L, true),
                getActionEvent(vm1Id, 300L, ActionEventType.RECOMMENDATION_ADDED,
                        action1Id, priceChange),
                getPowerStateEvent(vm2Id, 500L, false),
                getActionEvent(vm1Id, 400L, ActionEventType.RECOMMENDATION_ADDED,
                        action1Id, priceChange),
                getActionEvent(vm2Id, 200L, ActionEventType.RECOMMENDATION_REMOVED,
                        action2Id, priceChange),
                getResourceDeletedEvent(vm2Id, 600L)
        );

        // Insert 7 different events into store - with timestamps ranging from 200 to 600.

        // Add 7 events, remove them all and check if counts are good.
        store.addEvents(inputEvents);
        assertEquals(7, store.size());

        // Only get events between 400 and 600 related to UUID 101
        List<SavingsEvent> filteredEvents = store.removeEventsBetween(400L, 601L,
                ImmutableSet.of(101L));
        assertEquals(6, store.size());
        assertEquals(1, filteredEvents.size());

        // Get all events for UUID 101.
        store.addEvents(inputEvents);
        // Add an additional event with an unrelated UUID in the same time bucket as a UUID 101
        // event.
        store.addEvent(getActionEvent(vm3Id, 400L, ActionEventType.RECOMMENDATION_ADDED,
                action1Id, priceChange));
        filteredEvents = store.removeEventsBetween(0L, 1000L, ImmutableSet.of(101L));
        assertEquals(4, store.size());
        assertEquals(4, filteredEvents.size());
    }

    /**
     * Check that events don't get lost if they happen all at the same time.
     * We cannot guarantee the order of events occurring at the exact same time, only that non will
     * be lost.
     */
    @Test
    public void sameTimeEventsNoneLost() {
        assertEquals(0, store.size());

        ImmutableSet<SavingsEvent> inputSet = ImmutableSet.of(
                getPowerStateEvent(vm2Id, 200L, true),
                getPowerStateEvent(vm1Id, 200L, true),
                getPowerStateEvent(vm3Id, 200L, true)
        );

        inputSet.forEach(store::addEvent);

        // Verify there are a total of 3 unique timestamped events in the store now.
        assertEquals(3, store.size());

        final List<SavingsEvent> powerEvents200 = store.removeEventsSince(200L);
        assertEquals(0, store.size());
        assertEquals(3, powerEvents200.size());

        // Check that all input events are present in the returned set.
        assertTrue(powerEvents200.containsAll(inputSet));
    }
}
