package com.vmturbo.topology.event.library;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Sets;

import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;

public class TopologyEventsTest {


    /**
     * Tests {@link TopologyEvents#fromEntityEvents(Collection)}, verifying events are ordered correctly
     * by timestamp and {@link TopologyEvents#EVENT_TYPE_ORDER}.
     */
    @Test
    public void testEventOrdering() {

        final Instant creationTime = Instant.now();
        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
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

        final EntityEvents entityEvents = EntityEvents.newBuilder()
                .setEntityOid(1)
                .addEvents(deletionEvent)
                .addEvents(powerOnEvent)
                .addEvents(creationEvent)
                .build();

        // Invoke SUT
        final TopologyEvents topologyEvents = TopologyEvents.fromEntityEvents(Sets.newHashSet(entityEvents));

        // ASSERTS
        final Map<Long, TopologyEventLedger> actualLedgers = topologyEvents.ledgers();
        assertThat(actualLedgers, hasKey(1L));

        final TopologyEventLedger eventLedger = actualLedgers.get(1L);
        assertThat(eventLedger.events(), contains(creationEvent, powerOnEvent, deletionEvent));
    }

    /**
     * Tests merging of two {@link EntityEvents} for the same entity.
     */
    @Test
    public void testEventMerging() {

        final Instant creationTime = Instant.now();
        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
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

        final EntityEvents entityEventsA = EntityEvents.newBuilder()
                .setEntityOid(1)
                .addEvents(creationEvent)
                .build();

        final EntityEvents entityEventsB = EntityEvents.newBuilder()
                .setEntityOid(1)
                .addEvents(powerOnEvent)
                .build();

        // Invoke SUT
        final TopologyEvents topologyEvents = TopologyEvents.fromEntityEvents(
                Sets.newHashSet(entityEventsA, entityEventsB));

        // ASSERTS
        final Map<Long, TopologyEventLedger> actualLedgers = topologyEvents.ledgers();
        assertThat(actualLedgers, hasKey(1L));

        final TopologyEventLedger eventLedger = actualLedgers.get(1L);
        assertThat(eventLedger.events(), contains(creationEvent, powerOnEvent));
    }
}
