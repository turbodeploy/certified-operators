package com.vmturbo.topology.event.library.uptime;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.google.common.collect.ImmutableSortedSet;

import org.junit.Test;

import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.EntityStateChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.topology.event.library.TopologyEvents;
import com.vmturbo.topology.event.library.TopologyEvents.TopologyEventLedger;
import com.vmturbo.topology.event.library.uptime.EntityUptimeCalculator.CacheInvalidationException;
import com.vmturbo.topology.event.library.uptime.EntityUptimeCalculator.EntityUptimeCalculation;

public class EntityUptimeCalculatorTest {

    private static final UptimeCalculationRecorder CALCULATION_RECORDER = new UptimeCalculationRecorder();

    private static final TimeInterval UPTIME_WINDOW = TimeInterval.builder()
            .startTime(Instant.EPOCH.plus(1, ChronoUnit.HOURS))
            .endTime(Instant.EPOCH.plus(2, ChronoUnit.HOURS))
            .build();


    /**
     * Tests a simple case in which the VM powers on and powers off within the uptime window.
     */
    @Test
    public void testIntraPowerStateChange() {

        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(30, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_OFF)
                                .setDestinationState(EntityState.POWERED_ON)
                                .build()))
                .build();

        final TopologyEvent powerOffEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(40, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_ON)
                                .setDestinationState(EntityState.POWERED_OFF)
                                .build()))
                .build();

        final TopologyEventLedger eventLedger = TopologyEventLedger.builder()
                .entityOid(1)
                .events(ImmutableSortedSet.orderedBy(TopologyEvents.TOPOLOGY_EVENT_TIME_COMPARATOR)
                        .add(powerOnEvent, powerOffEvent)
                        .build())
                .build();


        // Invoke calculator
        final EntityUptimeCalculator uptimeCalculator = new EntityUptimeCalculator(CALCULATION_RECORDER, true);
        final EntityUptimeCalculation uptimeCalculation = uptimeCalculator.calculateUptime(eventLedger, UPTIME_WINDOW);

        // setup expected cache response
        final CachedEntityUptime expectedCachedUptime = CachedEntityUptime.builder()
                .startState(EntityState.UNKNOWN)
                .earliestPowerEvent(powerOnEvent)
                .latestPowerEvent(powerOffEvent)
                .uptimeToLatestEvent(Duration.ofMinutes(10))
                .calculationWindow(UPTIME_WINDOW)
                .build();

        assertThat(uptimeCalculation.cachedUptime(), equalTo(expectedCachedUptime));
        assertThat(uptimeCalculation.entityUptime().uptime(), equalTo(Duration.ofMinutes(10)));
    }

    /**
     * Tests carrying forward a power state prior to the uptime window
     */
    @Test
    public void testPowerOnBeforeWindow() {
        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().minus(30, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_OFF)
                                .setDestinationState(EntityState.POWERED_ON)
                                .build()))
                .build();

        final TopologyEventLedger eventLedger = TopologyEventLedger.builder()
                .entityOid(1)
                .events(ImmutableSortedSet.orderedBy(TopologyEvents.TOPOLOGY_EVENT_TIME_COMPARATOR)
                        .add(powerOnEvent)
                        .build())
                .build();


        // Invoke calculator
        final EntityUptimeCalculator uptimeCalculator = new EntityUptimeCalculator(CALCULATION_RECORDER, true);
        final EntityUptimeCalculation uptimeCalculation = uptimeCalculator.calculateUptime(eventLedger, UPTIME_WINDOW);

        // setup expected cache response
        final CachedEntityUptime expectedCachedUptime = CachedEntityUptime.builder()
                .startState(EntityState.POWERED_ON)
                .uptimeToLatestEvent(Duration.ofHours(1))
                .calculationWindow(UPTIME_WINDOW)
                .build();

        assertThat(uptimeCalculation.cachedUptime(), equalTo(expectedCachedUptime));

        assertThat(uptimeCalculation.entityUptime().uptime(), equalTo(Duration.ofHours(1)));
    }

    /**
     * Tests progressing a cached window, where the start state is powered on.
     * @throws CacheInvalidationException
     */
    @Test
    public void testProgressCachedWindowPoweredOn() throws CacheInvalidationException {


        final TopologyEvent powerOffEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(10, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_ON)
                                .setDestinationState(EntityState.POWERED_OFF)
                                .build()))
                .build();
        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(40, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_OFF)
                                .setDestinationState(EntityState.POWERED_ON)
                                .build()))
                .build();

        final CachedEntityUptime originalCachedUptime = CachedEntityUptime.builder()
                .startState(EntityState.POWERED_ON)
                .uptimeToLatestEvent(Duration.ofMinutes(30))
                .calculationWindow(UPTIME_WINDOW)
                .earliestPowerEvent(powerOffEvent)
                .latestPowerEvent(powerOnEvent)
                .build();


        // Invoke calculator
        final Instant newStartTime = UPTIME_WINDOW.startTime().plus(5, ChronoUnit.MINUTES);
        final EntityUptimeCalculator uptimeCalculator = new EntityUptimeCalculator(CALCULATION_RECORDER, true);
        final EntityUptimeCalculation uptimeCalculation =
                uptimeCalculator.progressStartTime(1, originalCachedUptime, newStartTime);

        assertThat(uptimeCalculation.entityUptime().uptime(), equalTo(Duration.ofMinutes(45)));
    }

    /**
     * Tests progressing a cached window, where the start state is powered off.
     * @throws CacheInvalidationException
     */
    @Test
    public void testProgressCachedWindowPoweredOff() throws CacheInvalidationException {


        final TopologyEvent powerOffEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(40, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_ON)
                                .setDestinationState(EntityState.POWERED_OFF)
                                .build()))
                .build();
        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(10, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_OFF)
                                .setDestinationState(EntityState.POWERED_ON)
                                .build()))
                .build();

        final CachedEntityUptime originalCachedUptime = CachedEntityUptime.builder()
                .startState(EntityState.POWERED_OFF)
                .uptimeToLatestEvent(Duration.ofMinutes(30))
                .calculationWindow(UPTIME_WINDOW)
                .earliestPowerEvent(powerOnEvent)
                .latestPowerEvent(powerOffEvent)
                .build();


        // Invoke calculator
        final Instant newStartTime = UPTIME_WINDOW.startTime().plus(5, ChronoUnit.MINUTES);
        final EntityUptimeCalculator uptimeCalculator = new EntityUptimeCalculator(CALCULATION_RECORDER, true);
        final EntityUptimeCalculation uptimeCalculation =
                uptimeCalculator.progressStartTime(1, originalCachedUptime, newStartTime);

        assertThat(uptimeCalculation.entityUptime().uptime(), equalTo(Duration.ofMinutes(30)));
    }

    /**
     * Try to progress the window past the earliest power event.
     * @throws CacheInvalidationException
     */
    @Test(expected = CacheInvalidationException.class)
    public void testProgressCachedWindowPastEarliestEvent() throws CacheInvalidationException {

        final TopologyEvent powerOnEvent = TopologyEvent.newBuilder()
                .setType(TopologyEventType.STATE_CHANGE)
                .setEventTimestamp(UPTIME_WINDOW.startTime().plus(10, ChronoUnit.MINUTES).toEpochMilli())
                .setEventInfo(TopologyEventInfo.newBuilder()
                        .setStateChange(EntityStateChangeDetails.newBuilder()
                                .setSourceState(EntityState.POWERED_OFF)
                                .setDestinationState(EntityState.POWERED_ON)
                                .build()))
                .build();

        final CachedEntityUptime originalCachedUptime = CachedEntityUptime.builder()
                .startState(EntityState.POWERED_OFF)
                .uptimeToLatestEvent(Duration.ofMinutes(50))
                .calculationWindow(UPTIME_WINDOW)
                .earliestPowerEvent(powerOnEvent)
                .latestPowerEvent(powerOnEvent)
                .build();


        // Invoke calculator
        final Instant newStartTime = UPTIME_WINDOW.startTime().plus(11, ChronoUnit.MINUTES);
        final EntityUptimeCalculator uptimeCalculator = new EntityUptimeCalculator(CALCULATION_RECORDER, true);
        final EntityUptimeCalculation uptimeCalculation =
                uptimeCalculator.progressStartTime(1, originalCachedUptime, newStartTime);
    }
}
