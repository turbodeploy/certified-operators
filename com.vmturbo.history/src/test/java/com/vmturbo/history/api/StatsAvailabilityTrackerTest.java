package com.vmturbo.history.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Test;

import com.vmturbo.history.api.StatsAvailabilityTracker.StatsAvailabilityStatus;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;

public class StatsAvailabilityTrackerTest {
    private final long LIVE_CONTEXT_ID = 1234;
    private final long PLAN_CONTEXT_ID = 5678;

    private final HistoryNotificationSender notificationSender = mock(HistoryNotificationSender.class);
    private final StatsAvailabilityTracker availabilityTracker = new StatsAvailabilityTracker(notificationSender);

    @Test
    public void testNoPartAvailable() {
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
    }

    @Test
    public void testOnlyTopologyAvailable() {
        StatsAvailabilityStatus status =
            availabilityTracker.topologyAvailable(LIVE_CONTEXT_ID, TopologyContextType.LIVE);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
    }

    @Test
    public void testOnlyProjectedTopologyAvailable() {
        StatsAvailabilityStatus status =
            availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
    }

    @Test
    public void testOnlyPriceIndexAvailable() {
        StatsAvailabilityStatus status =
            availabilityTracker.priceIndexAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
    }

    @Test
    public void testPriceIndexAndTopologyAvailable() {
        availabilityTracker.priceIndexAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        StatsAvailabilityStatus status =
            availabilityTracker.topologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
    }

    @Test
    public void testPriceIndexAndProjectedTopologyAvailable() {
        availabilityTracker.priceIndexAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        StatsAvailabilityStatus status =
            availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
    }

    @Test
    public void testTopologyAndProjectedTopologyAvailable() {
        availabilityTracker.topologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        StatsAvailabilityStatus status =
            availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
    }

    @Test
    public void testAllAvailable() {
        availabilityTracker.topologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        availabilityTracker.priceIndexAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        StatsAvailabilityStatus status =
            availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);

        assertEquals(StatsAvailabilityStatus.AVAILABLE, status);
        verify(notificationSender).statsAvailable(eq(PLAN_CONTEXT_ID));
    }

    @Test
    public void testTrackingRemovedAfterAvailable() {
        availabilityTracker.topologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);
        availabilityTracker.priceIndexAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));

        availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN);

        assertFalse(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
    }
}