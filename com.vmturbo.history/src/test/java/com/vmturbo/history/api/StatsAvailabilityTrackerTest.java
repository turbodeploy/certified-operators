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
    public void testOnlyTopologyAvailable() throws Exception {
        StatsAvailabilityStatus status =
            availabilityTracker.topologyAvailable(LIVE_CONTEXT_ID, TopologyContextType.LIVE, true);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
    }

    @Test
    public void testOnlyProjectedTopologyAvailable() throws Exception {
        StatsAvailabilityStatus status =
            availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN, true);
        assertEquals(StatsAvailabilityStatus.UNAVAILABLE, status);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
        assertFalse(availabilityTracker.isTracking(LIVE_CONTEXT_ID));
    }

    @Test
    public void testAllAvailable() throws Exception {
        availabilityTracker.topologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN, true);
        StatsAvailabilityStatus status =
            availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN, true);

        assertEquals(StatsAvailabilityStatus.AVAILABLE, status);
        verify(notificationSender).statsAvailable(eq(PLAN_CONTEXT_ID));
    }

    @Test
    public void testTrackingRemovedAfterAvailable() throws Exception {
        availabilityTracker.topologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN, true);

        assertTrue(availabilityTracker.isTracking(PLAN_CONTEXT_ID));

        availabilityTracker.projectedTopologyAvailable(PLAN_CONTEXT_ID, TopologyContextType.PLAN, true);

        assertFalse(availabilityTracker.isTracking(PLAN_CONTEXT_ID));
    }
}
