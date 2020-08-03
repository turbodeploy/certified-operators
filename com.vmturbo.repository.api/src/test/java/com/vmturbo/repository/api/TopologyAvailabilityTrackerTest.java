package com.vmturbo.repository.api;

import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.QueuedTopologyRequest;
import com.vmturbo.repository.api.TopologyAvailabilityTracker.TopologyUnavailableException;

/**
 * Unit tests for {@link TopologyAvailabilityTracker}.
 */
public class TopologyAvailabilityTrackerTest {
    private static final long REALTIME_CONTEXT = 7;

    private TopologyAvailabilityTracker availabilityTracker =
        new TopologyAvailabilityTracker(REALTIME_CONTEXT);

    /**
     * Expected exception to use in tests that expect a failure.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test waiting for realtime topology to be successfully available.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologySuccess() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // Topology is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId, REALTIME_CONTEXT);

        // This should complete immediately. No errors.
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test waiting for any realtime topology.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForSourceRealtimeTopologySuccess() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueAnyTopologyRequest(REALTIME_CONTEXT, TopologyType.SOURCE);

        // Topology is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId, REALTIME_CONTEXT);

        // This should complete immediately. No errors.
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test that waiters complete when receiving a "later" topology in realtime.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologyLaterTopology() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // NEWER topology than the one we're waiting for is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId + 11, REALTIME_CONTEXT);

        // This should complete immediately. No errors.
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }


    /**
     * Test appropriate exception when receiving a failure notification for the realtime
     * topology.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologyFailure() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // Topology is now available... BUT IT FAILED
        final String errMsg = "Failed";
        availabilityTracker.onSourceTopologyFailure(topologyId, REALTIME_CONTEXT, errMsg);

        // This should fail immediately.
        expectedException.expectMessage(errMsg);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test appropriate exception when receiving a failure notification for the realtime
     * topology, in the case that we look for "any" topology.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForSourceRealtimeTopologyFailure() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueAnyTopologyRequest(REALTIME_CONTEXT, TopologyType.SOURCE);

        // Topology is now available... BUT IT FAILED
        final String errMsg = "Failed";
        availabilityTracker.onSourceTopologyFailure(topologyId, REALTIME_CONTEXT, errMsg);

        // This should fail immediately.
        expectedException.expectMessage(errMsg);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test that an earlier realtime topology doesn't trigger waiters to complete.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologyEarlierTopologyTimeout() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // OLDER topology than the one we're waiting for is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId - 1, REALTIME_CONTEXT);

        // This should fail, because a newer topology is not available.
        expectedException.expect(TopologyUnavailableException.class);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test that we don't lose "fresher" notifications (by ID comparison) if we receive them
     * out of order.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologyDisorderedNotifications() throws Exception {
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // First, the topology we're expecting.
        availabilityTracker.onSourceTopologyAvailable(topologyId, REALTIME_CONTEXT);
        // Then, an older topology... This one shouldn't overwrite the newer one!
        availabilityTracker.onSourceTopologyAvailable(topologyId - 1, REALTIME_CONTEXT);

        // We should be complete :)
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test receiving availability notification before the request.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologyAlreadySuccess() throws Exception {
        final long topologyId = 10;
        // Topology is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId, REALTIME_CONTEXT);

        // Now get the request.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // This should complete immediately. No errors.
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test receiving failure notification before the request.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForRealtimeTopologyAlreadyFailed() throws Exception {
        final long topologyId = 10;
        // Topology is now available... BUT IT FAILED
        final String errMsg = "Failed";
        availabilityTracker.onSourceTopologyFailure(topologyId, REALTIME_CONTEXT, errMsg);

        // Now get the request.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // This should fail immediately.
        expectedException.expectMessage(errMsg);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test timeout without any topology notifications.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForTopologyTimeout() throws Exception {
        final long topologyId = 10;

        // Get the request.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(REALTIME_CONTEXT, topologyId);

        // This should fail after waiting.
        expectedException.expect(TopologyUnavailableException.class);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test successfully waiting for topology availability in the plan case.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForPlanTopologySuccess() throws Exception {
        final long planId = 123;
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(planId, topologyId);

        // Topology is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId, planId);

        // This should complete immediately. No errors.
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test failure to wait for topology availability in the plan case.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testWaitForPlanTopologyFail() throws Exception {
        final long planId = 123;
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(planId, topologyId);

        // Topology is now available... BUT IT FAILED
        final String errMsg = "Failed";
        availabilityTracker.onSourceTopologyFailure(topologyId, planId, errMsg);

        // This should fail immediately.
        expectedException.expectMessage(errMsg);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testWaitForSpecificTopologyType() throws Exception {
        final long planId = 123;
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest projectedTopologyRequest = availabilityTracker.queueAnyTopologyRequest(planId, TopologyType.PROJECTED);
        QueuedTopologyRequest sourceTopologyRequest = availabilityTracker.queueAnyTopologyRequest(planId, TopologyType.SOURCE);

        // Projected topology is now available... but it's a different topology ID!
        availabilityTracker.onSourceTopologyAvailable(topologyId + 10, planId);

        // The source request should complete immediately.
        sourceTopologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);

        // This should throw an exception, since we only have the source so far.
        try {
            projectedTopologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
            Assert.fail("Request for projected topology satisfied by source topology.");
        } catch (TopologyUnavailableException e) {
            // Expected.
        }

        availabilityTracker.onProjectedTopologyAvailable(topologyId + 11, planId);

        // Now it should return without an exception.
        projectedTopologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);

        QueuedTopologyRequest sourceTopologyRequest2 = availabilityTracker.queueAnyTopologyRequest(planId, TopologyType.SOURCE);
        // The source request should still return - i.e. the projected shouldn't affect the source.
        sourceTopologyRequest2.waitForTopology(1, TimeUnit.MILLISECONDS);
    }

    /**
     * Test that realtime notifications don't affect plan waiters.
     *
     * @throws Exception To satisfy compiler.
     */
    @Test
    public void testRealtimeNotificationNotAffectsPlan() throws Exception {
        final long planId = 123;
        final long topologyId = 10;
        // Get the request first.
        QueuedTopologyRequest topologyRequest =
            availabilityTracker.queueTopologyRequest(planId, topologyId);

        // Realtime topology is now available...
        availabilityTracker.onSourceTopologyAvailable(topologyId, REALTIME_CONTEXT);

        // This should throw an exception, because PLAN topology is not available yet.
        expectedException.expect(TopologyUnavailableException.class);
        topologyRequest.waitForTopology(1, TimeUnit.MILLISECONDS);
    }
}
