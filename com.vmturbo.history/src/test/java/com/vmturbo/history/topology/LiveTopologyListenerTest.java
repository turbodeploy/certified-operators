package com.vmturbo.history.topology;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.listeners.TopologyCoordinator;
import com.vmturbo.history.utils.SystemLoadHelper;

/**
 * Test the Live Topology processing classes
 */
public class LiveTopologyListenerTest {
    // TODO unify: revive these tests
    private static final long REALTIME_TOPOLOGY_ID = 7777777;

    private TopologyCoordinator topologyCoordinator;
    private StatsAvailabilityTracker availabilityTracker;
    private GroupServiceBlockingStub groupServiceClient = null;
    private SystemLoadHelper systemLoadHelper;

    @Before
    public void setup() {
        topologyCoordinator = Mockito.mock(TopologyCoordinator.class);
        availabilityTracker = Mockito.mock(StatsAvailabilityTracker.class);
        systemLoadHelper = mock(SystemLoadHelper.class);
    }

    /**
     * Verify that interleaved topology skipping is working
     */
    @Ignore
    @Test
    public void testInterleavedTopologySkipping() throws Exception {
//        TopologyCoordinator serviceUndertest = new TopologyCoordinator(
//            topologyCoordinator,
//                availabilityTracker,
//                groupServiceClient,
//                systemLoadHelper);

        RemoteIterator<TopologyEntityDTO> iterator = Mockito.mock(RemoteIterator.class);

        CountDownLatch latch = new CountDownLatch(1);

        // processBroadcast should wait for our signal so we can set up an interleaved topology
//        doAnswer(invocationOnMock -> {
//                latch.await();
//                return null;
//            }).when(topologyCoordinator).processChunks(any(), any(), any(), any());

        TopologyInfo topology1 = TopologyInfo.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_ID)
                .setTopologyId(1)
                .setCreationTime(1)
                .build();

        TopologyInfo topology2 = TopologyInfo.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_ID)
                .setTopologyId(2)
                .setCreationTime(2)
                .build();

        final ExecutorService threadPool = Executors.newSingleThreadExecutor();

        // Send the first topology for processing
//        Future<?> t1Future = threadPool.submit(() ->
//                serviceUndertest.onTopologyNotification(topology1, iterator));
//
        // Wait for the processing to start.
        // It doesn't finish, because the processing is blocked until latch counts down.
//        verify(topologyCoordinator, timeout(1000)).processChunks(any(), any(), any(), any());

        // New notification comes in while processing is still in progress.
//        serviceUndertest.onTopologyNotification(topology2, iterator);

        // unblock the processor
        latch.countDown();

//        t1Future.get();

        // verify that one topology (the first one) was marked available.
        verify(availabilityTracker).topologyAvailable(eq(REALTIME_TOPOLOGY_ID), eq(TopologyContextType.LIVE), eq(true));

        // verify that the next one goes through fine
        TopologyInfo topology3 = TopologyInfo.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_ID)
                .setTopologyId(3)
                .setCreationTime(3)
                .build();

//        serviceUndertest.onTopologyNotification(topology3,iterator);

        // done with the threads
        threadPool.shutdown();
    }
}
