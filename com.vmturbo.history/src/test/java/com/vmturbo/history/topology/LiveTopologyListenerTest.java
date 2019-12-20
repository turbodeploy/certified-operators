package com.vmturbo.history.topology;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.stats.StatsWriteCoordinator;

/**
 * Test the Live Topology processing classes
 */
public class LiveTopologyListenerTest {
    private static final long REALTIME_TOPOLOGY_ID = 7777777;

    private StatsWriteCoordinator statsWriteCoordinator;
    private StatsAvailabilityTracker availabilityTracker;

    @Before
    public void setup() {
        statsWriteCoordinator = Mockito.mock(StatsWriteCoordinator.class);
        availabilityTracker = Mockito.mock(StatsAvailabilityTracker.class);
    }

    /**
     * Verify that interleaved topology skipping is working
     */
    @Test
    public void testInterleavedTopologySkipping() throws Exception {
        LiveTopologyEntitiesListener serviceUndertest =
                        new LiveTopologyEntitiesListener(statsWriteCoordinator, availabilityTracker);

        RemoteIterator<TopologyDTO.Topology.DataSegment> iterator = Mockito.mock(RemoteIterator.class);

        CountDownLatch latch = new CountDownLatch(1);

        // processChunks should wait for our signal so we can set up an interleaved topology
        doAnswer(invocationOnMock -> {
                latch.await();
                return null;
            }).when(statsWriteCoordinator).processChunks(any(), any());

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
        Future<?> t1Future = threadPool.submit(() ->
                serviceUndertest.onTopologyNotification(topology1, iterator));

        // Wait for the processing to start.
        // It doesn't finish, because the processing is blocked until latch counts down.
        verify(statsWriteCoordinator, timeout(1000)).processChunks(any(), any());

        // New notification comes in while processing is still in progress.
        serviceUndertest.onTopologyNotification(topology2, iterator);

        // unblock the processor
        latch.countDown();

        t1Future.get();

        // verify that one topology (the first one) was marked available.
        verify(availabilityTracker).topologyAvailable(eq(REALTIME_TOPOLOGY_ID),
                eq(TopologyContextType.LIVE), eq(true));

        // verify that the next one goes through fine
        TopologyInfo topology3 = TopologyInfo.newBuilder()
                .setTopologyContextId(REALTIME_TOPOLOGY_ID)
                .setTopologyId(3)
                .setCreationTime(3)
                .build();

        serviceUndertest.onTopologyNotification(topology3,iterator);

        // done with the threads
        threadPool.shutdown();
    }
}
