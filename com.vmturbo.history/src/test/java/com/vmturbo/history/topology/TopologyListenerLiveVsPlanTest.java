package com.vmturbo.history.topology;

import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.ingesters.IngestersConfig;
import com.vmturbo.history.ingesters.plan.writers.PlanStatsWriter;
import com.vmturbo.history.listeners.TopologyCoordinator;

/**
 * Test that the StatsHistoryService distinguishes correctly between Live topologies and
 * Planning topologies.
 */
public class TopologyListenerLiveVsPlanTest {
    // TODO unify: revive these tests

    private static final long REALTIME_TOPOLOGY_ID = 7777777;
    private static final long OTHER_THAN_REALTIME_TOPOLOGY_ID = REALTIME_TOPOLOGY_ID + 1;
    private static final long TEST_TOPOLOGY_ID = 1234;
    private static final long TEST_SNAPSHOT_TIME = 5678;

    private TopologyCoordinator topologyCoordinator;
    private PlanStatsWriter planStatsWriter;
    private RemoteIterator<TopologyDTO.TopologyEntityDTO> testTopologyDTOs;
    private StatsAvailabilityTracker availabilityTracker;
    private GroupServiceBlockingStub groupServiceClient;

    @Before
    public void setup() {
        topologyCoordinator = Mockito.mock(TopologyCoordinator.class);
        planStatsWriter = Mockito.mock(PlanStatsWriter.class);
        testTopologyDTOs = Mockito.mock(RemoteIterator.class);
        availabilityTracker = Mockito.mock(StatsAvailabilityTracker.class);
        groupServiceClient = Mockito.mock(IngestersConfig.class).groupServiceBlockingStub();
    }

    @Test
    public void liveTopologyNotificationTest() throws Exception {
//        final TopologyCoordinator serviceUndertest = new LiveTopologyListener(
//            topologyCoordinator,
//                availabilityTracker,
//                groupServiceClient,
//                systemLoadHelper);
        // Arrange
        RemoteIterator<TopologyDTO.TopologyEntityDTO> iterator
                = Mockito.mock(RemoteIterator.class);

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(REALTIME_TOPOLOGY_ID)
            .setTopologyId(TEST_TOPOLOGY_ID)
            .setCreationTime(TEST_SNAPSHOT_TIME)
            .build();

        // Act
//        serviceUndertest.onTopologyNotification(topologyInfo, iterator);

        // Assert
//        verify(topologyCoordinator).processChunks(Mockito.eq(topologyInfo), Mockito.eq(iterator), any(), any());
        verifyNoMoreInteractions(topologyCoordinator);
        verifyNoMoreInteractions(planStatsWriter);
    }

    /** Currently ignored, will be revisited.
     * @throws Exception if something goes wrong.
     */
    @Ignore
    @Test
    public void planTopologyNotificationTest() throws Exception {
//        final PlanTopologyListener serviceUndertest = new PlanTopologyListener(
//                planStatsWriter,
//                availabilityTracker);
        // Arrange

        // Act
//        serviceUndertest.onPlanAnalysisTopology(
//                TopologyInfo.newBuilder()
//                        .setTopologyContextId(OTHER_THAN_REALTIME_TOPOLOGY_ID)
//                        .setTopologyId(TEST_TOPOLOGY_ID)
//                        .setCreationTime(TEST_SNAPSHOT_TIME)
//                        .build(),
//                testTopologyDTOs);
        // Assert
//        verify(planStatsWriter, times(1)).processChunks(anyObject(), eq(testTopologyDTOs));
        verifyNoMoreInteractions(planStatsWriter);
        verifyNoMoreInteractions(topologyCoordinator);
    }
}
