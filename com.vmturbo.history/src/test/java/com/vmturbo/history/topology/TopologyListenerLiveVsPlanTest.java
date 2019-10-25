package com.vmturbo.history.topology;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.stats.StatsWriteCoordinator;

/**
 * Test that the StatsHistoryService distinguishes correctly between Live topologies and
 * Planning topologies.
 */
public class TopologyListenerLiveVsPlanTest {

    private static final long REALTIME_TOPOLOGY_ID = 7777777;
    private static final long OTHER_THAN_REALTIME_TOPOLOGY_ID = REALTIME_TOPOLOGY_ID + 1;
    private static final long TEST_TOPOLOGY_ID = 1234;
    private static final long TEST_SNAPSHOT_TIME = 5678;

    private StatsWriteCoordinator statsWriteCoordinator;
    private PlanStatsWriter planStatsWriter;
    private RemoteIterator<TopologyDTO.Topology.DataSegment> testTopologyDTOs;
    private StatsAvailabilityTracker availabilityTracker;

    @Before
    public void setup() {
        statsWriteCoordinator = Mockito.mock(StatsWriteCoordinator.class);
        planStatsWriter = Mockito.mock(PlanStatsWriter.class);
        testTopologyDTOs = Mockito.mock(RemoteIterator.class);
        availabilityTracker = Mockito.mock(StatsAvailabilityTracker.class);
    }

    @Test
    public void liveTopologyNotificationTest() throws Exception {
        final LiveTopologyEntitiesListener serviceUndertest = new LiveTopologyEntitiesListener(
                        statsWriteCoordinator,
                availabilityTracker);
        // Arrange
        RemoteIterator<TopologyDTO.Topology.DataSegment> iterator
                = Mockito.mock(RemoteIterator.class);

        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(REALTIME_TOPOLOGY_ID)
            .setTopologyId(TEST_TOPOLOGY_ID)
            .setCreationTime(TEST_SNAPSHOT_TIME)
            .build();

        // Act
        serviceUndertest.onTopologyNotification(topologyInfo, iterator);

        // Assert
        verify(statsWriteCoordinator).processChunks(Mockito.eq(topologyInfo), Mockito.eq(iterator));
        verifyNoMoreInteractions(statsWriteCoordinator);
        verifyNoMoreInteractions(planStatsWriter);
    }

    @Test
    public void planTopologyNotificationTest() throws Exception {
        final PlanTopologyEntitiesListener serviceUndertest = new PlanTopologyEntitiesListener(
                planStatsWriter,
                availabilityTracker);
        // Arrange

        // Act
        serviceUndertest.onPlanAnalysisTopology(
                TopologyInfo.newBuilder()
                        .setTopologyContextId(OTHER_THAN_REALTIME_TOPOLOGY_ID)
                        .setTopologyId(TEST_TOPOLOGY_ID)
                        .setCreationTime(TEST_SNAPSHOT_TIME)
                        .build(),
                testTopologyDTOs);
        // Assert
        verify(planStatsWriter, times(1)).processChunks(anyObject(), eq(testTopologyDTOs));
        verifyNoMoreInteractions(planStatsWriter);
        verifyNoMoreInteractions(statsWriteCoordinator);
    }
}
