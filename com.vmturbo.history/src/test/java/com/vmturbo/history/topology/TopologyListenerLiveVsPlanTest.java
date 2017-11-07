package com.vmturbo.history.topology;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.stats.LiveStatsWriter;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.utils.TopologyOrganizer;

/**
 * Test that the StatsHistoryService distinguishes correctly between Live topologies and
 * Planning topologies.
 */
public class TopologyListenerLiveVsPlanTest {

    private static final long REALTIME_TOPOLOGY_ID = 7777777;
    private static final long OTHER_THAN_REALTIME_TOPOLOGY_ID = REALTIME_TOPOLOGY_ID + 1;
    private static final long TEST_TOPOLOGY_ID = 1234;
    private static final long TEST_SNAPSHOT_TIME = 5678;

    private LiveStatsWriter liveStatsWriter;
    private PlanStatsWriter planStatsWriter;
    private RemoteIterator<TopologyDTO.TopologyEntityDTO> testTopologyDTOs;
    private StatsAvailabilityTracker availabilityTracker;

    @Before
    public void setup() {
        liveStatsWriter = Mockito.mock(LiveStatsWriter.class);
        planStatsWriter = Mockito.mock(PlanStatsWriter.class);
        testTopologyDTOs = Mockito.mock(RemoteIterator.class);
        availabilityTracker = Mockito.mock(StatsAvailabilityTracker.class);
    }

    @Test
    public void liveTopologyNotificationTest() throws Exception {
        final LiveTopologyEntitiesListener serviceUndertest = new LiveTopologyEntitiesListener(
                liveStatsWriter,
                availabilityTracker);
        // Arrange
        RemoteIterator<TopologyDTO.TopologyEntityDTO> iterator
                = Mockito.mock(RemoteIterator.class);

        // Act
        serviceUndertest.onTopologyNotification(
                TopologyInfo.newBuilder()
                        .setTopologyContextId(REALTIME_TOPOLOGY_ID)
                        .setTopologyId(TEST_TOPOLOGY_ID)
                        .setCreationTime(TEST_SNAPSHOT_TIME)
                        .build(),
                iterator);

        // Assert
        ArgumentCaptor<TopologyOrganizer> organizerArgCaptor = ArgumentCaptor.forClass(TopologyOrganizer.class);

        verify(liveStatsWriter).processChunks(organizerArgCaptor.capture(), Mockito.eq(iterator));
        TopologyOrganizer capturedOrganizer = organizerArgCaptor.getValue();
        assertThat(capturedOrganizer.getTopologyContextId(), is(REALTIME_TOPOLOGY_ID));
        assertThat(capturedOrganizer.getTopologyId(), is(TEST_TOPOLOGY_ID));
        assertThat(capturedOrganizer.getSnapshotTime(), is(TEST_SNAPSHOT_TIME));
        verifyNoMoreInteractions(liveStatsWriter);
        verifyNoMoreInteractions(planStatsWriter);
    }

    @Test
    public void planTopologyNotificationTest() throws Exception {
        final PlanTopologyEntitiesListener serviceUndertest = new PlanTopologyEntitiesListener(
                planStatsWriter,
                availabilityTracker);
        // Arrange

        // Act
        serviceUndertest.onTopologyNotification(
                TopologyInfo.newBuilder()
                        .setTopologyContextId(OTHER_THAN_REALTIME_TOPOLOGY_ID)
                        .setTopologyId(TEST_TOPOLOGY_ID)
                        .setCreationTime(TEST_SNAPSHOT_TIME)
                        .build(),
                testTopologyDTOs);
        // Assert
        verify(planStatsWriter, times(1)).processChunks(anyObject(), eq(testTopologyDTOs));
        verifyNoMoreInteractions(planStatsWriter);
        verifyNoMoreInteractions(liveStatsWriter);
    }
}