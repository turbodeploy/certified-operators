package com.vmturbo.history.market;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.api.StatsAvailabilityTracker.TopologyContextType;
import com.vmturbo.history.stats.LiveStatsWriter;
import com.vmturbo.history.stats.PlanStatsWriter;
import com.vmturbo.history.stats.PriceIndexWriter;
import com.vmturbo.history.stats.projected.ProjectedStatsStore;
import com.vmturbo.history.topology.TopologySnapshotRegistry;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;

/**
 * Test for the priceIndex listener. Ensure that the proper stats writer is called for
 * realtime topologies vs. plan topologies.
 */
@RunWith(value = Parameterized.class)
public class PriceIndexListenerLiveVsPlanTest {

    private static final long REALTIME_TOPOLOGY_CONTEXT_ID = 7777777;
    private static final long OTHER_THAN_REALTIME_TOPOLOGY_CONTEXT_ID
            = REALTIME_TOPOLOGY_CONTEXT_ID + 1;
    private static final int TEST_OID = 7;
    private static final float TEST_CURRENT_VALUE = 1.234F;
    private static final float TEST_PROJECTED_VALUE = 4.567F;
    private static final int TEST_TOPOLOGY_ID = 5678;

    @Parameters(name = "{index}: topologyContextId {0} realtimeCalls {1} planCalls {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {REALTIME_TOPOLOGY_CONTEXT_ID, 1, 0},
                {OTHER_THAN_REALTIME_TOPOLOGY_CONTEXT_ID, 0, 1}
        });
    }

    @Parameter(value = 0)
    public long testTopologyContextId;

    @Parameter(value = 1)
    public int statsWriterCalls;

    @Parameter(value = 2)
    public int planStatsWriterCalls;


    @Test
    public void testLivePriceIndexListener() throws Exception {
        // Arrange
        LiveStatsWriter statsWriter = Mockito.mock(LiveStatsWriter.class);
        PlanStatsWriter planStatsWriter = Mockito.mock(PlanStatsWriter.class);
        PriceIndexWriter priceIndexWriter = Mockito.mock(PriceIndexWriter.class);
        StatsAvailabilityTracker availabilityTracker =
                Mockito.mock(StatsAvailabilityTracker.class);
        ProjectedStatsStore projectedStatsStore = Mockito.mock(ProjectedStatsStore.class);

        TopologySnapshotRegistry snapshotRegistry = new TopologySnapshotRegistry();

        TopologyOrganizer mockSnapshotInfo
                = Mockito.mock(TopologyOrganizer.class);
        snapshotRegistry.registerTopologySnapshot(testTopologyContextId, mockSnapshotInfo);

        PriceIndexDTOs.PriceIndexMessagePayload priceIndexMessagePayload =
                PriceIndexDTOs.PriceIndexMessagePayload.newBuilder()
                        .setOid(TEST_OID)
                        .setPriceindexCurrent(TEST_CURRENT_VALUE)
                        .setPriceindexProjected(TEST_PROJECTED_VALUE)
                        .build();
        PriceIndexMessage priceIndexMessage = PriceIndexMessage.newBuilder()
                .setTopologyContextId(testTopologyContextId)
                .setTopologyId(TEST_TOPOLOGY_ID)
                .addPayload(priceIndexMessagePayload)
                .build();

        MarketListener listenerUnderTest = new MarketListener(statsWriter,
                planStatsWriter, priceIndexWriter, snapshotRegistry,
                REALTIME_TOPOLOGY_CONTEXT_ID,
                availabilityTracker, projectedStatsStore);

        // Act
        listenerUnderTest.onPriceIndexReceived(priceIndexMessage);

        // Assert
        Mockito.verify(priceIndexWriter, times(statsWriterCalls)).persistPriceIndexInfo(anyLong(),
                anyLong(), any(List.class));
        Mockito.verify(planStatsWriter, times(planStatsWriterCalls)).persistPlanPriceIndexInfo(
                any());
        Mockito.verify(availabilityTracker).priceIndexAvailable(
            eq(testTopologyContextId),
            eq(testTopologyContextId == REALTIME_TOPOLOGY_CONTEXT_ID ?
                TopologyContextType.LIVE : TopologyContextType.PLAN));
        Mockito.verifyZeroInteractions(statsWriter);
        Mockito.verifyZeroInteractions(planStatsWriter);
    }
}