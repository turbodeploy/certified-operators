package com.vmturbo.history.listeners;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.ingesters.live.LiveTopologyIngester;
import com.vmturbo.history.ingesters.live.ProjectedLiveTopologyIngester;
import com.vmturbo.history.ingesters.plan.PlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.ProjectedPlanTopologyIngester;
import com.vmturbo.history.listeners.IngestionStatus.IngestionState;
import com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor;

/**
 * Topology Coordinator tests.
 */
public class TopologyCoordinatorTest extends Assert {

    private static final int REALTIME_TOPOLOGY_CONTEXT_ID = 77777;
    private static final Timestamp[] TIMESTAMPS = new Timestamp[]{
            Timestamp.valueOf("2019-11-09 22:11:34")
    };
    private static final TopologyInfo[] TOPOLOGY_INFOS = Stream.of(TIMESTAMPS)
            .map(t -> TopologyInfo.newBuilder()
                    .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                    .setCreationTime(t.getTime())
                    .build())
            .toArray(TopologyInfo[]::new);

    private TopologyCoordinator topologyCoordinator;
    private ProcessingStatus processingStatus;

    /**
     * Set up for testing.
     *
     * <p>Most everything is mocked.</p>
     */
    @Before
    public void before() {
        HistorydbIO historydbIo = mock(HistorydbIO.class);
        final ImmutableTopologyCoordinatorConfig config =
                ImmutableTopologyCoordinatorConfig.builder()
                        .topologyRetentionSecs((int)TimeUnit.HOURS.toSeconds(3))
                        .ingestionTimeoutSecs(Integer.MAX_VALUE)
                        .hourlyRollupTimeoutSecs(Integer.MAX_VALUE)
                        .repartitioningTimeoutSecs(Integer.MAX_VALUE)
                        .processingLoopMaxSleepSecs(60)
                        .realtimeTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                        .build();
        // we need a real processing status object, but we need to prevent stub out its load method
        processingStatus = Mockito.spy(new ProcessingStatus(config, historydbIo));
        doNothing().when(processingStatus).load();
        topologyCoordinator = new TopologyCoordinator(
                mock(LiveTopologyIngester.class),
                mock(ProjectedLiveTopologyIngester.class),
                mock(PlanTopologyIngester.class),
                mock(ProjectedPlanTopologyIngester.class),
                mock(RollupProcessor.class),
                processingStatus,
                mock(Thread.class),
                mock(StatsAvailabilityTracker.class),
                historydbIo,
                config);
        topologyCoordinator.startup();
    }

    /**
     * Test that, when a topology summary message is received about a previously unknown live topology, the ingestion
     * status is set to Expected.
     */
    @Test
    public void testLiveSummaryMarksIngestionExpected() {
        topologyCoordinator.onTopologySummary(
                TopologySummary.newBuilder()
                        .setTopologyInfo(TOPOLOGY_INFOS[0])
                        .build());
        assertEquals(IngestionState.Expected, processingStatus.getIngestion(TopologyFlavor.Live, TOPOLOGY_INFOS[0]).getState());

    }

    /**
     * Test that, when a topology summary message is received about a previously known live topology, the ingestion
     * status is not changed.
     */
    @Test
    public void testLiveSummaryLeavesExistingIngestionUnchanged() {
        processingStatus.receive(TopologyFlavor.Live, TOPOLOGY_INFOS[0], "x");
        topologyCoordinator.onTopologySummary(
                TopologySummary.newBuilder()
                        .setTopologyInfo(TOPOLOGY_INFOS[0])
                        .build()
        );
        assertEquals(IngestionState.Received, processingStatus.getIngestion(TopologyFlavor.Live, TOPOLOGY_INFOS[0]).getState());
    }

    /**
     * Test that, when an analysis summary message is received about a previously unknown projected topology,
     * the ingestion status is set to Expected.
     */
    @Test
    public void testAnalysisSummaryMarksIngestionExpected() {
        topologyCoordinator.onAnalysisSummary(
                AnalysisSummary.newBuilder()
                        .setSourceTopologyInfo(TOPOLOGY_INFOS[0])
                        .build());
        assertEquals(IngestionState.Expected, processingStatus.getIngestion(TopologyFlavor.Projected, TOPOLOGY_INFOS[0]).getState());
    }

    /**
     * Test that, when an analysis summary message is received about a previously known live topology, the ingestion
     * status is not changed.
     */
    @Test
    public void testAnalysisSummaryLeavesExistingIngestionUnchanged() {
        processingStatus.receive(TopologyFlavor.Projected, TOPOLOGY_INFOS[0], "x");
        topologyCoordinator.onAnalysisSummary(
                AnalysisSummary.newBuilder()
                        .setSourceTopologyInfo(TOPOLOGY_INFOS[0])
                        .build()
        );
        assertEquals(IngestionState.Received, processingStatus.getIngestion(TopologyFlavor.Projected, TOPOLOGY_INFOS[0]).getState());
    }
}
