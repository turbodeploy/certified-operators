package com.vmturbo.history.listeners;

import static org.hamcrest.Matchers.contains;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisSummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologySummary;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.ingesters.live.ProjectedRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.live.SourceRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.plan.ProjectedPlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.SourcePlanTopologyIngester;
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
        DSLContext dsl = mock(DSLContext.class);
        final ImmutableTopologyCoordinatorConfig config =
                ImmutableTopologyCoordinatorConfig.builder()
                        .topologyRetentionSecs((int)TimeUnit.HOURS.toSeconds(3))
                        .ingestionTimeoutSecs(Integer.MAX_VALUE)
                        .hourlyRollupTimeoutSecs(Integer.MAX_VALUE)
                        .repartitioningTimeoutSecs(Integer.MAX_VALUE)
                        .processingLoopMaxSleepSecs(60)
                        .build();
        // we need a real processing status object, but we need to prevent stub out its load method
        processingStatus = Mockito.spy(new ProcessingStatus(config, dsl));
        doNothing().when(processingStatus).load();
        topologyCoordinator = new TopologyCoordinator(
                mock(SourceRealtimeTopologyIngester.class),
                mock(ProjectedRealtimeTopologyIngester.class),
                mock(SourcePlanTopologyIngester.class),
                mock(ProjectedPlanTopologyIngester.class),
                mock(RollupProcessor.class),
                processingStatus,
                new Thread(),
                mock(StatsAvailabilityTracker.class),
                dsl,
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

    /**
     * Test that the tag values produced for the SKIP_TOPOLOGY safety valve metric correctly
     * identify the topology being skipped.
     */
    @Test
    public void testMetricTags() {
        final TopologyInfo realtimeInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.REALTIME)
                .build();
        final TopologyInfo planInfo = TopologyInfo.newBuilder()
                .setTopologyType(TopologyType.PLAN)
                .build();
        assertThat(Arrays.asList(TopologyCoordinator.getLabelsForSkipSafetyValve(realtimeInfo, TopologyFlavor.Live)),
                contains("skip-topology", "live", "source"));
        assertThat(Arrays.asList(TopologyCoordinator.getLabelsForSkipSafetyValve(realtimeInfo, TopologyFlavor.Projected)),
                contains("skip-topology", "live", "projected"));
        assertThat(Arrays.asList(TopologyCoordinator.getLabelsForSkipSafetyValve(planInfo, TopologyFlavor.Live)),
                contains("skip-topology", "plan", "source"));
        assertThat(Arrays.asList(TopologyCoordinator.getLabelsForSkipSafetyValve(planInfo, TopologyFlavor.Projected)),
                contains("skip-topology", "plan", "projected"));
    }
}
