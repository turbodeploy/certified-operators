package com.vmturbo.history.listeners;

import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Missed;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Processed;
import static com.vmturbo.history.listeners.ProcessingLoop.ProcessingAction.Idle;
import static com.vmturbo.history.listeners.ProcessingLoop.ProcessingAction.Repartition;
import static com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor.Live;
import static com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor.Projected;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import io.opentracing.SpanContext;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.history.api.StatsAvailabilityTracker;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.db.bulk.BulkInserterStats;
import com.vmturbo.history.ingesters.live.ProjectedRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.live.SourceRealtimeTopologyIngester;
import com.vmturbo.history.ingesters.plan.ProjectedPlanTopologyIngester;
import com.vmturbo.history.ingesters.plan.SourcePlanTopologyIngester;
import com.vmturbo.history.listeners.ImmutableTopologyCoordinatorConfig.Builder;
import com.vmturbo.history.listeners.ProcessingLoop.ProcessingAction;
import com.vmturbo.history.schema.abstraction.Tables;

/**
 * This class tests various "safety valve" feature of {@link TopologyCoordinator} and associated
 * classes.
 *
 * <p>These are features that attempt to guard against various long-duration failures in ingestion
 * and rollup processing that, if they occur, can cause serious failure in the basic operation
 * of this history component, resulting in a variety of visible defects in the functioning of
 * the overall product.</p>
 */
public class TopologyCoordinatorSafetyValveTest {

    private static final int REALTIME_TOPOLOGY_CONTEXT_ID = 77777;

    // config builder with default values for these tests... override as needed */
    private final Supplier<Builder> configBuilder = () ->
            ImmutableTopologyCoordinatorConfig.builder()
                    .topologyRetentionSecs(Integer.MAX_VALUE)
                    .ingestionTimeoutSecs(Integer.MAX_VALUE)
                    .hourlyRollupTimeoutSecs(Integer.MAX_VALUE)
                    .repartitioningTimeoutSecs(Integer.MAX_VALUE)
                    .processingLoopMaxSleepSecs(1);

    /**
     * Test that {@link ProcessingLoop} kicks off a repartitioning action from time to time
     * in the absence of any other activity.
     *
     * <p>Repartitioning normally happens as part of daily/monthly retention processing, but
     * if that stops happening for any reason, repartitioning must continue to happen in order
     * to avoid running out of space in the database volume. We have a separate safety valve
     * that should also keep rollups happening, but this is a backup to that one.</p>
     *
     */
    @Test
    public void testRepartitionSafetyValve() {
        final ImmutableTopologyCoordinatorConfig config = configBuilder.get()
                .repartitioningTimeoutSecs(2)
                .build();
        final TopologyCoordinator topologyCoordinator = mock(TopologyCoordinator.class);
        final HistorydbIO historydbIo = mock(HistorydbIO.class);
        final ProcessingStatus procStatus = new ProcessingStatus(config, historydbIo);
        final ProcessingStatus procStatusSpy = spy(procStatus);
        doNothing().when(procStatusSpy).load();
        final ProcessingLoop procLoop = new ProcessingLoop(
                topologyCoordinator, procStatusSpy, config);
        final ProcessingLoop procLoopSpy = spy(procLoop);
        final List<ProcessingAction> actions = new ArrayList<>();
        // capture actions chosen by processing loop - in our case they should all be
        // Repartition action, since there's nothing actually going on externally.
        doAnswer((Answer<Pair<ProcessingAction, Object>>)invocation -> {
            final Pair<ProcessingAction, Object> result =
                    (Pair<ProcessingAction, Object>)invocation.callRealMethod();
            actions.add(result.getLeft());
            return result;
        }).when(procLoopSpy).chooseNextAction();
        procLoopSpy.run(20);
        // no outside events, so we should only ever do repartitions
        assertTrue(actions.stream().allMatch(a -> a == Idle || a == Repartition));
        // running for 20 iterations with 1 second sleeps and max 2 seconds between repartitions,
        // we should get  repartitions at 0, 2, 4, 6, ..., 18 seconds if operations take zero time
        // and sleep times are exact. We'll be happy if we got at least 4, meaning that we got
        // our initial call and at least three more, proving that the loop is waking up repeatedly.
        assertTrue(actions.stream().filter(a -> a == Repartition).count() >= 3);
    }

    /**
     * Test that ingestions never sit forever without being processed.
     *
     * <p>If ingestion is left to fester in Received state, it will prevent all future topologies
     * from the same topic from being received. They will follow the festering topology in the
     * kafka partition and so won't be noticed by our kafka consumer until the partition offset
     * advances, which only happens when a message is consumed. (This is about how we use kafka,
     * not about how kafka works.)</p>
     *
     * <p>The safety valve causes a listener thread that is awaiting a decision from the processing
     * loop (to either process or skip the topology) to time out and skip the topology after a
     * configured period of waiting.</p>
     */
    @Test
    public void testThatIngestionsTimeOut() {
        final ImmutableTopologyCoordinatorConfig config = configBuilder.get()
                .ingestionTimeoutSecs(3)
                .build();
        // we only care about the listener, almost everything else can be mocked
        final HistorydbIO historydbIO = mock(HistorydbIO.class);
        final TopologyCoordinator topologyCoordinator = new TopologyCoordinator(
                mock(SourceRealtimeTopologyIngester.class),
                mock(ProjectedRealtimeTopologyIngester.class),
                mock(SourcePlanTopologyIngester.class),
                mock(ProjectedPlanTopologyIngester.class),
                mock(RollupProcessor.class),
                new ProcessingStatus(config, historydbIO), // this needs to be real
                new Thread(), // processing loop
                mock(StatsAvailabilityTracker.class),
                historydbIO,
                config);
        topologyCoordinator.startup();
        RemoteIterator<Topology.DataSegment> topology = mock(RemoteIterator.class);
        when(topology.hasNext()).thenReturn(false);
        TopologyInfo info = TopologyInfo.newBuilder()
                .setCreationTime(Instant.now().toEpochMilli())
                .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                .build();
        // normally, we send a topology to the listener and it waits for processing loop to
        // release it to process the topology at an appropriate time, but here it won't get that
        // release. Instead it should time out (after 3 seconds) and then skip the topology,
        // which it will do by reading all the chunks. We've rigged our topology to be empty
        // (hasNext always returns false), so a single call to hasNext is proof that the listener
        // did, in fact, time out, and that it then proactively drained the topology.
        topologyCoordinator.onTopologyNotification(info, topology, mock(SpanContext.class));
        verify(topology, times(1)).hasNext();
    }

    /**
     * Test that hourly rollups are not delayed indefinitely because a snapshot fails to become
     * fully resolved.
     *
     * <p>Normally, when a live topology is received, that creates a new snapshot in the processing
     * status. That snapshot will track two different topologies associated with the creation time
     * of the topology: the primary topology, and a projected topology sent by market component.
     * The processing loop won't run hourly rollups for that snapshot until it has become fully
     * resolved, meaning that both topologies have been processed (state = Processed or Failed),
     * intentionally Skipped, or detected as Missing (because a following topology has been
     * received or announced). If a snapshot never resolves, it will hold up hourly rollups, and
     * that, in turn, will hold up daily and monthly rollups.</p>
     *
     * <p>The safety valve is that if a snapshot remains unresolved for too long since it was
     * established, it will be forced into a resolved status so that hourly rollups can proceed.
     * Note: there is one exception to this: if an ingestion is in Processing state, it cannot be
     * immediately resolved by changing its state. The active processing must be allowed to
     * complete, at which point that ingestion will be in either Processed or Failed state.</p>
     * @throws InterruptedException if interrupted
     */
    @Test
    public void testHourlyRollupsRunWhenStale() throws InterruptedException {
        final ImmutableTopologyCoordinatorConfig config = configBuilder.get()
                .processingLoopMaxSleepSecs(1)
                .hourlyRollupTimeoutSecs(3)
                .build();
        // everything is mocked except topology coordinator, processing status, and processing loop
        final RollupProcessor rollupProcessor = mock(RollupProcessor.class);
        final HistorydbIO historydbIO = mock(HistorydbIO.class);
        final ProcessingStatus processingStatus = new ProcessingStatus(config, historydbIO);
        final ProcessingStatus procStatusSpy = spy(processingStatus);
        doNothing().when(procStatusSpy).load();
        doNothing().when(procStatusSpy).store();
        final CompletableFuture<TopologyCoordinator> topologyCoordinatorFuture =
                new CompletableFuture<>();
        final SourceRealtimeTopologyIngester sourceRealtimeTopologyIngester = mock(SourceRealtimeTopologyIngester.class);
        // we need to return from our fake ingestion a result that indicates at least one table
        // was modified, else the code to perform rollups will skip it as an optimization
        final BulkInserterStats vmStats = new BulkInserterStats(
                Tables.VM_STATS_LATEST, Tables.VM_STATS_LATEST, Tables.VM_STATS_LATEST);
        final BulkInserterFactoryStats stats = new BulkInserterFactoryStats(Collections.singletonList(vmStats));
        when(sourceRealtimeTopologyIngester.processBroadcast(any(), any(), any())).thenReturn(Pair.of(0, stats));
        TopologyCoordinator topologyCoordinator = new TopologyCoordinator(
                sourceRealtimeTopologyIngester,
                mock(ProjectedRealtimeTopologyIngester.class),
                mock(SourcePlanTopologyIngester.class),
                mock(ProjectedPlanTopologyIngester.class),
                rollupProcessor,
                procStatusSpy,
                new Thread(new ProcessingLoop(topologyCoordinatorFuture, procStatusSpy, config)),
                mock(StatsAvailabilityTracker.class),
                historydbIO,
                config);
        topologyCoordinatorFuture.complete(topologyCoordinator);
        topologyCoordinator.startup();
        // send an empty live topology, which should be processed normally
        Instant snapshotTime = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final TopologyInfo info = TopologyInfo.newBuilder()
                .setCreationTime(snapshotTime.toEpochMilli())
                .setTopologyContextId(REALTIME_TOPOLOGY_CONTEXT_ID)
                .build();
        RemoteIterator<Topology.DataSegment> topology = mock(RemoteIterator.class);
        when(topology.hasNext()).thenReturn(false);
        // this won't return until the topology has been processed
        topologyCoordinator.onTopologyNotification(info, topology, mock(SpanContext.class));
        assertThat(procStatusSpy.getIngestion(Live, info).getState(), is(Processed));
        // we won't send a projected topology, which should prevent the snapshot from ever being
        // fully resolved, and after a few seconds we should see hourly rollups kicked off
        // because of the safety valve, with the primary topology in Processed state and the
        // projected topology in Missed state.
        Thread.sleep(5000);
        verify(rollupProcessor, times(1)).performHourRollups(anyList(), any(Instant.class));
        assertThat(procStatusSpy.getIngestion(Projected, info).getState(), is(Missed));
    }
}
