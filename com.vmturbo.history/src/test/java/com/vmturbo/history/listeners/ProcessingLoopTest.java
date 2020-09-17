package com.vmturbo.history.listeners;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.listeners.IngestionStatus.IngestionState;
import com.vmturbo.history.listeners.ProcessingLoop.ProcessingAction;
import com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor;

/**
 * Tests operation of the processing loop.
 */
public class ProcessingLoopTest {

    private static final long CYCLE_TIME_MILLIS = TimeUnit.MINUTES.toMillis(10);
    private static final int REALTIME_TOPOLOGY_CONTEXT_ID = 77777;

    private ProcessingStatus ps;
    private ProcessingLoop loop;
    private HistorydbIO historydbIO = mock(HistorydbIO.class);
    private static final Instant t0 = Instant.parse("2019-10-21T15:34:23Z");

    /**
     * Set up our {@link ProcessingLoop} and {@link ProcessingStatus} instances.
     */
    @Before
    public void before() {
        // config value are set up so that we don't ever drop processing status during these tests,
        // and we never time out for things.
        final ImmutableTopologyCoordinatorConfig config =
                ImmutableTopologyCoordinatorConfig.builder()
                        .topologyRetentionSecs(Integer.MAX_VALUE)
                        .ingestionTimeoutSecs(Integer.MAX_VALUE)
                        .repartitioningTimeoutSecs(Integer.MAX_VALUE)
                        .hourlyRollupTimeoutSecs(Integer.MAX_VALUE)
                        .processingLoopMaxSleepSecs(60)
                        .build();
        this.ps = new ProcessingStatus(config, historydbIO);
        this.loop = new ProcessingLoop(mock(TopologyCoordinator.class), ps, config);
    }

    /**
     * Test the proper functioning of the {@link ProcessingLoop} action selection logic.
     *
     * <p>Our method is to perform a sequence of processing status changes, simulating the
     * arrival of relevant events, such as receipt of topologies or notifications, completion
     * of ingestion processing or rollups, etc. After each change, we see what action the
     * processing loop chooses next, and test that it is as expected. We don't actually run
     * the processing loop, we just ask what it would do.</p>
     */
    @Test
    public void testProcessingLoop() {

        // there's always a repartitioning run at startup
        assertThat(nextAction(), isRepartion());
        ps.setLastRepartitionTime(Instant.now());

        // c1Live Topology received and starts ingestion (c1 means cycle 1)
        final IngestionStatus c1Live = receive(TopologyFlavor.Live, 1);
        assertThat(nextAction(), isIngestion(c1Live));
        startIngestion(TopologyFlavor.Live, 1);
        assertThat(c1Live.getState(), Matchers.is(IngestionState.Processing));

        // Projected topology received and starts ingestion
        final IngestionStatus c1Proj = receive(TopologyFlavor.Projected, 1);
        assertThat(nextAction(), isIngestion(c1Proj));
        startIngestion(TopologyFlavor.Projected, 1);
        assertThat(c1Proj.getState(), Matchers.is(IngestionState.Processing));

        // Proj topology completes ingestion
        finishIngestion(c1Proj);
        assertThat(c1Proj.getState(), Matchers.is(IngestionState.Processed));
        assertThat(nextAction(), isIdle());

        // Live topology completes ingestion, hourly rollup starts
        finishIngestion(c1Live);
        assertThat(c1Live.getState(), Matchers.is(IngestionState.Processed));
        assertThat(nextAction(), isHourRollup(1));
        startHourRollup(c(1));

        // receive live topology; rollups still going, so nothing happens
        final IngestionStatus c2Live = receive(TopologyFlavor.Live, 2);
        assertThat(c2Live.getState(), Matchers.is(IngestionState.Received));
        assertThat(nextAction(), isIdle());

        // hourly rollups finish, c2Live ingestion starts
        finishHourRollup(c(1));
        assertThat(nextAction(), isIngestion(c2Live));
        startIngestion(TopologyFlavor.Live, 2);

        // c3Live notification is received, still idle
        final IngestionStatus c3Live = expect(TopologyFlavor.Live, 3);
        assertThat(nextAction(), isIdle());

        // c2Live completes, still idle
        finishIngestion(c2Live);
        assertThat(nextAction(), isIdle());

        // c3Live is received; ingestion starts
        receive(TopologyFlavor.Live, 3);
        assertThat(nextAction(), isIngestion((c3Live)));
        startIngestion(TopologyFlavor.Live, 3);

        // c3Proj is received; c2Proj is missed; c3Proj doesn't start b/c c2 rollups are now
        // eligible, and they go first. But those can't start until c3Live is finished.
        final IngestionStatus c3Proj = receive(TopologyFlavor.Projected, 3);
        assertThat(nextAction(), isIdle());
        assertThat(ingestionState(TopologyFlavor.Projected, 2), Matchers.is(IngestionState.Missed));

        // notification for c4Live received but only notification, not the topology itself
        // still waiting for c3Live
        final IngestionStatus c4Live = expect(TopologyFlavor.Live, 4);
        assertThat(nextAction(), isIdle());
        assertThat(c4Live.getState(), Matchers.is(IngestionState.Expected));

        // c3Live completes finally); hourly rollups can run for c2
        finishIngestion(c3Live);
        assertThat(nextAction(), isHourRollup(2));
        startHourRollup(c(2));

        // c4Live received, idle waiting for c2 and c3 hourly rollups
        c4Live.receive();
        assertThat(nextAction(), isIdle());
        assertThat(c4Live.getState(), Matchers.is((IngestionState.Received)));

        // c2 hourly rollups complete; c3Proj can now be ingested
        finishHourRollup(c(2));
        assertThat(nextAction(), isIngestion(c3Proj));
        startIngestion(TopologyFlavor.Projected, 3);

        // notification received for c5Live; c4Live will be skipped
        final IngestionStatus c5Live = expect(TopologyFlavor.Live, 5);
        assertThat(nextAction(), isIdle());
        assertThat(c4Live.getState(), Matchers.is(IngestionState.Skipped));

        // c3 Proj ingestion completes; c3 hourly rollups start
        finishIngestion(c3Proj);
        assertThat(nextAction(), isHourRollup(3));
        startHourRollup(c(3));

        // c3 hourly rollups complete; c4 is an hour change, so daily/monthly rollups now run
        // for c3
        finishHourRollup(c(3));
        assertThat(nextAction(), isDayMonthRollup(3));
        // we also now change c4Live to skipped, since we've heard about c5Live (and we're no
        // longer blocked waiting for c hourly to finish)
        startDayMonthRollup(c(3));

        // c3 day/month rollups complete, c5Live hasn't been received, so we're idle for now
        finishDayMonthRollup(c(3));
        assertThat(nextAction(), isIdle());

        // receive c5Live; it can now run
        receive(TopologyFlavor.Live, 5);
        assertThat(nextAction(), isIngestion(c5Live));
        startIngestion(TopologyFlavor.Live, 5);

        // receive c5Proj topology; c4 proj topo is missed; that means c4 is now fully
        // resolved and ready for hourly rollups, but it must wait until current ingestions complete
        final IngestionStatus c5Proj = receive(TopologyFlavor.Projected, 5);
        assertThat(nextAction(), isIdle());

        // c5Live completes; now c4 hourly rollups can run.
        finishIngestion(c5Live);
        assertThat(nextAction(), isHourRollup(4));
        startHourRollup(c(4));

        // once c4 hourlies are done, c5Proj can be ingested, and then c5 hourlies can run
        finishHourRollup(c(4));
        assertThat(nextAction(), isIngestion(c5Proj));
        startIngestion(TopologyFlavor.Projected, 5);
        finishIngestion(c5Proj);
        assertThat(nextAction(), isHourRollup(5));
        startHourRollup(c(5));
        finishHourRollup(c(5));

        // that's as far as we'll go with this
        assertThat(nextAction(), isIdle());

        // Oh, except that while we're sitting on our built-up processor status, this is a good
        // opportunity to try round-tripping all the TimestampStatuses through de/serialization.
        ps.getSnapshots().forEach(ss ->
                assertEquals(ss.toJson(), SnapshotStatus.fromJson((ss.toJson())).toJson()));
    }


    // following two variables are used to simulate the read-write lock that keeps the processing
    // loop from considering a next action when any rollups are in progress, or from beginning
    // a rollup activity if there are any ingestions in progress.
    private boolean rollupsInProgress = false;
    private boolean ingestionsInProgress = false;

    private Pair<ProcessingAction, Object> nextAction() {
        // nothing new can start if any rollup is in progress
        if (rollupsInProgress) {
            return Pair.of(ProcessingAction.Idle, null);
        }
        final Pair<ProcessingAction, Object> nextAction = loop.chooseNextAction();
        // can't kick off rollups if there are any ingestions going on
        if (ingestionsInProgress
                && (nextAction.getLeft() == ProcessingAction.RunHourRollup
                || nextAction.getLeft() == ProcessingAction.RunDayMonthRollup)) {
            return Pair.of(ProcessingAction.Idle, null);
        }
        return nextAction;
    }

    private IngestionStatus receive(TopologyFlavor flavor, int cycle) {
        return ps.receive(flavor, topInfo(c(cycle)), label(flavor, cycle));
    }

    private IngestionStatus expect(TopologyFlavor flavor, int cycle) {
        return ps.expect(flavor, topInfo(c(cycle)), label(flavor, cycle));
    }

    private IngestionState ingestionState(TopologyFlavor flavor, int cycle) {
        return ps.getIngestion(flavor, topInfo(c(cycle))).getState();
    }

    private IngestionStatus startIngestion(TopologyFlavor flavor, int cycle) {
        ps.startIngestion(flavor, topInfo(c(cycle)), label(flavor, cycle));
        ingestionsInProgress = true;
        return ps.getIngestion(flavor, topInfo(c(cycle)));
    }

    private void finishIngestion(IngestionStatus ingestion) {
        ingestion.finishIngestion(new BulkInserterFactoryStats(Collections.emptyList()));
        ingestionsInProgress = false;
    }

    private void startHourRollup(Instant t) {
        ps.startHourRollup(t);
        rollupsInProgress = true;
    }

    private void finishHourRollup(Instant t) {
        ps.finishHourRollup(t);
        rollupsInProgress = false;
    }

    private void startDayMonthRollup(Instant t) {
        ps.startDayMonthRollup(t);
        rollupsInProgress = true;
    }

    private void finishDayMonthRollup(Instant t) {
        ps.finishDayMonthRollup(t);
        rollupsInProgress = false;
    }

    private TopologyInfo topInfo(Instant t) {
        return TopologyInfo.newBuilder()
                .setCreationTime(t.toEpochMilli())
                .build();
    }

    // Return time that cycle n should start.
    private static Instant c(int n) {
        return t0.plus((n - 1) * CYCLE_TIME_MILLIS, ChronoUnit.MILLIS);
    }

    // Return a label for the topology with given flavor and cycle number.
    private String label(TopologyFlavor flavor, int cycle) {
        return String.format("%s topology @%s", flavor, c(cycle));
    }


    /**
     * Matcher for a RunHourRollup processing action.
     */
    static class IsHourRollup extends TypeSafeMatcher<Pair<ProcessingAction, Object>> {

        private final Pair<ProcessingAction, Object> expected;

        IsHourRollup(Instant t) {
            this.expected = Pair.of(ProcessingAction.RunHourRollup, t);
        }

        @Override
        protected boolean matchesSafely(final Pair<ProcessingAction, Object> action) {
            return action.equals(expected);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(String.format("Hourly Rollup: %s", expected.getRight()));
        }

    }

    private static IsHourRollup isHourRollup(int cycle) {
        return new IsHourRollup(c(cycle));
    }

    /**
     * Matcher for a RunDayMonthRollup processing action.
     */
    static class IsDayMonthRollup extends TypeSafeMatcher<Pair<ProcessingAction, Object>> {

        private final Pair<ProcessingAction, Object> expected;

        IsDayMonthRollup(Instant t) {
            this.expected = Pair.of(ProcessingAction.RunDayMonthRollup, t);
        }

        @Override
        protected boolean matchesSafely(final Pair<ProcessingAction, Object> action) {
            return action.equals(expected);
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText(String.format("Daily/Monthly Rollup: %s", expected.getRight()));
        }

    }

    private static IsDayMonthRollup isDayMonthRollup(int cycle) {
        return new IsDayMonthRollup(c(cycle));
    }

    /**
     * Matcher for a RunIngestion processing action.
     */
    static class IsIngestion extends TypeSafeMatcher<Pair<ProcessingAction, Object>> {

        private final Pair<ProcessingAction, Object> expected;

        IsIngestion(IngestionStatus injection) {
            this.expected = Pair.of(ProcessingAction.RunIngestion, injection);
        }

        @Override
        protected boolean matchesSafely(final Pair<ProcessingAction, Object> action) {
            return action.equals(expected);
        }

        @Override
        public void describeTo(final Description description) {
            final IngestionStatus ingestion = (IngestionStatus)expected.getRight();
            description.appendText(String.format("Ingestion: %s", ingestion.getTopologyLabel()));
        }
    }

    private static IsIngestion isIngestion(IngestionStatus ingestion) {
        return new IsIngestion(ingestion);
    }

    /**
     * Matcher for an Idle processing action.
     */
    static class IsIdle extends TypeSafeMatcher<Pair<ProcessingAction, Object>> {
        @Override
        protected boolean matchesSafely(final Pair<ProcessingAction, Object> action) {
            return action.getLeft() == ProcessingAction.Idle;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Idle");
        }

    }

    private static IsIdle isIdle() {
        return new IsIdle();
    }

    /**
     * Matcher for an Repartition processor action.
     */
    static class IsRepartition extends TypeSafeMatcher<Pair<ProcessingAction, Object>> {
        @Override
        protected boolean matchesSafely(final Pair<ProcessingAction, Object> action) {
            return action.getLeft() == ProcessingAction.Repartition;
        }

        @Override
        public void describeTo(final Description description) {
            description.appendText("Repartition");
        }

    }

    private static IsRepartition isRepartion() {
        return new IsRepartition();
    }
}
