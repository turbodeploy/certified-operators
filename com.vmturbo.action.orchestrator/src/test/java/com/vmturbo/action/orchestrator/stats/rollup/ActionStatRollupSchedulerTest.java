package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jooq.exception.DataAccessException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ActionStatRollup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ActionStatRollupFactory;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.RollupDirection;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatRollupScheduler.ScheduledRollupInfo;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatTable.RolledUpActionStats;

public class ActionStatRollupSchedulerTest {

    private final ActionStatTable.Reader fromReader = mock(ActionStatTable.Reader.class);
    private final ActionStatTable.Writer toWriter = mock(ActionStatTable.Writer.class);

    private final ExecutorService executorService = mock(ExecutorService.class);

    private final List<RollupDirection> rollupDependencies = Collections.singletonList(
        ImmutableRollupDirection.builder()
            .fromTableReader(fromReader)
            .toTableWriter(toWriter)
            .description("fromTo")
            .build());

    private final ActionStatRollupFactory rollupFactory = mock(ActionStatRollupFactory.class);

    private final ActionStatRollupScheduler rollupScheduler =
        new ActionStatRollupScheduler(rollupDependencies, executorService, rollupFactory);

    private static final int MGMT_UNIT_SUBGROUP_ID = 7;
    private static final LocalDateTime START_TIME =
        LocalDateTime.ofEpochSecond(10000, 100, ZoneOffset.UTC);

    @Before
    public void startup() {
        when(fromReader.rollupReadyTimes()).thenReturn(Collections.singletonList(
            ImmutableRollupReadyInfo.builder()
                .addManagementUnits(MGMT_UNIT_SUBGROUP_ID)
                .startTime(START_TIME)
                .build()));
    }

    @After
    public void teardown() {
        executorService.shutdownNow();
    }

    @Test
    public void testScheduleRollups() {
        final ActionStatRollup rollup = mock(ActionStatRollup.class);
        when(rollupFactory.newRollup(any())).thenReturn(rollup);

        // Run
        rollupScheduler.scheduleRollups();

        verify(executorService, timeout(1000)).submit(rollup);

        final ArgumentCaptor<ScheduledRollupInfo> rollupInfoCaptor =
            ArgumentCaptor.forClass(ScheduledRollupInfo.class);
        verify(rollupFactory).newRollup(rollupInfoCaptor.capture());
        ScheduledRollupInfo rollupInfo = rollupInfoCaptor.getValue();
        assertThat(rollupInfo.startTime(), is(START_TIME));
        assertThat(rollupInfo.mgmtUnitSubgroupId(), is(MGMT_UNIT_SUBGROUP_ID));
        assertThat(rollupInfo.rollupDirection(), is(rollupDependencies.get(0)));
    }

    @Test
    public void testScheduleRollupTwiceNoReSubmit() {
        final ActionStatRollup rollup = mock(ActionStatRollup.class);

        when(rollupFactory.newRollup(any())).thenReturn(rollup);

        final ActionStatRollupScheduler rollupScheduler =
            new ActionStatRollupScheduler(rollupDependencies, executorService, rollupFactory);

        // Run to schedule the rollup.
        rollupScheduler.scheduleRollups();
        // The rollup is still running.
        when(rollup.completionStatus()).thenReturn(Optional.empty());
        // Schedule the roll-up again.
        rollupScheduler.scheduleRollups();

        // Should only run the rollup once.
        verify(executorService, timeout(1000).times(1)).submit(rollup);
    }

    @Test
    public void testScheduleRollupReSubmitFailedRollup() {
        final ActionStatRollup rollup = mock(ActionStatRollup.class);

        when(rollupFactory.newRollup(any())).thenReturn(rollup);

        final ActionStatRollupScheduler rollupScheduler =
            new ActionStatRollupScheduler(rollupDependencies, executorService, rollupFactory);

        // Run to schedule the rollup.
        rollupScheduler.scheduleRollups();
        // The rollup completes after we "trim" the succeeded rollups, but before we schedule
        // the new one. Completes with "false", which means failed.
        when(rollup.completionStatus()).thenReturn(Optional.empty()).thenReturn(Optional.of(false));
        // Schedule roll-up again. It should get re-submitted, because the first one failed.
        rollupScheduler.scheduleRollups();

        // Should run the rollup twice.
        verify(executorService, timeout(1000).times(2)).submit(rollup);
    }

    @Test
    public void testScheduleRollupClearsSucceededRollup() {
        final ActionStatRollup rollup = mock(ActionStatRollup.class);

        when(rollupFactory.newRollup(any())).thenReturn(rollup);

        final ActionStatRollupScheduler rollupScheduler =
            new ActionStatRollupScheduler(rollupDependencies, executorService, rollupFactory);

        // Run to schedule the rollup.
        rollupScheduler.scheduleRollups();
        assertTrue(rollupScheduler.getScheduledRollups().values().contains(rollup));

        // The original rollup completes with "true", which means succeeded.
        when(rollup.completionStatus()).thenReturn(Optional.of(true));

        final ActionStatRollup otherRollup = mock(ActionStatRollup.class);
        // Return a different object the next time we call the rollup factory.
        when(rollupFactory.newRollup(any())).thenReturn(otherRollup);
        rollupScheduler.scheduleRollups();
        assertFalse(rollupScheduler.getScheduledRollups().values().contains(rollup));

    }

    @Test
    public void testScheduleRollupClearsFailedRollup() {
        final ActionStatRollup rollup = mock(ActionStatRollup.class);

        when(rollupFactory.newRollup(any())).thenReturn(rollup);

        final ActionStatRollupScheduler rollupScheduler =
            new ActionStatRollupScheduler(rollupDependencies, executorService, rollupFactory);

        // Run to schedule the rollup.
        rollupScheduler.scheduleRollups();
        assertTrue(rollupScheduler.getScheduledRollups().values().contains(rollup));

        // The original rollup completes with "false", which means succeeded.
        when(rollup.completionStatus()).thenReturn(Optional.of(false));

        final ActionStatRollup otherRollup = mock(ActionStatRollup.class);
        // Return a different object the next time we call the rollup factory.
        when(rollupFactory.newRollup(any())).thenReturn(otherRollup);
        rollupScheduler.scheduleRollups();
        assertFalse(rollupScheduler.getScheduledRollups().values().contains(rollup));
    }

    @Test
    public void testRollupRun() {
        final int numActionPlanSnapshots = 10;
        final ActionStatRollup rollup = new ActionStatRollup(ImmutableScheduledRollupInfo.builder()
            .rollupDirection(rollupDependencies.get(0))
            .mgmtUnitSubgroupId(MGMT_UNIT_SUBGROUP_ID)
            .startTime(START_TIME)
            .build());

        final RolledUpActionStats rolledUpActionStats = mock(RolledUpActionStats.class);
        when(fromReader.rollup(MGMT_UNIT_SUBGROUP_ID, START_TIME)).thenReturn(Optional.of(rolledUpActionStats));
        rollup.run();

        verify(fromReader).rollup(MGMT_UNIT_SUBGROUP_ID, START_TIME);
        verify(toWriter).insert(MGMT_UNIT_SUBGROUP_ID, rolledUpActionStats);

        assertTrue(rollup.completionStatus().get());
    }

    @Test
    public void testRollupRunReaderFailed() {
        final ActionStatRollup rollup = new ActionStatRollup(ImmutableScheduledRollupInfo.builder()
            .rollupDirection(rollupDependencies.get(0))
            .mgmtUnitSubgroupId(MGMT_UNIT_SUBGROUP_ID)
            .startTime(START_TIME)
            .build());

        when(fromReader.rollup(MGMT_UNIT_SUBGROUP_ID, START_TIME))
            .thenThrow(new DataAccessException("foo"));
        rollup.run();

        verify(fromReader).rollup(MGMT_UNIT_SUBGROUP_ID, START_TIME);
        verifyZeroInteractions(toWriter);

        assertFalse(rollup.completionStatus().get());
    }

    @Test
    public void testRollupRunWriterFailed() {
        final ActionStatRollup rollup = new ActionStatRollup(ImmutableScheduledRollupInfo.builder()
            .rollupDirection(rollupDependencies.get(0))
            .mgmtUnitSubgroupId(MGMT_UNIT_SUBGROUP_ID)
            .startTime(START_TIME)
            .build());

        final RolledUpActionStats rolledUpActionStats = mock(RolledUpActionStats.class);
        when(fromReader.rollup(MGMT_UNIT_SUBGROUP_ID, START_TIME)).thenReturn(Optional.of(rolledUpActionStats));

        doThrow(new DataAccessException("foo"))
            .when(toWriter).insert(MGMT_UNIT_SUBGROUP_ID, rolledUpActionStats);

        rollup.run();

        verify(fromReader).rollup(MGMT_UNIT_SUBGROUP_ID, START_TIME);
        verify(toWriter).insert(MGMT_UNIT_SUBGROUP_ID, rolledUpActionStats);

        assertFalse(rollup.completionStatus().get());
    }

}
