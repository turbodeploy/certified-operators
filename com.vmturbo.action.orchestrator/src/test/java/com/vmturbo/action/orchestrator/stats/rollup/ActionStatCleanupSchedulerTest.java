package com.vmturbo.action.orchestrator.stats.rollup;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.jooq.exception.DataAccessException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler.ActionStatCleanup;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler.ActionStatCleanupFactory;
import com.vmturbo.action.orchestrator.stats.rollup.ActionStatCleanupScheduler.ScheduledCleanupInfo;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;

public class ActionStatCleanupSchedulerTest {

    private final ActionStatTable table = mock(ActionStatTable.class);
    private final ActionStatTable.Writer tableWriter = mock(ActionStatTable.Writer.class);

    private final ExecutorService executorService = mock(ExecutorService.class);

    private final ActionStatCleanupFactory cleanupFactory = mock(ActionStatCleanupFactory.class);

    private final RetentionPeriodFetcher retentionPeriodFetcher = mock(RetentionPeriodFetcher.class);

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private static final int MS_BETWEEN_CLEANUPS = 30_000;

    private final ActionStatCleanupScheduler cleanupScheduler =
        new ActionStatCleanupScheduler(clock, Collections.singletonList(table), retentionPeriodFetcher,
            executorService, MS_BETWEEN_CLEANUPS, TimeUnit.MILLISECONDS, cleanupFactory);

    @Before
    public void setup() {
        when(table.writer()).thenReturn(tableWriter);
    }

    @Test
    public void testScheduleCleanups() {
        final ActionStatCleanup cleanup = mock(ActionStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = RollupTestUtils.time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));

        final ArgumentCaptor<ActionStatCleanup> cleanupCaptor =
            ArgumentCaptor.forClass(ActionStatCleanup.class);

        verify(executorService).submit(cleanupCaptor.capture());

        final ActionStatCleanup scheduledCleanup = cleanupCaptor.getValue();
        assertThat(scheduledCleanup, is(cleanup));

        final ArgumentCaptor<ScheduledCleanupInfo> cleanupInfoCaptor =
            ArgumentCaptor.forClass(ScheduledCleanupInfo.class);
        verify(cleanupFactory).newCleanup(cleanupInfoCaptor.capture());

        final ScheduledCleanupInfo cleanupInfo = cleanupInfoCaptor.getValue();
        assertThat(cleanupInfo.tableWriter(), is(tableWriter));
        assertThat(cleanupInfo.trimToTime(), is(trimTime));
    }

    @Test
    public void testScheduleCleanupTwiceNoReSubmit() {
        final ActionStatCleanup cleanup = mock(ActionStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = RollupTestUtils.time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));
        verify(executorService).submit(any(ActionStatCleanup.class));

        // Suppose some time passed, so we're ready to re-schedule cleanups.
        clock.addTime(MS_BETWEEN_CLEANUPS + 1, ChronoUnit.MILLIS);
        // The cleanup is still running.
        when(cleanup.completionStatus()).thenReturn(Optional.empty());

        // Run scheduling again, there should be nothing else submitted.
        assertThat(cleanupScheduler.scheduleCleanups(), is(0));
        verifyNoMoreInteractions(executorService);
    }

    @Test
    public void testScheduleCleanupReSubmitFailedRollup() {
        final ActionStatCleanup cleanup = mock(ActionStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = RollupTestUtils.time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));

        // Suppose some time passed, so we're ready to re-schedule cleanups.
        clock.addTime(MS_BETWEEN_CLEANUPS + 1, ChronoUnit.MILLIS);
        // The cleanup completes after we "trim" the succeeded cleanups, but before we schedule
        // the new one. Completes with "false", which means failed.
        when(cleanup.completionStatus()).thenReturn(Optional.of(false));

        // Run scheduling again, the cleanup should be re-submitted.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));
        verify(executorService, times(2)).submit(any(ActionStatCleanup.class));
    }


    @Test
    public void testScheduleCleanupClearsSucceededRollup() {
        final ActionStatCleanup cleanup = mock(ActionStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = RollupTestUtils.time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));

        // The original cleanup completes with "true", which means succeeded.
        when(cleanup.completionStatus()).thenReturn(Optional.of(true));

        final ActionStatCleanup otherCleanup = mock(ActionStatCleanup.class);
        // Return a different object the next time we call the rollup factory.
        when(cleanupFactory.newCleanup(any())).thenReturn(otherCleanup);

        cleanupScheduler.scheduleCleanups();

        assertFalse(cleanupScheduler.getScheduledCleanups().values().contains(cleanup));

    }

    @Test
    public void testScheduleCleanupEnforceMinTime() {
        final ActionStatCleanup cleanup = mock(ActionStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = RollupTestUtils.time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));
        verify(executorService).submit(any(ActionStatCleanup.class));

        // Suppose some time passed, but less than the minimum ms between cleanups.
        clock.addTime(MS_BETWEEN_CLEANUPS - 1, ChronoUnit.MILLIS);
        // The cleanup completes after we "trim" the succeeded cleanups, but before we schedule
        // the new one. Completes with "false", which means failed.
        // Normally this should cause the cleanup to be re-scheduled.
        when(cleanup.completionStatus()).thenReturn(Optional.of(false));

        // Run scheduling again. Since not enough time has passed, we shouldn't re-schedule
        // the cleanup.
        assertThat(cleanupScheduler.scheduleCleanups(), is(0));
        verifyNoMoreInteractions(executorService);
    }


    @Test
    public void testScheduleClearsFailedCleanup() {
        final ActionStatCleanup cleanup = mock(ActionStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = RollupTestUtils.time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.scheduleCleanups(), is(1));

        // The original cleanup completes with "false", which means failed.
        when(cleanup.completionStatus()).thenReturn(Optional.of(false));

        final ActionStatCleanup otherCleanup = mock(ActionStatCleanup.class);
        // Return a different object the next time we call the rollup factory.
        when(cleanupFactory.newCleanup(any())).thenReturn(otherCleanup);

        cleanupScheduler.scheduleCleanups();

        assertFalse(cleanupScheduler.getScheduledCleanups().values().contains(cleanup));
    }

    @Test
    public void testCleanupInitialStatus() {
        final LocalDateTime trimTime = RollupTestUtils.time(1, 2);
        final ActionStatCleanup cleanup = new ActionStatCleanup(ImmutableScheduledCleanupInfo.builder()
            .tableWriter(tableWriter)
            .trimToTime(trimTime)
            .build());
        assertFalse(cleanup.completionStatus().isPresent());
    }

    @Test
    public void testCleanupRun() {
        final LocalDateTime trimTime = RollupTestUtils.time(1, 2);
        final ActionStatCleanup cleanup = new ActionStatCleanup(ImmutableScheduledCleanupInfo.builder()
            .tableWriter(tableWriter)
            .trimToTime(trimTime)
            .build());

        cleanup.run();

        verify(tableWriter).trim(trimTime);

        assertTrue(cleanup.completionStatus().get());
    }

    @Test
    public void testCleanupRunWriterFailed() {
        final LocalDateTime trimTime = RollupTestUtils.time(1, 2);
        final ActionStatCleanup cleanup = new ActionStatCleanup(ImmutableScheduledCleanupInfo.builder()
            .tableWriter(tableWriter)
            .trimToTime(trimTime)
            .build());

        doThrow(new DataAccessException("foo")).when(tableWriter).trim(any());

        cleanup.run();

        verify(tableWriter).trim(trimTime);

        assertFalse(cleanup.completionStatus().get());
    }

}
