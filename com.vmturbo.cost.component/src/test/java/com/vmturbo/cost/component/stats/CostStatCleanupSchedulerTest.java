package com.vmturbo.cost.component.stats;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.cost.component.stats.CostStatCleanupScheduler.CostsStatCleanup;
import com.vmturbo.cost.component.stats.CostStatCleanupScheduler.CostStatCleanupFactory;
import com.vmturbo.cost.component.stats.CostStatTable.Trimmer;

public class CostStatCleanupSchedulerTest {
    private final CostStatTable table = mock(CostStatTable.class);

    private final RetentionPeriodFetcher retentionPeriodFetcher = mock(RetentionPeriodFetcher.class);

    private final Trimmer tableTrimmer = mock(Trimmer.class);

    private final ExecutorService executorService = mock(ExecutorService.class);

    private static final int MS_BETWEEN_CLEANUPS = 30_000;

    private static final long cleanUpBetweenTimePeriods = 360000L;

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final CostStatCleanupFactory cleanupFactory = mock(CostStatCleanupFactory.class);

    private final ThreadPoolTaskScheduler taskScheduler = mock(ThreadPoolTaskScheduler.class);

    private final CostStatCleanupScheduler cleanupScheduler = new CostStatCleanupScheduler(clock, Collections.singletonList(table),
            retentionPeriodFetcher, executorService, MS_BETWEEN_CLEANUPS, TimeUnit.MILLISECONDS, cleanupFactory, taskScheduler, cleanUpBetweenTimePeriods);

    @Before
    public void setup() {
        when(table.writer()).thenReturn(tableTrimmer);
    }


    @After
    public void stop() {
        executorService.shutdownNow();
        taskScheduler.shutdown();
    }

    @Test
    public void testScheduleCleanups() throws InterruptedException {
        final CostsStatCleanup cleanup = mock(CostsStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);
        // Purely for testing purposes
        when(cleanup.completionStatus()).thenReturn(Optional.of(false));
        cleanupScheduler.scheduleCleanups();

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.getScheduledCleanups().size(), is(1));

        final ArgumentCaptor<ImmutableScheduledCleanupInformation> cleanupInfoCaptor =
                ArgumentCaptor.forClass(ImmutableScheduledCleanupInformation.class);
        verify(cleanupFactory).newCleanup(cleanupInfoCaptor.capture());

        final ImmutableScheduledCleanupInformation cleanupInfo = cleanupInfoCaptor.getValue();
        assertThat(cleanupInfo.tableWriter(), is(tableTrimmer));
        assertThat(cleanupInfo.trimToTime(), is(trimTime));
    }

    @Test
    public void testScheduleCleanupTwiceNoReSubmit() throws InterruptedException {
        final CostsStatCleanup cleanup = mock(CostsStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);
        when(cleanup.completionStatus()).thenReturn(Optional.of(false));
        cleanupScheduler.scheduleCleanups();

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.getScheduledCleanups().size(), is(1));
        verify(executorService).invokeAll(any());

        // Suppose some time passed, so we're ready to re-schedule cleanups.
        clock.addTime(MS_BETWEEN_CLEANUPS + 1, ChronoUnit.MILLIS);
        // The cleanup is still running.
        when(cleanup.completionStatus()).thenReturn(Optional.empty());
    }

    @Test
    public void testScheduleCleanupClearsSucceededRollup() {
        final CostsStatCleanup cleanup = mock(CostsStatCleanup.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);
        final LocalDateTime trimTime = time(12, 10);
        when(table.getTrimTime(retentionPeriods)).thenReturn(trimTime);


        // The original cleanup completes with "true", which means succeeded.
        when(cleanup.completionStatus()).thenReturn(Optional.of(true));
        // Run. The first time we run we will schedule cleanups for sure.
        cleanupScheduler.scheduleCleanups();
        assertThat(cleanupScheduler.getScheduledCleanups().size(), is(0));

        assertFalse(cleanupScheduler.getScheduledCleanups().values().contains(cleanup));

    }


    public static LocalDateTime time(final int hourOfDay, final int minuteOfHour) {
        return LocalDateTime.of(2018, Month.SEPTEMBER, 30, hourOfDay, minuteOfHour);
    }
}
