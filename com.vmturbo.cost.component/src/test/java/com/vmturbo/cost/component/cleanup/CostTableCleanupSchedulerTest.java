package com.vmturbo.cost.component.cleanup;

import static com.vmturbo.cost.component.db.Tables.ENTITY_COST;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.cleanup.CostTableCleanup.Trimmer;
import com.vmturbo.cost.component.cleanup.CostTableCleanupScheduler.CleanupTaskFactory;
import com.vmturbo.cost.component.cleanup.CostTableCleanupScheduler.CostTableCleanupTask;
import com.vmturbo.cost.component.cleanup.CostTableCleanupScheduler.ScheduledCleanupInformation;

public class CostTableCleanupSchedulerTest {
    private final CostTableCleanup table = mock(CostTableCleanup.class);

    private final Trimmer tableTrimmer = mock(Trimmer.class);

    private final ExecutorService executorService = mock(ExecutorService.class);

    private static final int MS_BETWEEN_CLEANUPS = 30_000;

    private static final long cleanUpBetweenTimePeriods = 360000L;

    private final MutableFixedClock clock = new MutableFixedClock(1_000_000);

    private final CleanupTaskFactory cleanupFactory = mock(CleanupTaskFactory.class);

    private final ThreadPoolTaskScheduler taskScheduler = mock(ThreadPoolTaskScheduler.class);

    private final CostTableCleanupScheduler cleanupScheduler = new CostTableCleanupScheduler(
            Collections.singletonList(table),
            executorService,
            cleanupFactory,
            taskScheduler,
            Duration.ofMillis(cleanUpBetweenTimePeriods));

    @Before
    public void setup() {

        when(table.writer()).thenReturn(tableTrimmer);
        when(table.tableInfo()).thenReturn(ImmutableTableInfo.builder()
                .table(ENTITY_COST)
                .timeField(ENTITY_COST.CREATED_TIME)
                .shortTableName("entity_cost")
                .build());
    }


    @After
    public void stop() {
        executorService.shutdownNow();
        taskScheduler.shutdown();
    }

    @Test
    public void testScheduleCleanups() throws InterruptedException {
        final CostTableCleanupTask cleanup = mock(CostTableCleanupTask.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final LocalDateTime trimTime = time(12, 10);
        when(table.getTrimTime()).thenReturn(trimTime);
        // Purely for testing purposes
        when(cleanup.completionStatus()).thenReturn(Optional.of(false));
        cleanupScheduler.scheduleCleanups();

        // Run. The first time we run we will schedule cleanups for sure.
        assertThat(cleanupScheduler.getScheduledCleanups().size(), is(1));

        final ArgumentCaptor<ScheduledCleanupInformation> cleanupInfoCaptor =
                ArgumentCaptor.forClass(ScheduledCleanupInformation.class);
        verify(cleanupFactory).newCleanup(cleanupInfoCaptor.capture());

        final ScheduledCleanupInformation cleanupInfo = cleanupInfoCaptor.getValue();
        assertThat(cleanupInfo.tableWriter(), is(tableTrimmer));
        assertThat(cleanupInfo.trimToTime(), is(trimTime));
    }

    @Test
    public void testScheduleCleanupTwiceNoReSubmit() throws InterruptedException {
        final CostTableCleanupTask cleanup = mock(CostTableCleanupTask.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final LocalDateTime trimTime = time(12, 10);
        when(table.getTrimTime()).thenReturn(trimTime);
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
        final CostTableCleanupTask cleanup = mock(CostTableCleanupTask.class);
        when(cleanupFactory.newCleanup(any())).thenReturn(cleanup);

        final LocalDateTime trimTime = time(12, 10);
        when(table.getTrimTime()).thenReturn(trimTime);


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
