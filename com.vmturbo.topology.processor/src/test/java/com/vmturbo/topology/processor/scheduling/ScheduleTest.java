package com.vmturbo.topology.processor.scheduling;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests for the {@link Schedule} class.
 */
public class ScheduleTest {

    private final ScheduledFuture<?> scheduledTask = Mockito.mock(ScheduledFuture.class);

    private final long scheduleIntervalMillis = 100;

    private final Schedule schedule =
        new Schedule(scheduledTask, scheduleIntervalMillis) {
            @Override
            public void cancel() {
                scheduledTask.cancel(true);
            }
        };

    @Test
    public void testGetElapsedTimeSeconds() throws Exception {
        when(scheduledTask.getDelay(TimeUnit.MILLISECONDS)).thenReturn(10L);

        assertEquals(scheduleIntervalMillis - 10, schedule.getElapsedTimeMillis());
    }

    @Test
    public void testGetDelay() throws Exception {
        when(scheduledTask.getDelay(TimeUnit.MINUTES)).thenReturn(2L);

        assertEquals(2, schedule.getDelay(TimeUnit.MINUTES));
    }

    @Test
    public void testGetScheduleData() throws Exception {
        assertEquals(
            scheduleIntervalMillis,
            schedule.getScheduleData().getScheduleIntervalMillis()
        );
    }
}