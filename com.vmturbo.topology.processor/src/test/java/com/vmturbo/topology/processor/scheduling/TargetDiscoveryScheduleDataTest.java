package com.vmturbo.topology.processor.scheduling;


import static org.junit.Assert.assertEquals;

import java.util.concurrent.ScheduledFuture;

import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule.TargetDiscoveryScheduleData;

public class TargetDiscoveryScheduleDataTest {
    private final ScheduledFuture<?> scheduledTask = Mockito.mock(ScheduledFuture.class);

    @Test
    public void testGetScheduleData() {
        final long scheduleIntervalMillis = 100;
        TargetDiscoverySchedule discoverySchedule =
            new TargetDiscoverySchedule(scheduledTask, 1, scheduleIntervalMillis, true);

        TargetDiscoveryScheduleData data = (TargetDiscoveryScheduleData)discoverySchedule.getScheduleData();
        assertEquals(scheduleIntervalMillis, data.getScheduleIntervalMillis());
        assertEquals(true, data.isSynchedToBroadcast());
    }
}