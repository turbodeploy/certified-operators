package com.vmturbo.components.common.utils;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TimeUtilTest {

    @Test
    public void testHumanReadableHoursMinsSecs() throws Exception {
        final Duration duration =
            Duration.ofSeconds(TimeUnit.HOURS.toSeconds(1) + TimeUnit.MINUTES.toSeconds(28) + 15);
        assertEquals("1h 28m 15s", TimeUtil.humanReadable(duration));
    }

    @Test
    public void testHumanReadableDaysHoursMinsSecs() throws Exception {
        final Duration duration =
            Duration.ofSeconds(
                TimeUnit.DAYS.toSeconds(3) +
                    TimeUnit.HOURS.toSeconds(1) +
                    TimeUnit.MINUTES.toSeconds(28) +
                    15);
        assertEquals("73h 28m 15s", TimeUtil.humanReadable(duration));
    }

    @Test
    public void testHumanReadableWithNanos() throws Exception {
        final Duration duration = Duration.ofSeconds(19, (long)(0.243 * 1e9));
        assertEquals("19.243s", TimeUtil.humanReadable(duration));
    }
}