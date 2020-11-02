package com.vmturbo.common.protobuf.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Matchers.eq;

import java.time.Duration;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.utils.Timers.LoggingTimer;
import com.vmturbo.common.protobuf.utils.Timers.NanoTimer;
import com.vmturbo.common.protobuf.utils.Timers.TimingMap;
import com.vmturbo.common.protobuf.utils.Timers.TimingMapTimer;

/**
 * Tests for timers utilities.
 */
public class TimersTest {

    /**
     * A fake timer.
     */
    private static class FakeTimer implements NanoTimer {
        private long iters;

        @Override
        public long nanoTime() {
            iters++;
            return iters * 1_000_000_000; // Increment by 1 second every time nanoTime is called.
        }
    }

    /**
     * Test timing using logging timer.
     */
    @Test
    public void testLoggingTimer() {
        final Logger logger = Mockito.mock(Logger.class);
        try (LoggingTimer timer = new LoggingTimer("foo", logger, new FakeTimer())) {
            // Do nothing
        }

        // The logger should have received a log message.
        Mockito.verify(logger).info(eq("Time for {}: {}"), eq("foo"), eq(Duration.ofSeconds(1)));
    }

    /**
     * Test timing using a timing map.
     */
    @Test
    public void testTimingMap() {
        final TimingMap timingMap = new TimingMap(new FakeTimer());
        foo(timingMap);
        bar(timingMap);

        final String str = timingMap.toString();
        assertThat(str, containsString("Total PT53S, Iterations: 53, Avg: PT1S"));
        assertThat(str, containsString("foo: Total: PT20S, Iterations: 20, Avg: PT1S"));
        assertThat(str, containsString("bar: Total: PT33S, Iterations: 33, Avg: PT1S"));
    }

    private void foo(@Nonnull TimingMap timingMap) {
        for (int i = 0; i < 20; i++) {
            try (TimingMapTimer tim = timingMap.time("foo")) {
                // Do nothing
            }
        }
    }

    private void bar(@Nonnull TimingMap timingMap) {
        for (int i = 0; i < 33; i++) {
            try (TimingMapTimer tim = timingMap.time("bar")) {
                // Do nothing
            }
        }
    }
}