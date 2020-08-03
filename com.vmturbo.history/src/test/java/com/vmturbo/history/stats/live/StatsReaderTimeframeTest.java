package com.vmturbo.history.stats.live;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher;
import com.vmturbo.components.common.utils.RetentionPeriodFetcher.RetentionPeriods;
import com.vmturbo.components.common.utils.TimeFrameCalculator;

/**
 * Test methods select timeframe and table based on startTime, endTime.
 */
@RunWith(Parameterized.class)
public class StatsReaderTimeframeTest {

    /**
     * These are the time ranges queried by the UX, and the expected tables to supply the
     * stats values.
     *
     * @return delta startTime, delta endTime, and expectedTableToRead for each test
     */
    @Parameters(name="{index}: startTime {0}, endTime {1}, timeFrame {2}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {TimeUnit.MINUTES.toMillis(60), TimeUnit.MINUTES.toMillis(10), TimeFrame.LATEST},
                {TimeUnit.HOURS.toMillis(24), TimeUnit.HOURS.toMillis(1), TimeFrame.HOUR},
                {TimeUnit.DAYS.toMillis(7), TimeUnit.DAYS.toMillis(1), TimeFrame.DAY},
                {TimeUnit.DAYS.toMillis(30), TimeUnit.DAYS.toMillis(7), TimeFrame.DAY},
                {TimeUnit.DAYS.toMillis(365), TimeUnit.DAYS.toMillis(30), TimeFrame.MONTH}
        });
    }

    /**
     * These are the test parameters injected by Parameterized runner. Must be public
     * to allow injection.

     * Value to subtract from NOW to give earliest time in the range.
     */
    @Parameter(0)
    public long startTimeDeltaMs;

    /**
     * Value to add to NOW to give the most recent time in the range.
     */
    @Parameter(1)
    public long endTimeDeltaMs;

    /**
     * The timeframe which should be expected for this time range.
     */
    @Parameter(2)
    public TimeFrame expectedTimeFrame;

    @Test
    public void testTimeFrameCalculation() throws Exception {

        // copied from standard OpsManager configuration
        final int NUM_RETAINED_MINUTES=120;
        final int NUM_RETAINED_HOURS=72;
        final int NUM_RETAINED_DAYS=60;

        final Clock clock = Clock.systemUTC();
        final RetentionPeriodFetcher retentionPeriodFetcher = mock(RetentionPeriodFetcher.class);

        final RetentionPeriods retentionPeriods = mock(RetentionPeriods.class);
        when(retentionPeriods.latestRetentionMinutes()).thenReturn(NUM_RETAINED_MINUTES);
        when(retentionPeriods.hourlyRetentionHours()).thenReturn(NUM_RETAINED_HOURS);
        when(retentionPeriods.dailyRetentionDays()).thenReturn(NUM_RETAINED_DAYS);
        when(retentionPeriodFetcher.getRetentionPeriods()).thenReturn(retentionPeriods);

        final TimeFrameCalculator timeFrameCalculator =
                new TimeFrameCalculator(clock, retentionPeriodFetcher);

        final long NOW = clock.millis();

        assertThat(timeFrameCalculator.millis2TimeFrame(NOW - startTimeDeltaMs), is(expectedTimeFrame));
    }


}
