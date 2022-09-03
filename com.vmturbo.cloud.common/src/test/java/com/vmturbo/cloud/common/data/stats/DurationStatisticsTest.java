package com.vmturbo.cloud.common.data.stats;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Duration;

import org.junit.Test;

/**
 *  Test class for {@link DurationStatistics}.
 */
public class DurationStatisticsTest {

    /**
     * Test collection of multiple duration stats.
     */
    @Test
    public void testCollection() {

        final Duration halfHourDuration = Duration.ofMinutes(30);
        final Duration hourDuration = Duration.ofHours(1);
        final Duration fifteenMinuteDuration = Duration.ofSeconds(60 * 15);

        final DurationStatistics.Collector collector = DurationStatistics.collector();
        collector.collect(halfHourDuration);
        collector.collect(hourDuration);
        collector.collect(fifteenMinuteDuration);

        final DurationStatistics actualStats = collector.toStatistics();

        assertThat(actualStats.count(), equalTo(3L));
        assertThat(actualStats.average(), equalTo(Duration.ofMinutes(35)));
        assertThat(actualStats.sum(), equalTo(Duration.ofMinutes(105)));
        assertThat(actualStats.max(), equalTo(hourDuration));
        assertThat(actualStats.min(), equalTo(fifteenMinuteDuration));
    }

    /**
     * Tests overflow of nanos value.
     */
    @Test
    public void testOverflow() {

        final Duration maxDuration = Duration.ofNanos(Long.MAX_VALUE);
        final DurationStatistics.Collector collector = DurationStatistics.collector();
        collector.collect(maxDuration);
        collector.collect(maxDuration);

        final DurationStatistics actualStats = collector.toStatistics();

        assertThat(actualStats.count(), equalTo(2L));
        assertThat(actualStats.average(), equalTo(maxDuration));
        assertThat(actualStats.sum(), equalTo(maxDuration.multipliedBy(2)));
        assertThat(actualStats.max(), equalTo(maxDuration));
        assertThat(actualStats.min(), equalTo(maxDuration));
    }
}
