package com.vmturbo.cloud.commitment.analysis.runtime.data;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;

import org.junit.Test;

public class DurationStatisticsTest {

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
}
