package com.vmturbo.cloud.commitment.analysis.util;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import com.vmturbo.cloud.common.data.TimeInterval;

public class TimeCalculatorTest {


    @Test
    public void testFlooredDivision() {
        // 3.5 hours
        final Duration dividend = Duration.ofMinutes(210);
        final Duration divisor = Duration.ofHours(1);

        assertThat(TimeCalculator.flooredDivision(dividend, divisor), equalTo(3L));
    }


    @Test
    public void testDivision() {
        final Duration dividend = Duration.ofMinutes(210);
        final Duration divisor = Duration.ofHours(1);

        assertThat(TimeCalculator.divide(dividend, divisor), closeTo(3.5, .000001));
    }

    @Test
    public void testOverlap() {

        final TimeInterval timeIntervalA = TimeInterval.builder()
                .startTime(Instant.ofEpochSecond(100))
                .endTime(Instant.ofEpochSecond(200))
                .build();

        final TimeInterval timeIntervalB = TimeInterval.builder()
                .startTime(Instant.ofEpochSecond(175))
                .endTime(Instant.ofEpochSecond(300))
                .build();

        assertThat(TimeCalculator.overlap(timeIntervalA, timeIntervalB), equalTo(Duration.ofSeconds(25)));
    }

    @Test
    public void testNoOverlap() {

        final TimeInterval timeIntervalA = TimeInterval.builder()
                .startTime(Instant.ofEpochSecond(100))
                .endTime(Instant.ofEpochSecond(200))
                .build();

        final TimeInterval timeIntervalB = TimeInterval.builder()
                .startTime(Instant.ofEpochSecond(225))
                .endTime(Instant.ofEpochSecond(300))
                .build();

        assertThat(TimeCalculator.overlap(timeIntervalA, timeIntervalB), equalTo(Duration.ZERO));
    }
}
