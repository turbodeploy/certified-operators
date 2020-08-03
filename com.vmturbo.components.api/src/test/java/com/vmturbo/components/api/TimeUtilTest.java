package com.vmturbo.components.api;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;

import org.junit.Test;

/**
 * Unit tests for {@link TimeUtil}.
 */
public class TimeUtilTest {

    @Test
    public void testLocalDateTimeToMilli() {
        final long epochMilli = 100;
        final LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochMilli),
                Clock.systemUTC().getZone());
        assertThat(TimeUtil.localDateTimeToMilli(dateTime, Clock.systemUTC()), is(epochMilli));
    }

}
