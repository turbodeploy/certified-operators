package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingTimeLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimit.TrackingUseLimit;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingNoLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingTimeLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder.TrackingUseLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxConfiguration.TraxLimit;
import com.vmturbo.trax.TraxConfiguration.TraxNoLimit;
import com.vmturbo.trax.TraxConfiguration.TraxTimeLimit;
import com.vmturbo.trax.TraxConfiguration.TraxUseLimit;

/**
 * Tests for {@link com.vmturbo.trax.TraxConfiguration.TraxLimit}s.
 */
public class TraxLimitTest {

    final Clock clock = Mockito.mock(Clock.class);

    /**
     * testNoLimit.
     */
    @Test
    public void testNoLimit() {
        final TraxNoLimit noLimit = TraxNoLimit.getInstance();

        assertTrue(noLimit.tryTracking());
        assertFalse(noLimit.isExhausted());
    }

    /**
     * testNoLimitToProto.
     */
    @Test
    public void testNoLimitToProto() {
        assertEquals(TrackingLimitRemainder.newBuilder()
            .setNoLimit(TrackingNoLimitRemainder.getDefaultInstance())
            .build(), TraxNoLimit.getInstance().toProto());
    }

    /**
     * testNoLimitFromProto.
     */
    @Test
    public void testNoLimitFromProto() {
        assertTrue(TraxLimit.fromProto(TrackingLimit.newBuilder()
            .build()) instanceof TraxNoLimit);
    }

    /**
     * testUseLimit.
     */
    @Test
    public void testUseLimit() {
        final TraxUseLimit useLimit = new TraxUseLimit(100);

        for (int i = 0; i < 100; i++) {
            assertFalse(useLimit.isExhausted());
            useLimit.tryTracking();
        }

        assertTrue(useLimit.isExhausted());
    }

    /**
     * testUseLimitToProto.
     */
    @Test
    public void testUseLimitToProto() {
        final TraxUseLimit useLimit = new TraxUseLimit(100);
        useLimit.tryTracking();
        useLimit.tryTracking();

        assertEquals(TrackingLimitRemainder.newBuilder()
            .setUseLimit(TrackingUseLimitRemainder.newBuilder()
                .setOriginalUseLimit(100)
                .setRemainingUseLimit(98))
            .build(), useLimit.toProto());
    }

    /**
     * testUseLimitFromProto.
     */
    @Test
    public void testUseLimitFromProto() {
        final TraxLimit limit = TraxLimit.fromProto(TrackingLimit.newBuilder()
                .setUseLimit(TrackingUseLimit.newBuilder()
                .setMaxCalculationsToTrack(45)).build());
        assertTrue(limit instanceof TraxUseLimit);
        final TraxUseLimit useLimit = (TraxUseLimit)limit;
        assertEquals(45, useLimit.getOriginalUseLimit());
        assertEquals(45, useLimit.getRemainingUseLimit());
    }

    /**
     * testTimeLimit.
     */
    @Test
    public void testTimeLimit() {
        when(clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(60));

        final TraxTimeLimit limit = new TraxTimeLimit(30, clock);
        assertFalse(limit.isExhausted());

        when(clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(60 + 20));
        assertEquals((double)TimeUnit.MINUTES.toSeconds(10), limit.remainingSeconds(), 0);
        assertFalse(limit.isExhausted());

        when(clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(60 + 40));
        assertTrue(limit.isExhausted());
    }

    /**
     * testTimeLimitToProto.
     */
    @Test
    public void testTimeLimitToProto() {
        when(clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(60));

        final TraxTimeLimit limit = new TraxTimeLimit(30, clock);
        when(clock.millis()).thenReturn(TimeUnit.MINUTES.toMillis(60 + 20));

        assertEquals(TrackingLimitRemainder.newBuilder()
            .setTimeLimit(TrackingTimeLimitRemainder.newBuilder()
                .setExpirationTimestamp(TimeUnit.MINUTES.toMillis(90))
                .setRemainingSeconds((double)TimeUnit.MINUTES.toSeconds(10))
                .setOriginalTimeLimitMinutes(30)
                .setHumanReadableExpirationTime("1970-01-01T01:30:00Z"))
            .build(), limit.toProto());
    }

    /**
     * testTimeLimitFromProto.
     */
    @Test
    public void testTimeLimitFromProto() {
        final TraxLimit limit = TraxLimit.fromProto(TrackingLimit.newBuilder()
            .setTimeLimit(TrackingTimeLimit.newBuilder()
                .setTimeLimitMinutes(25)).build());
        assertTrue(limit instanceof TraxTimeLimit);
        final TraxTimeLimit timeLimit = (TraxTimeLimit)limit;
        assertEquals(25, timeLimit.getOriginalTimeLimitMinutes());
        assertFalse(timeLimit.isExhausted());
    }

    /**
     * testNoTrackingWhenLimitDoesNotPermit.
     */
    @Test
    public void testNoTrackingWhenLimitDoesNotPermit() {
        final TraxLimit limit = Mockito.mock(TraxLimit.class);
        when(limit.isExhausted()).thenReturn(false);

        final TopicSettings settings = new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG)
            .build(), limit, Collections.singleton("foo"));
        try {
            TraxConfiguration.configureTopics(settings);

            when(limit.tryTracking()).thenReturn(false);
            try (TraxContext context = Trax.track("foo")) {
                final String str = trax(5.0, "foo")
                    .plus(10.0, "bar")
                    .compute("addition")
                    .calculationStack();
                Assert.assertEquals("15.0", str);
            }

            when(limit.tryTracking()).thenReturn(true);
            try (TraxContext context = Trax.track("foo")) {
                final String str = trax(5.0, "foo")
                    .plus(10.0, "bar")
                    .compute("addition")
                    .calculationStack();
                Assert.assertEquals(
                    "15[addition] = 5[foo] + 10[bar]\n" +
                        "|__ 5[foo]\n" +
                        "\\__ 10[bar]\n", str);
            }
        } finally {
            TraxConfiguration.clearAllConfiguration();
        }
    }
}
