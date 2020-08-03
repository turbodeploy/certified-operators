package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.trax.Trax.TrackingLimitRemainder;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.trax.TraxConfiguration.TopicSettings;
import com.vmturbo.trax.TraxConfiguration.TraxContext;
import com.vmturbo.trax.TraxConfiguration.TraxContext.TrackingTraxContext;
import com.vmturbo.trax.TraxConfiguration.TraxLimit;
import com.vmturbo.trax.TraxConfiguration.TraxUseLimit;

/**
 * Tests for {@link TraxConfiguration}.
 * <p/>
 * Note that trax tests should only be run single-threaded because configuration occurs statically.
 */
public class TraxConfigurationTest {

    /**
     * Clean up the configuration.
     */
    @Before
    public void setup() {
        TraxConfiguration.clearAllConfiguration();
    }

    /**
     * Clean up the configuration.
     */
    @After
    public void tearDown() {
        TraxConfiguration.clearAllConfiguration();
    }

    /**
     * testMoreVerboseThan.
     */
    @Test
    public void testMoreVerboseThan() {
        final TrackingTraxContext off = new TrackingTraxContext(Thread.currentThread(),
            Collections.emptyList(),
            TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.OFF).build());
        final TrackingTraxContext debug = new TrackingTraxContext(Thread.currentThread(),
            Collections.emptyList(),
            TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.DEBUG).build());
        final TrackingTraxContext trace = new TrackingTraxContext(Thread.currentThread(),
            Collections.emptyList(),
            TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.TRACE).build());

        assertTrue(trace.moreVerboseThan(off));
        assertTrue(trace.moreVerboseThan(debug));
        assertTrue(debug.moreVerboseThan(off));

        assertFalse(trace.moreVerboseThan(trace));
        assertFalse(debug.moreVerboseThan(debug));
        assertFalse(off.moreVerboseThan(off));

        assertFalse(off.moreVerboseThan(debug));
        assertFalse(off.moreVerboseThan(trace));
        assertFalse(debug.moreVerboseThan(trace));

        off.close();
        debug.close();
        trace.close();
    }

    /**
     * testMoreVerboseThanFallsBackToDecimalPlacesOnEqualVerbosity.
     */
    @Test
    public void testMoreVerboseThanFallsBackToDecimalPlacesOnEqualVerbosity() {
        final TrackingTraxContext threeDecimals = new TrackingTraxContext(Thread.currentThread(),
            Collections.emptyList(),
            TraxTopicConfiguration.newBuilder()
                .setVerbosity(Verbosity.DEBUG)
                .setMaxDecimalPlaces(3)
                .build());
        final TrackingTraxContext fourDecimals = new TrackingTraxContext(Thread.currentThread(),
            Collections.emptyList(),
            TraxTopicConfiguration.newBuilder()
                .setVerbosity(Verbosity.DEBUG)
                .setMaxDecimalPlaces(4)
                .build());
        final TrackingTraxContext trace = new TrackingTraxContext(Thread.currentThread(),
            Collections.emptyList(),
            TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.TRACE).build());

        assertTrue(fourDecimals.moreVerboseThan(threeDecimals));
        assertTrue(trace.moreVerboseThan(threeDecimals));
        assertFalse(threeDecimals.moreVerboseThan(threeDecimals));

        threeDecimals.close();
        fourDecimals.close();
        trace.close();
    }

    /**
     * testVerbosityOffByDefault.
     */
    @Test
    public void testVerbosityOffByDefault() {
        assertEquals(Verbosity.OFF, TraxConfiguration.verbosity(Thread.currentThread()));
    }

    /**
     * testConfiguredOff.
     */
    @Test
    public void testConfiguredOff() {
        TraxConfiguration.configureTopics("foo", Verbosity.OFF);

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.OFF, TraxConfiguration.verbosity(Thread.currentThread()));
        }
    }

    /**
     * testConfiguredDebug.
     */
    @Test
    public void testConfiguredDebug() {
        TraxConfiguration.configureTopics("foo", Verbosity.DEBUG);

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.DEBUG, TraxConfiguration.verbosity(Thread.currentThread()));
        }

        try (TraxContext context = Trax.track("bar")) {
            assertEquals(Verbosity.OFF, TraxConfiguration.verbosity(Thread.currentThread()));
        }
    }

    /**
     * testConfiguredTrace.
     */
    @Test
    public void testConfiguredTrace() {
        TraxConfiguration.configureTopics("foo", Verbosity.TRACE);

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.TRACE, TraxConfiguration.verbosity(Thread.currentThread()));
        }

        try (TraxContext context = Trax.track("bar")) {
            assertEquals(Verbosity.OFF, TraxConfiguration.verbosity(Thread.currentThread()));
        }
    }

    /**
     * testConfigurationTakesMostVerbose.
     */
    @Test
    public void testConfigurationTakesMostVerbose() {
        TraxConfiguration.configureTopics("foo", Verbosity.OFF);
        TraxConfiguration.configureTopics("bar", Verbosity.TRACE);
        TraxConfiguration.configureTopics("12345", Verbosity.OFF);
        TraxConfiguration.configureTopics("baz", Verbosity.DEBUG);

        try (TraxContext context = Trax.track("foo", "bar", "12345", "baz")) {
            assertEquals(Verbosity.TRACE, TraxConfiguration.verbosity(Thread.currentThread()));
        }

        try (TraxContext context = Trax.track("foo", "12345", "baz")) {
            assertEquals(Verbosity.DEBUG, TraxConfiguration.verbosity(Thread.currentThread()));
        }
    }

    /**
     * testConfigureOverrideVerbosity.
     */
    @Test
    public void testConfigureOverrideVerbosity() {
        TraxConfiguration.configureTopics("foo", Verbosity.OFF);
        TraxConfiguration.configureTopics("bar", Verbosity.TRACE);
        TraxConfiguration.configureTopics("12345", Verbosity.OFF);
        TraxConfiguration.configureTopics("baz", Verbosity.DEBUG);

        final TraxTopicConfiguration.Builder builder = TraxTopicConfiguration.newBuilder();
        try (TraxContext context = Trax.track(builder.setVerbosity(Verbosity.OFF).build(),
            "foo", "bar", "12345", "baz")) {
            assertEquals(Verbosity.OFF, TraxConfiguration.verbosity(Thread.currentThread()));
        }
        try (TraxContext context = Trax.track(builder.setVerbosity(Verbosity.DEBUG).build(),
            "foo", "bar", "12345", "baz")) {
            assertEquals(Verbosity.DEBUG, TraxConfiguration.verbosity(Thread.currentThread()));
        }
        try (TraxContext context = Trax.track(builder.setVerbosity(Verbosity.TRACE).build(),
            "foo", "bar", "12345", "baz")) {
            assertEquals(Verbosity.TRACE, TraxConfiguration.verbosity(Thread.currentThread()));
        }
    }

    /**
     * testConfigureMaxDecimalPlaces.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testConfigureMaxDecimalPlaces() throws Exception {
        TraxConfiguration.configureTopics("foo", Verbosity.DEBUG);

        final TraxTopicConfiguration.Builder builder = TraxTopicConfiguration.newBuilder();
        try (TraxContext c = Trax.track(builder.setMaxDecimalPlaces(2).build(), "foo")) {
            assertEquals("1.23\n", trax(1.234567).calculationStack());
        }

        try (TraxContext c = Trax.track(builder.setMaxDecimalPlaces(3).build(), "foo")) {
            assertEquals("1.235\n", trax(1.234567).calculationStack());
        }
    }

    /**
     * testDefaultTopic.
     */
    @Test
    public void testDefaultTopic() {
        TraxConfiguration.configureTopics(TraxConfiguration.DEFAULT_TOPIC_NAME, Verbosity.DEBUG);

        try (TraxContext context = Trax.track("foo")) {
            // Even outside of an explicit context, something in the DEFAULT scope can be tracked.
            assertEquals(
                "3[sum] = 1[foo] + 2[bar]\n" +
                "|__ 1[foo]\n" +
                "\\__ 2[bar]\n",
                trax(1, "foo").plus(2, "bar").compute("sum").calculationStack());
        }
    }


    /**
     * testConfigurationScopePrecedence.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testConfigurationScopePrecedence() throws Exception {
        TraxConfiguration.configureTopics("foo", Verbosity.DEBUG);

        try (TraxContext outer = Trax.track("foo")) {
            assertEquals(Verbosity.DEBUG, TraxConfiguration.verbosity(Thread.currentThread()));

            try (TraxContext inner = Trax.track(TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.OFF).build())) {
                assertEquals(Verbosity.DEBUG,
                    TraxConfiguration.verbosity(Thread.currentThread()));

                try (TraxContext innerInner = Trax.track(TraxTopicConfiguration.newBuilder()
                    .setVerbosity(Verbosity.TRACE).build())) {
                    assertEquals(Verbosity.TRACE, TraxConfiguration.verbosity(Thread.currentThread()));
                }

                assertEquals(Verbosity.DEBUG,
                    TraxConfiguration.verbosity(Thread.currentThread()));
            }

            assertEquals(Verbosity.DEBUG, TraxConfiguration.verbosity(Thread.currentThread()));
        }

        assertEquals(Verbosity.OFF, TraxConfiguration.verbosity(Thread.currentThread()));
    }

    /**
     * testExhaustedLimitIsNotUsed.
     */
    @Test
    public void testExhaustedLimitIsNotUsed() {
        final TraxLimit limit = Mockito.mock(TraxLimit.class);
        when(limit.isExhausted()).thenReturn(true);
        when(limit.toProto()).thenReturn(TrackingLimitRemainder.getDefaultInstance());

        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG).build(), limit, Collections.singleton("foo")));
        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.OFF, context.getVerbosity());
        }
    }

    /**
     * testExhaustedLimitIsCleared.
     */
    @Test
    public void testExhaustedLimitIsCleared() {
        final TraxLimit limit = Mockito.mock(TraxLimit.class);
        when(limit.isExhausted()).thenReturn(true);
        when(limit.toProto()).thenReturn(TrackingLimitRemainder.getDefaultInstance());

        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG).build(), limit, Collections.singleton("foo")));

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.OFF, context.getVerbosity());
        }

        assertEquals(0, TraxConfiguration.getTrackingTopics().size());
    }

    /**
     * testUseLimitIsDecrementedWithUse.
     */
    @Test
    public void testUseLimitIsDecrementedWithUse() {
        final TraxLimit limit = new TraxUseLimit(1);

        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG).build(), limit, Collections.singleton("foo")));

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.DEBUG, context.getVerbosity());
        }
        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.OFF, context.getVerbosity());
        }
    }

    /**
     * testSharedUseLimit.
     */
    @Test
    public void testSharedUseLimit() {
        final TraxUseLimit limit = new TraxUseLimit(2);

        TraxConfiguration.configureTopics(new TopicSettings(TraxTopicConfiguration.newBuilder()
            .setVerbosity(Verbosity.DEBUG).build(), limit, Arrays.asList("foo", "bar")));

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.DEBUG, context.getVerbosity());
        }

        assertEquals(1, limit.getRemainingUseLimit());

        try (TraxContext context = Trax.track("bar")) {
            assertEquals(Verbosity.DEBUG, context.getVerbosity());
        }

        assertEquals(0, limit.getRemainingUseLimit());

        try (TraxContext context = Trax.track("foo")) {
            assertEquals(Verbosity.OFF, context.getVerbosity());
        }

        // Check that now that both topics are exhausted, they are cleared.
        assertEquals(0, TraxConfiguration.getTrackingTopics().size());
    }
}
