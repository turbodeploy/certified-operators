package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.After;
import org.junit.Test;

import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.trax.TraxConfiguration.TraxContext;

/**
 * Tests for {@link TraxCollectors}.
 */
public class TraxCollectorsTest {

    private static final double DOUBLE_DELTA = 1e-8;

    /**
     * Clear Trax configurations after tests.
     */
    @After
    public void clearTraxConfiguration() {
        TraxConfiguration.clearAllConfiguration();
    }

    private void enableTraxDebug() {
        enableTrax(Verbosity.DEBUG);
    }

    private void enableTrax(@Nonnull final Verbosity verbosity) {
        TraxConfiguration.configureTopics(getClass().getName(), verbosity);
    }

    /**
     * testEmptySummationOffVerbosity.
     */
    @Test
    public void testEmptySummationOffVerbosity() {
        final TraxNumber number = Stream.<TraxNumber>empty()
            .collect(TraxCollectors.sum());

        assertEquals(0, number.getValue(), 0);
    }

    /**
     * testEmptySummationDebugVerbosity.
     */
    @Test
    public void testEmptySummationDebugVerbosity() {
        enableTraxDebug();

        final TraxNumber number = Stream.<TraxNumber>empty()
            .collect(TraxCollectors.sum());

        assertEquals(0, number.getValue(), 0);
    }

    /**
     * testSingleElementSummationOffVerbosity.
     */
    @Test
    public void testSingleElementSummationOffVerbosity() {
        final TraxNumber number = Stream.of(trax(2.0, "foo"))
            .collect(TraxCollectors.sum());

        assertEquals(2.0, number.getValue(), 0);
    }

    /**
     * testSingleElementSummationDebugVerbosity.
     */
    @Test
    public void testSingleElementSummationDebugVerbosity() {
        withDebugEnabled(() -> {
            final TraxNumber number = Stream.of(trax(2.0, "foo"))
                .collect(TraxCollectors.sum("bar"));

            assertEquals(2.0, number.getValue(), 0);
            assertEquals(
                "2[bar] = 2[foo]\n" +
                "\\__ 2[foo]\n",
                number.calculationStack());
        });
    }

    /**
     * testTwoElementSummationOffVerbosity.
     */
    @Test
    public void testTwoElementSummationOffVerbosity() {
        final TraxNumber number = Stream.of(
            trax(2.0, "foo"),
            trax(1.0, "bar")
        ).collect(TraxCollectors.sum());

        assertEquals(3.0, number.getValue(), DOUBLE_DELTA);
    }

    /**
     * testTwoElementSummationDebugVerbosity.
     */
    @Test
    public void testTwoElementSummationDebugVerbosity() {
        withDebugEnabled(() -> {
            final TraxNumber number = Stream.of(
                trax(2.0, "foo"),
                trax(1.0, "bar")
            ).collect(TraxCollectors.sum("result"));

            assertEquals(3.0, number.getValue(), DOUBLE_DELTA);
            assertEquals(
                "3[result] = 2[foo] + 1[bar]\n" +
                "|__ 2[foo]\n" +
                "\\__ 1[bar]\n",
                number.calculationStack());
        });
    }

    /**
     * testMultipleElementSummationOffVerbosity.
     */
    @Test
    public void testMultipleElementSummationOffVerbosity() {
        final TraxNumber number = Stream.of(
            trax(2.0, "foo"),
            trax(1.0, "bar"),
            trax(0.5, "baz")
        ).collect(TraxCollectors.sum());

        assertEquals(3.5, number.getValue(), DOUBLE_DELTA);
    }

    /**
     * testMultipleElementSummationDebugVerbosity.
     */
    @Test
    public void testMultipleElementSummationDebugVerbosity() {
        withDebugEnabled(() -> {
            final TraxNumber number = Stream.of(
                trax(2.0, "foo"),
                trax(1.0, "bar"),
                trax(0.5, "baz")
            ).collect(TraxCollectors.sum("result"));

            assertEquals(3.5, number.getValue(), DOUBLE_DELTA);
            assertEquals(
                "3.5[result] = 2[foo] + 1[bar] + 0.5[baz]\n" +
                    "|__ 2[foo]\n" +
                    "|__ 1[bar]\n" +
                    "\\__ 0.5[baz]\n",
                number.calculationStack());
        });
    }

    /**
     * testNestedMultipleElementSummationOffVerbosity.
     */
    @Test
    public void testNestedMultipleElementSummationOffVerbosity() {
        final TraxNumber number = Stream.of(
            trax(2.0, "foo"),
            trax(1.0, "bar"),
            trax(0.5, "baz"),
            trax(0.25, "quux").plus(0.25, "frob").compute("inner")
        ).collect(TraxCollectors.sum());

        assertEquals(4.0, number.getValue(), DOUBLE_DELTA);
    }

    /**
     * testNestedMultipleElementSummationDebugVerbosity.
     */
    @Test
    public void testNestedMultipleElementSummationDebugVerbosity() {
        withDebugEnabled(() -> {
            final TraxNumber number = Stream.of(
                trax(2.0, "foo"),
                trax(1.0, "bar"),
                trax(0.5, "baz"),
                trax(0.25, "quux").plus(0.25, "frob").compute("inner")
            ).collect(TraxCollectors.sum());

            assertEquals(4.0, number.getValue(), DOUBLE_DELTA);
            assertEquals(
                "4 = 2[foo] + 1[bar] + 0.5[baz] + 0.5[inner]\n" +
                    "|__ 2[foo]\n" +
                    "|__ 1[bar]\n" +
                    "|__ 0.5[baz]\n" +
                    "\\__ 0.5[inner] = 0.25[quux] + 0.25[frob]\n" +
                    "    |__ 0.25[quux]\n" +
                    "    \\__ 0.25[frob]\n",
                number.calculationStack());
        });
    }

    /**
     * testNestedMultipleElementSummationTraceVerbosity.
     */
    @Test
    public void testNestedMultipleElementSummationTraceVerbosity() {
        withTraxEnabled(Verbosity.TRACE, () -> {
            final TraxNumber number = Stream.of(
                trax(2.0, "foo"),
                trax(1.0, "bar"),
                trax(0.5, "baz"),
                trax(0.25, "quux").plus(0.25, "frob").compute("inner")
            ).collect(TraxCollectors.sum());

            assertEquals(4.0, number.getValue(), DOUBLE_DELTA);
            assertThat(number.calculationStack(), MultilineEscapingRegexMatcher.matchesPattern(
                "4 = 2[foo] + 1[bar] + 0.5[baz] + 0.5[inner] (TraxCollectorsTest.java:\\d+)\n" +
                    "|__ 2[foo] (TraxCollectorsTest.java:\\d+)\n" +
                    "|__ 1[bar] (TraxCollectorsTest.java:\\d+)\n" +
                    "|__ 0.5[baz] (TraxCollectorsTest.java:\\d+)\n" +
                    "\\__ 0.5[inner] = 0.25[quux] + 0.25[frob] (TraxCollectorsTest.java:\\d+)\n" +
                    "    |__ 0.25[quux] (TraxCollectorsTest.java:\\d+)\n" +
                    "    \\__ 0.25[frob] (TraxCollectorsTest.java:\\d+)"));
        });
    }

    private void withDebugEnabled(@Nonnull final Runnable testcode) {
        withTraxEnabled(Verbosity.DEBUG, testcode);
    }

    private void withTraxEnabled(@Nonnull final Verbosity verbosity,
                                 @Nonnull final Runnable testcode) {
        enableTrax(verbosity);
        try (TraxContext context = Trax.track(getClass().getName())) {
            testcode.run();
        }
    }
}
