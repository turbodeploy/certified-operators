package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration;
import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.trax.TraxConfiguration.TraxContext;

/**
 * Tests for {@link Trax} at trace verbosity level.
 */
public class TraxTraceTest {

    /**
     * Setup at Trace verbosity before tests.
     */
    @BeforeClass
    public static void setupTraxConfiguration() {
        TraxConfiguration.configureTopics(TraxTraceTest.class.getName(), Verbosity.TRACE);
    }

    /**
     * Clear configurations after tests.
     */
    @AfterClass
    public static void clearTraxConfiguration() {
        TraxConfiguration.clearAllConfiguration();
    }

    /**
     * testSimple.
     */
    @Test
    public void testSimple() {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final String str = trax(5.0, "foo").calculationStack();
            assertThat(str, MultilineEscapingRegexMatcher.matchesPattern("5[foo] (TraxTraceTest.java:\\d+)"));
        }
    }

    /**
     * testCalculation.
     */
    @Test
    public void testCalculation() {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final String str = trax(5.0, "foo")
                .plus(10.0, "bar")
                .compute("addition")
                .calculationStack();

            assertThat(str, MultilineEscapingRegexMatcher.matchesPattern(
                "15[addition] = 5[foo] + 10[bar] (TraxTraceTest.java:\\d+)\n" +
                    "|__ 5[foo] (TraxTraceTest.java:\\d+)\n" +
                    "\\__ 10[bar] (TraxTraceTest.java:\\d+)\n"));
        }
    }

    /**
     * testNested.
     */
    @Test
    public void testNested() {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sub = trax(1.0, "a")
                .minus(2.0, "b", "This number comes from customer-specific rate card")
                .compute("sub");
            final TraxNumber div = trax(4.0, "d").dividedBy(6.0, "e").compute("div");
            final TraxNumber add = div.plus(sub).compute("result");

            assertThat(add.calculationStack(), MultilineEscapingRegexMatcher.matchesPattern(
                "-0.333333[result] = 0.666667[div] + -1[sub] (TraxTraceTest.java:\\d+)\n" +
                "|__ 0.666667[div] = 4[d] / 6[e] (TraxTraceTest.java:\\d+)\n" +
                "|   |__ 4[d] (TraxTraceTest.java:\\d+)\n" +
                "|   \\__ 6[e] (TraxTraceTest.java:\\d+)\n" +
                "\\__ -1[sub] = 1[a] - 2[b] (TraxTraceTest.java:\\d+)\n" +
                "    |__ 1[a] (TraxTraceTest.java:\\d+)\n" +
                "    \\__ 2[b] (This number comes from customer-specific rate card)\n"));
        }
    }

    /**
     * testDoublyNested.
     */
    @Test
    public void testDoublyNested() {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sub = trax(1.0, "a")
                .minus(2.0, "b", "This number comes from customer-specific rate card")
                .compute("sub");
            final TraxNumber div = trax(4.0, "d").dividedBy(6.0, "e").compute("div");
            final TraxNumber add = div.plus(sub).compute("add");
            final TraxNumber mul = trax(6.0, "quantity").times(add).compute("result");

            assertThat(mul.calculationStack(), MultilineEscapingRegexMatcher.matchesPattern(
                "-2[result] = 6[quantity] x -0.333333[add] (TraxTraceTest.java:\\d+)\n" +
                    "|__ 6[quantity] (TraxTraceTest.java:\\d+)\n" +
                    "\\__ -0.333333[add] = 0.666667[div] + -1[sub] (TraxTraceTest.java:\\d+)\n" +
                    "    |__ 0.666667[div] = 4[d] / 6[e] (TraxTraceTest.java:\\d+)\n" +
                    "    |   |__ 4[d] (TraxTraceTest.java:\\d+)\n" +
                    "    |   \\__ 6[e] (TraxTraceTest.java:\\d+)\n" +
                    "    \\__ -1[sub] = 1[a] - 2[b] (TraxTraceTest.java:\\d+)\n" +
                    "        |__ 1[a] (TraxTraceTest.java:\\d+)\n" +
                    "        \\__ 2[b] (This number comes from customer-specific rate card)\n"));
        }
    }

    /**
     * testConfigurationScope.
     */
    @Test
    public void testConfigurationScope() {
        // OFF scope
        final List<TraxNumber> numbers = new ArrayList<>();
        final TraxNumber a = trax(1.0, "a");
        numbers.add(a);

        try (TraxContext dbgContext = Trax.track(Thread.currentThread(),
            // DEBUG scope
            TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.DEBUG).build(), getClass().getName())) {
            final TraxNumber c = trax(2.0, "b").plus(a).compute("c", "no show description");
            numbers.add(c);

            try (TraxContext traceContext = Trax.track(Thread.currentThread(),
                // TRACE scope
                TraxTopicConfiguration.newBuilder().setVerbosity(Verbosity.TRACE).build(), getClass().getName())) {
                final TraxNumber min = Trax.min(trax(4.0, "d"), c)
                    .compute("min", "trace details");
                numbers.add(min);

                assertThat(min.calculationStack(), MultilineEscapingRegexMatcher.matchesPattern(
                    "3[min] = min(4[d], 3[c]) (trace details)\n" + // Within trace context, has details
                        "|__ 4[d] (TraxTraceTest.java:\\d+)\n" +
                        "\\__ 3[c] = 2[b] + 1\n" + // b,c are reated within debug context, have names but no details
                        "    |__ 2[b]\n" +
                        "    \\__ 1\n" // Created outside the contexts - no info preserved, not even name
                ));
            }

            // DEBUG scope again
            numbers.add(c.minus(2.0, "e").compute("f", "do not show details"));
            assertEquals("8[sum] = 1 + 3[c] + 3[min] + 1[f]\n" +
                    "|__ 1\n" +
                    "|__ 3[c] = 2[b] + 1\n" +
                    "|   |__ 2[b]\n" +
                    "|   \\__ 1\n" +
                    "|__ 3[min] = min(4[d], 3[c])\n" +
                    "|   |__ 4[d]\n" +
                    "|   \\__ 3[c] = 2[b] + 1\n" +
                    "|       |__ 2[b]\n" +
                    "|       \\__ 1\n" +
                    "\\__ 1[f] = 3[c] - 2[e]\n" +
                    "    |__ 3[c] = 2[b] + 1\n" +
                    "    |   |__ 2[b]\n" +
                    "    |   \\__ 1\n" +
                    "    \\__ 2[e]\n",
                numbers.stream()
                    .collect(TraxCollectors.sum("sum"))
                    .calculationStack());
        }

        numbers.add(trax(0.5, "OFF"));
        assertEquals("8.5", numbers.stream()
            .collect(TraxCollectors.sum("sum"))
            .calculationStack());
    }
}