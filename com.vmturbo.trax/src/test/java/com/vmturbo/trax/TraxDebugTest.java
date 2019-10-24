package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;
import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.vmturbo.common.protobuf.trax.Trax.TraxTopicConfiguration.Verbosity;
import com.vmturbo.trax.TraxConfiguration.TraxContext;

/**
 * Tests for {@link Trax} in debug mode.
 */
public class TraxDebugTest {

    /**
     * setupTraxConfiguration.
     */
    @BeforeClass
    public static void setupTraxConfiguration() {
        TraxConfiguration.configureTopics(TraxDebugTest.class.getName(), Verbosity.DEBUG);
    }

    /**
     * clearTraxConfiguration.
     */
    @AfterClass
    public static void clearTraxConfiguration() {
        TraxConfiguration.clearAllConfiguration();
    }

    /**
     * testConstant.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testSimple() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final String str = trax(5.0, "foo").calculationStack();
            Assert.assertEquals("5[foo]\n", str);
        }
    }

    /**
     * testCalculation.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCalculation() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final String str = trax(5.0, "foo")
                .plus(10.0, "bar")
                .compute("addition")
                .calculationStack();
            Assert.assertEquals(
                "15[addition] = 5[foo] + 10[bar]\n" +
                "|__ 5[foo]\n" +
                "\\__ 10[bar]\n",
                str);
        }
    }

    /**
     * testCalculation.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testNaming() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final String str = trax(5.0, "foo")
                .named("bar")
                .calculationStack();
            Assert.assertEquals(
                "5[bar] = 5[foo]\n" +
                    "\\__ 5[foo]\n",
                str);
        }
    }

    /**
     * testNested.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testNested() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sub = trax(1.0, "a")
                .minus(2.0, "b", "This number comes from customer-specific rate card")
                .compute("sub");
            final TraxNumber div = trax(4.0, "d").dividedBy(6.0, "e").compute("div");
            final TraxNumber add = div.plus(sub).compute("result");

            assertEquals(
                "-0.333333[result] = 0.666667[div] + -1[sub]\n" +
                    "|__ 0.666667[div] = 4[d] / 6[e]\n" +
                    "|   |__ 4[d]\n" +
                    "|   \\__ 6[e]\n" +
                    "\\__ -1[sub] = 1[a] - 2[b]\n" +
                    "    |__ 1[a]\n" +
                    "    \\__ 2[b]\n",
                add.calculationStack());
        }
    }

    /**
     * testDoublyNested.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testDoublyNested() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sub = trax(1.0, "a")
                .minus(2.0, "b", "This number comes from customer-specific rate card")
                .compute("sub");
            final TraxNumber div = trax(4.0, "d").dividedBy(6.0, "e").compute("div");
            final TraxNumber add = div.plus(sub).compute("add");
            final TraxNumber mul = trax(6.0, "quantity").times(add).compute("result");

            assertEquals(
                "-2[result] = 6[quantity] x -0.333333[add]\n" +
                    "|__ 6[quantity]\n" +
                    "\\__ -0.333333[add] = 0.666667[div] + -1[sub]\n" +
                    "    |__ 0.666667[div] = 4[d] / 6[e]\n" +
                    "    |   |__ 4[d]\n" +
                    "    |   \\__ 6[e]\n" +
                    "    \\__ -1[sub] = 1[a] - 2[b]\n" +
                    "        |__ 1[a]\n" +
                    "        \\__ 2[b]\n",
                mul.calculationStack());
        }
    }

    /**
     * testOneElementMinimum.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOneElementMinimum() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber a = trax(1.0, "a");
            final TraxNumber min = Trax.min(a).compute("min");

            assertEquals(
                "1[min] = min(1[a])\n" +
                    "\\__ 1[a]\n",
                min.calculationStack());
        }
    }

    /**
     * testTwoElementMinimum.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testTwoElementMinimum() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sub = trax(1.0, "a")
                .minus(2.0, "b", "This number comes from customer-specific rate card")
                .compute("sub");
            final TraxNumber div = trax(4.0, "d").dividedBy(6.0, "e").compute("div");
            final TraxNumber min = Trax.min(sub, div).compute("min");

            assertEquals(
                "-1[min] = min(-1[sub], 0.666667[div])\n" +
                "|__ -1[sub] = 1[a] - 2[b]\n" +
                "|   |__ 1[a]\n" +
                "|   \\__ 2[b]\n" +
                "\\__ 0.666667[div] = 4[d] / 6[e]\n" +
                "    |__ 4[d]\n" +
                "    \\__ 6[e]\n",
                min.calculationStack());
        }
    }

    /**
     * testMultipleMinimum.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMultipleMinimum() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber min = Trax.min(
                trax(1.0, "a"),
                trax(2.0, "b"),
                trax(3.0, "c"),
                trax(4.0, "d")
            ).compute("min");

            assertEquals(
                "1[min] = min(1[a], 2[b], 3[c], 4[d])\n" +
                    "|__ 1[a]\n" +
                    "|__ 2[b]\n" +
                    "|__ 3[c]\n" +
                    "\\__ 4[d]\n",
                min.calculationStack());
        }
    }

    /**
     * testMinOrDefaultNoElements.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMinOrDefaultNoElements() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber min = Trax.minOrDefault(Collections.emptyList(), trax(1, "my default"))
                .compute("min");

            assertEquals(
                "1[min] = min()\n" +
                    "\\__ 1[my default]\n",
                min.calculationStack());
        }
    }

    /**
     * testMinOrDefaultOneElement.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMinOrDefaultOneElement() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber min = Trax.minOrDefault(Collections.singletonList(trax(2, "value")), trax(1, "my default"))
                .compute("min");

            assertEquals(
                "2[min] = min(2[value])\n" +
                    "\\__ 2[value]\n",
                min.calculationStack());
        }
    }

    /**
     * testMinOrDefaultMultipleElements.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMinOrDefaultMultipleElements() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber min = Trax.minOrDefault(
                Arrays.asList(trax(2, "foo"),
                    trax(1.5, "bar"),
                    trax(3, "baz")),
                trax(1, "my default"))
                .compute("min");

            assertEquals(
                "1.5[min] = min(2[foo], 1.5[bar], 3[baz])\n" +
                    "|__ 2[foo]\n" +
                    "|__ 1.5[bar]\n" +
                    "\\__ 3[baz]\n",
                min.calculationStack());
        }
    }

    /**
     * testOneElementMaximum.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testOneElementMaximum() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber a = trax(1.0, "a");
            final TraxNumber max = Trax.min(a).compute("max");

            assertEquals(
                "1[max] = min(1[a])\n" +
                    "\\__ 1[a]\n",
                max.calculationStack());
        }
    }

    /**
     * testTwoElementMaximum.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testTwoElementMaximum() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sub = trax(1.0, "a")
                .minus(2.0, "b", "This number comes from customer-specific rate card")
                .compute("sub");
            final TraxNumber div = trax(4.0, "d").dividedBy(6.0, "e").compute("div");
            final TraxNumber max = Trax.max(sub, div).compute("max");

            assertEquals(
                "0.666667[max] = max(-1[sub], 0.666667[div])\n" +
                "|__ -1[sub] = 1[a] - 2[b]\n" +
                "|   |__ 1[a]\n" +
                "|   \\__ 2[b]\n" +
                "\\__ 0.666667[div] = 4[d] / 6[e]\n" +
                "    |__ 4[d]\n" +
                "    \\__ 6[e]\n",
                max.calculationStack());
        }
    }

    /**
     * testMultipleMaximum.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMultipleMaximum() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber max = Trax.max(
                trax(1.0, "a"),
                trax(2.0, "b"),
                trax(3.0, "c"),
                trax(4.0, "d")
            ).compute("max");

            assertEquals(
                "4[max] = max(1[a], 2[b], 3[c], 4[d])\n" +
                    "|__ 1[a]\n" +
                    "|__ 2[b]\n" +
                    "|__ 3[c]\n" +
                    "\\__ 4[d]\n",
                max.calculationStack());
        }
    }

    /**
     * testMaxOrDefaultNoElements.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMaxOrDefaultNoElements() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber max = Trax.maxOrDefault(Collections.emptyList(), trax(1, "my default"))
                .compute("max");

            assertEquals(
                "1[max] = max()\n" +
                    "\\__ 1[my default]\n",
                max.calculationStack());
        }
    }

    /**
     * testMaxOrDefaultOneElement.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMaxOrDefaultOneElement() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber max = Trax.maxOrDefault(Collections.singletonList(trax(2, "value")),
                trax(1, "my default"))
                .compute("max");

            assertEquals(
                "2[max] = max(2[value])\n" +
                    "\\__ 2[value]\n",
                max.calculationStack());
        }
    }

    /**
     * testMaxOrDefaultMultipleElements.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testMaxOrDefaultMultipleElements() throws Exception {
        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber max = Trax.maxOrDefault(
                Arrays.asList(
                    trax(2, "foo"),
                    trax(1.5, "bar"),
                    trax(3, "baz")),
                trax(1, "my default"))
                .compute("max");

            assertEquals(
                "3[max] = max(2[foo], 1.5[bar], 3[baz])\n" +
                    "|__ 2[foo]\n" +
                    "|__ 1.5[bar]\n" +
                    "\\__ 3[baz]\n",
                max.calculationStack());
        }
    }

    /**
     * testConstant.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testConstant() throws Exception {
        assertEquals("1.01[my_constant]\n",
            Trax.traxConstant(1.01, "my_constant").calculationStack());
    }

    /**
     * testCalculationWithConstant.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testCalculationWithConstant() throws Exception {
        final TraxNumber constant = Trax.traxConstant(1.01, "my_constant");

        try (TraxContext context = Trax.track(getClass().getName())) {
            final TraxNumber sum = trax(0.5, "value").plus(constant).compute("sum");
            // Even though we created the constant number outside of the context
            // when debug verbosity was not enabled, we should still see its name because
            // constant capture debug verbosity level calculation details.
            assertEquals("1.51[sum] = 0.5[value] + 1.01[my_constant]\n" +
                "|__ 0.5[value]\n" +
                "\\__ 1.01[my_constant]\n",
                sum.calculationStack());
        }
    }
}