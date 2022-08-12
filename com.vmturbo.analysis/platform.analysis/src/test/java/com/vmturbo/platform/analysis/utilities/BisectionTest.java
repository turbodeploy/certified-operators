package com.vmturbo.platform.analysis.utilities;

import static com.vmturbo.platform.analysis.utilities.Bisection.asDouble;
import static com.vmturbo.platform.analysis.utilities.Bisection.asLong;
import static java.lang.Math.E;
import static java.lang.Math.PI;
import static java.lang.Math.exp;
import static java.lang.Math.sin;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.function.DoubleUnaryOperator;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;

/**
 * A test suite for the {@link Bisection} class.
 *
 * Individual test cases are written in a way that will potentially allow reusing them for other
 * root-finding methods.
 */
@RunWith(JUnitParamsRunner.class)
public class BisectionTest {
    // Fields
    private static final double maxError = 1e-10;

    // Methods

    /**
     * Feeds {@link Bisection#asLong(double)} valid input and compares output against expected one.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Bisection.asLong({0}) == {1}")
    public final void testAsLong(double input, long output) {
        assertEquals(output, asLong(input));
    }

    private static Object[] parametersForTestAsLong() {
        final Object[][] testCases = {
            {Double.POSITIVE_INFINITY, 0x7FF0_0000_0000_0000L},
            {Double.MAX_VALUE, 0x7FEF_FFFF_FFFF_FFFFL},
            {Double.MIN_NORMAL, 0x0010_0000_0000_0000L},
            {Math.nextAfter(Double.MIN_NORMAL,0.0), 0x000F_FFFF_FFFF_FFFFL},
            {Double.MIN_VALUE, 0x0000_0000_0000_0001L},
            {2.0, 0x4000_0000_0000_0000L},
            {1.0, 0x3FF0_0000_0000_0000L},
            {0.0, 0L},
            {Double.NaN, Double.doubleToRawLongBits(Double.NaN)},
        };

        // Test the negative values as well.
        return Stream.concat(Arrays.stream(testCases), Arrays.stream(testCases).map(
            testCase -> new Object[]{-(double)testCase[0], -(long)testCase[1]}
        )).toArray();
    }

    /**
     * Feeds {@link Bisection#asDouble(long)} valid input and compares output against expected one.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Bisection.asDouble({0}) == {1}")
    public final void testAsDouble(long input, double output) {
        assertEquals(output, asDouble(input), 0.0);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAsDouble() {
        // swap input and output and replace input for negative 0.0.
        return Arrays.stream(parametersForTestAsLong())
            .map(testCase ->
                Double.doubleToRawLongBits((double)((Object[])testCase)[0]) == 0x8000_0000_0000_0000L
                ? new Object[]{0x8000_0000_0000_0000L, -0.0}
                : new Object[]{((Object[]) testCase)[1], ((Object[]) testCase)[0]}
            ).toArray();
    }

    /**
     * Tests {@link Bisection#asLong(double)}'s order preservation property on pairs of floating-
     * point numbers.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0} < {1} => Bisection.asLong({0}) < Bisection.asLong({1})")
    public final void testAsLong_Order_Preservation(double x1, double x2) {
        if (x1 < x2) {
            assertTrue(asLong(x1) < asLong(x2));
        } else if (x1 == x2) {
            assertEquals(asLong(x1), asLong(x2));
        } else if(x1 > x2){
            assertTrue(asLong(x1) > asLong(x2));
        } // else at least one of x1, x2 is NaN
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestAsLong_Order_Preservation() {
        Object[] numbers = Arrays.stream(parametersForTestAsLong())
            .map(testCase -> ((Object[]) testCase)[0]) // isolate first parameter
            .filter(testInput -> !Double.isNaN((double)testInput)).toArray();// NaNs are not ordered

        // Note: the original plan was to test some random pairs as well, but adding e.g. 20 numbers
        // (or ~ doubling the list or ~ quadrupling the number of tests) exceeds a threshold after
        // which the testing framework becomes sluggish (we go from ~3s to ~1m to run the tests).

        // generate combinations with repetition
        Object[][] testCases = new Object[numbers.length*(numbers.length+1)/2][2];
        int k = 0;
        for (int i = 0 ; i < numbers.length ; i++) {
            for (int j = i ; j < numbers.length ; j++) {
                testCases[k][0] = numbers[i];
                testCases[k][1] = numbers[j];
                ++k;
            }
        }

        return testCases;
    }

    /**
     * Feeds {@link Bisection#solve(DoubleUnaryOperator, double, double)}
     * valid input and tests that it returns the unique root present in the interval.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Bisection.solve({0},{1},{2}) == {3}")
    public final void testSolve_Positive_OneRoot(DoubleUnaryOperator function, double a, double b, double output) {
        assertEquals(output, Bisection.solve(function, a, b), maxError);
    }

    /**
     * Good candidates for testing:
     *
     * - Functions increasing vs decreasing in the given interval.
     * - Interval endpoint signs: -/-, -/+, +/+.
     * - Potential overflow when endpoints have high magnitude and the following sign combinations:
     *   -/-, -/+, +/+.
     * - Intervals where the root is the midpoint vs arbitrary intervals. In general, cases where
     *   root = k * (b-a)/2^n + a for natural k and n are of special interest.
     * - Combinations of the above.
     */
    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSolve_Positive_OneRoot() {
        return new Object[][]{
            // sin -- root == midpoint
            {(DoubleUnaryOperator)Math::sin,       -PI/2,   PI/2, 0.0},
            {(DoubleUnaryOperator)Math::sin,     -3*PI/2,  -PI/2, -PI},
            {(DoubleUnaryOperator)Math::sin,        PI/2, 3*PI/2,  PI},
            // -sin -- root == midpoint
            {(DoubleUnaryOperator)x -> - sin(x),   -PI/2,   PI/2, 0.0},
            {(DoubleUnaryOperator)x -> - sin(x), -3*PI/2,  -PI/2, -PI},
            {(DoubleUnaryOperator)x -> - sin(x),    PI/2, 3*PI/2,  PI},
            // sin -- arbitrary root
            {(DoubleUnaryOperator)Math::sin, -1, 2, 0.0},
            {(DoubleUnaryOperator)Math::sin, -4, -2, -PI},
            {(DoubleUnaryOperator)Math::sin,  3, 4, PI},
            // -sin -- arbitrary root
            {(DoubleUnaryOperator)x -> - sin(x), -2.5, 1.5, 0.0},
            {(DoubleUnaryOperator)x -> - sin(x), -5.7, -2.1, -PI},
            {(DoubleUnaryOperator)x -> - sin(x), 0.001, 3.2,  PI},
            // exp -- 0/+
            {(DoubleUnaryOperator)x -> exp(x) - E, 0, 2, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, 0, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, 0, Double.POSITIVE_INFINITY, 1},
            // exp -- -/0
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -100, 0, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, 0, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, 0, -1},
            // TODO: more concisely express using loops.
            // exp -- -/+ -- root == 1
            {(DoubleUnaryOperator)x -> exp(x) - E, -5, 2, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, -5, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, -5, Double.POSITIVE_INFINITY, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, -Double.MAX_VALUE, 2, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, -Double.MAX_VALUE, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, Double.NEGATIVE_INFINITY, 2, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1},
            // exp -- -/+ -- root == -1
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -5, 2, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -5, Double.MAX_VALUE, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -5, Double.POSITIVE_INFINITY, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, 2, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, Double.MAX_VALUE, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, 2, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1},
            // -exp -- -/+ -- root == 1
            {(DoubleUnaryOperator)x -> -exp(x) + E, -5, 2, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, -5, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, -5, Double.POSITIVE_INFINITY, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, -Double.MAX_VALUE, 2, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, -Double.MAX_VALUE, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, Double.NEGATIVE_INFINITY, 2, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1},
            // -exp -- -/+ -- root == -1
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -5, 2, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -5, Double.MAX_VALUE, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -5, Double.POSITIVE_INFINITY, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -Double.MAX_VALUE, 2, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -Double.MAX_VALUE, Double.MAX_VALUE, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, Double.NEGATIVE_INFINITY, 2, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1},
            // x - c -- +/+
            {(DoubleUnaryOperator)x -> x - 42, 1, 100, 42},
            {(DoubleUnaryOperator)x -> x - 0.9*Double.MAX_VALUE, 1, Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> x - 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, 0.95*Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> x - 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, Double.POSITIVE_INFINITY, 0.9*Double.MAX_VALUE},
            // c - x -- +/+
            {(DoubleUnaryOperator)x -> -x + 42, 1, 100, 42},
            {(DoubleUnaryOperator)x -> -x + 0.9*Double.MAX_VALUE, 1, Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> -x + 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, 0.95*Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> -x + 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, Double.POSITIVE_INFINITY, 0.9*Double.MAX_VALUE},
            // x - c -- -/-
            {(DoubleUnaryOperator)x -> x + 42, -100, -1, -42},
            {(DoubleUnaryOperator)x -> x + 0.9*Double.MAX_VALUE, -Double.MAX_VALUE, -1, -0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> x + 0.9*Double.MAX_VALUE, -0.95*Double.MAX_VALUE, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> x + 0.9*Double.MAX_VALUE, Double.NEGATIVE_INFINITY, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},
            // c - x -- -/-
            {(DoubleUnaryOperator)x -> -x - 42, -100, -1, -42},
            {(DoubleUnaryOperator)x -> -x - 0.9*Double.MAX_VALUE, -Double.MAX_VALUE, -1, -0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> -x - 0.9*Double.MAX_VALUE, -0.95*Double.MAX_VALUE, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},
            {(DoubleUnaryOperator)x -> -x - 0.9*Double.MAX_VALUE, Double.NEGATIVE_INFINITY, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},

            // miscellaneous -/+
            {(DoubleUnaryOperator)x -> x, -PI/2, PI/2, 0.0},
            {(DoubleUnaryOperator)x -> x-1, -1e300, 1e300, 1.0},
            {(DoubleUnaryOperator)x -> x+1, -Double.MAX_VALUE, Double.MAX_VALUE, -1.0},
            {(DoubleUnaryOperator)x -> x, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0},
            {(DoubleUnaryOperator)x -> x*x*x, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0},
            {(DoubleUnaryOperator)Math::log, Double.MIN_VALUE, Double.POSITIVE_INFINITY, 1},
            {(DoubleUnaryOperator)x -> 1/x - 1, 0.0, Double.POSITIVE_INFINITY, 1},
            {(DoubleUnaryOperator)x -> 1/x + 1, Double.NEGATIVE_INFINITY, -0.0, -1},
            // price function
            {(DoubleUnaryOperator)u -> u * PriceFunctionFactory.createStandardWeightedPriceFunction(1.0)
                .unitPrice(u, null, null, null, null) - 7.777777777, 0.01, 0.99, 0.7},
        };
    }

    /**
     * Feeds {@link Bisection#solve(DoubleUnaryOperator, double, double)}
     * valid input and tests that its result is a root.
     *
     * When there are multiple roots in the interval, it is implementation-dependent which one the
     * root-finding method will converge to, even between bisection implementations. So we just
     * check the property of the result (that it's a root) instead of the specific value.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: {0}(Bisection.solve({0},{1},{2})) == 0.0")
    public final void testSolve_Positive_ManyRoots(DoubleUnaryOperator function, double a, double b) {
        assertEquals(0.0, function.applyAsDouble(Bisection.solve(function, a, b)), maxError);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSolve_Positive_ManyRoots() {
        return new Object[][]{
            {(DoubleUnaryOperator)x ->  (x-2)*x*(x+2),  -3,  3},
            {(DoubleUnaryOperator)x -> -(x-2)*x*(x+2), -30, 30},
            {(DoubleUnaryOperator)Math::sin, -33, 33},
            {(DoubleUnaryOperator)Math::cos, -12, 42},
        };
    }

    /**
     * Feeds {@link Bisection#solve(DoubleUnaryOperator, double, double)}
     * invalid input and tests that the proper exception is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: Bisection.solve({0},{1},{2})")
    public final void testSolve_Negative(DoubleUnaryOperator function, double a, double b) {
        Bisection.solve(function, a, b);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSolve_Negative() {
        return new Object[][]{
            // NaN argument(s)
            {(DoubleUnaryOperator) Math::sin, 0, Double.NaN},
            {(DoubleUnaryOperator) Math::sin, Double.NaN, 0},
            {(DoubleUnaryOperator) Math::sin, Double.NaN, Double.NaN},
            {(DoubleUnaryOperator) x -> x, 0, Double.NaN},
            {(DoubleUnaryOperator) x -> x, Double.NaN, 0},
            {(DoubleUnaryOperator) x -> x, Double.NaN, Double.NaN},
            // left endpoint >= right endpoint
            {(DoubleUnaryOperator) Math::cos, 1, 1},
            {(DoubleUnaryOperator) Math::cos, PI, -PI},
            {(DoubleUnaryOperator) Math::cos, 3.14, 3},
            {(DoubleUnaryOperator) Math::cos, -1.9, 2},
            {(DoubleUnaryOperator) x -> x, -2, -2},
            {(DoubleUnaryOperator) x -> x, 3, -3},
            {(DoubleUnaryOperator) x -> x, 3.1, 3},
            {(DoubleUnaryOperator) x -> x, -1.9, -2.1},
            // values at endpoints don't have opposite signs
            {(DoubleUnaryOperator) x -> x, 0, 4},
            {(DoubleUnaryOperator) x -> x, 3, 4},
            {(DoubleUnaryOperator) x -> x, -4, -3},
            {(DoubleUnaryOperator) x -> x*x, -4, 4},
            {(DoubleUnaryOperator) Math::cos, 1, PI/2},
            {(DoubleUnaryOperator) Math::cos, 1, 6},
            {(DoubleUnaryOperator) Math::cos, -10, -4},
            {(DoubleUnaryOperator) Math::cos, -PI, PI},
        };
    }

} // end class BisectionTest
