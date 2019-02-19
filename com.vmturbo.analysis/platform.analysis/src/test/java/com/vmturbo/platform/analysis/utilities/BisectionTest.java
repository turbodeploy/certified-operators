package com.vmturbo.platform.analysis.utilities;

import static java.lang.Math.E;
import static java.lang.Math.PI;
import static java.lang.Math.abs;
import static java.lang.Math.exp;
import static java.lang.Math.sin;
import static org.junit.Assert.assertEquals;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import org.junit.Test;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;

import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

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
    private static final int maxIterations = Integer.MAX_VALUE;
    private static final DoubleBinaryOperator errorFunction = (x, y) -> abs(x-y);

    // Methods

    /**
     * Feeds {@link Bisection#solve(double, DoubleBinaryOperator, int, DoubleUnaryOperator, double, double)}
     * valid input and tests that it returns the unique root present in the interval.
     */
    @Test
    @Parameters
    @TestCaseName("Test #{index}: Bisection.solve({0},{1},{2}) == {3}")
    public final void testSolve_Positive_OneRoot(DoubleUnaryOperator function, double a, double b, double output) {
        assertEquals(output, Bisection.solve(maxError, errorFunction, maxIterations, function, a, b), maxError);
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
//            {(DoubleUnaryOperator)Math::sin,       -PI/2,   PI/2, 0.0},
            {(DoubleUnaryOperator)Math::sin,     -3*PI/2,  -PI/2, -PI},
            {(DoubleUnaryOperator)Math::sin,        PI/2, 3*PI/2,  PI},
            // -sin -- root == midpoint
//            {(DoubleUnaryOperator)x -> - sin(x),   -PI/2,   PI/2, 0.0},
            {(DoubleUnaryOperator)x -> - sin(x), -3*PI/2,  -PI/2, -PI},
            {(DoubleUnaryOperator)x -> - sin(x),    PI/2, 3*PI/2,  PI},
            // sin -- arbitrary root
            {(DoubleUnaryOperator)Math::sin, -1, 2, 0.0},
            {(DoubleUnaryOperator)Math::sin, -4, -2, -PI},
            {(DoubleUnaryOperator)Math::sin,  3, 4, PI},
            // -sin -- arbitrary root
//            {(DoubleUnaryOperator)x -> - sin(x), -2.5, 1.5, 0.0},
            {(DoubleUnaryOperator)x -> - sin(x), -5.7, -2.1, -PI},
            {(DoubleUnaryOperator)x -> - sin(x), 0.001, 3.2,  PI},
            // exp -- 0/+
//            {(DoubleUnaryOperator)x -> exp(x) - E, 0, 2, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, 0, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, 0, Double.POSITIVE_INFINITY, 1},
            // exp -- -/0
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -100, 0, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, 0, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, 0, -1},
            // TODO: more concisely express using loops.
            // exp -- -/+ -- root == 1
            {(DoubleUnaryOperator)x -> exp(x) - E, -5, 2, 1},
            {(DoubleUnaryOperator)x -> exp(x) - E, -5, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, -5, Double.POSITIVE_INFINITY, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, -Double.MAX_VALUE, 2, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, -Double.MAX_VALUE, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, Double.NEGATIVE_INFINITY, 2, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> exp(x) - E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1},
            // exp -- -/+ -- root == -1
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -5, 2, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -5, Double.MAX_VALUE, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -5, Double.POSITIVE_INFINITY, -1},
            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, 2, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, Double.MAX_VALUE, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, 2, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, -1},
//            {(DoubleUnaryOperator)x -> exp(x) - 1/E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1},
            // -exp -- -/+ -- root == 1
            {(DoubleUnaryOperator)x -> -exp(x) + E, -5, 2, 1},
            {(DoubleUnaryOperator)x -> -exp(x) + E, -5, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, -5, Double.POSITIVE_INFINITY, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, -Double.MAX_VALUE, 2, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, -Double.MAX_VALUE, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, Double.NEGATIVE_INFINITY, 2, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, 1},
//            {(DoubleUnaryOperator)x -> -exp(x) + E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 1},
            // -exp -- -/+ -- root == -1
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -5, 2, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -5, Double.MAX_VALUE, -1},
//            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -5, Double.POSITIVE_INFINITY, -1},
            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -Double.MAX_VALUE, 2, -1},
//            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -Double.MAX_VALUE, Double.MAX_VALUE, -1},
//            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, -Double.MAX_VALUE, Double.POSITIVE_INFINITY, -1},
//            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, Double.NEGATIVE_INFINITY, 2, -1},
//            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, Double.NEGATIVE_INFINITY, Double.MAX_VALUE, -1},
//            {(DoubleUnaryOperator)x -> -exp(x) + 1/E, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -1},
            // x - c -- +/+
            {(DoubleUnaryOperator)x -> x - 42, 1, 100, 42},
//            {(DoubleUnaryOperator)x -> x - 0.9*Double.MAX_VALUE, 1, Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> x - 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, 0.95*Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> x - 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, Double.POSITIVE_INFINITY, 0.9*Double.MAX_VALUE},
            // c - x -- +/+
            {(DoubleUnaryOperator)x -> -x + 42, 1, 100, 42},
//            {(DoubleUnaryOperator)x -> -x + 0.9*Double.MAX_VALUE, 1, Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> -x + 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, 0.95*Double.MAX_VALUE, 0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> -x + 0.9*Double.MAX_VALUE, 0.85*Double.MAX_VALUE, Double.POSITIVE_INFINITY, 0.9*Double.MAX_VALUE},
            // x - c -- -/-
            {(DoubleUnaryOperator)x -> x + 42, -100, -1, -42},
//            {(DoubleUnaryOperator)x -> x + 0.9*Double.MAX_VALUE, -Double.MAX_VALUE, -1, -0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> x + 0.9*Double.MAX_VALUE, -0.95*Double.MAX_VALUE, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> x + 0.9*Double.MAX_VALUE, Double.NEGATIVE_INFINITY, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},
            // c - x -- -/-
            {(DoubleUnaryOperator)x -> -x - 42, -100, -1, -42},
//            {(DoubleUnaryOperator)x -> -x - 0.9*Double.MAX_VALUE, -Double.MAX_VALUE, -1, -0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> -x - 0.9*Double.MAX_VALUE, -0.95*Double.MAX_VALUE, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},
//            {(DoubleUnaryOperator)x -> -x - 0.9*Double.MAX_VALUE, Double.NEGATIVE_INFINITY, -0.85*Double.MAX_VALUE, -0.9*Double.MAX_VALUE},

            // miscellaneous -/+
//            {(DoubleUnaryOperator)x -> x, -PI/2, PI/2, 0.0},
            {(DoubleUnaryOperator)x -> x-1, -1e300, 1e300, 1.0},
//            {(DoubleUnaryOperator)x -> x+1, -Double.MAX_VALUE, Double.MAX_VALUE, -1.0},
//            {(DoubleUnaryOperator)x -> x, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0},
//            {(DoubleUnaryOperator)x -> x*x*x, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0},
//            {(DoubleUnaryOperator)Math::log, Double.MIN_VALUE, Double.POSITIVE_INFINITY, 1},
            // price function
            {(DoubleUnaryOperator)u -> u * PriceFunction.Cache.createStandardWeightedPriceFunction(1.0)
                .unitPrice(u, null, null, null, null) - 7.777777777, 0.01, 0.99, 0.7},
        };
    }

    /**
     * Feeds {@link Bisection#solve(double, DoubleBinaryOperator, int, DoubleUnaryOperator, double, double)}
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
        assertEquals(0.0, function.applyAsDouble(Bisection.solve(
            maxError, errorFunction, maxIterations, function, a, b)), maxError);
    }

    @SuppressWarnings("unused") // it is used reflectively
    private static Object[] parametersForTestSolve_Positive_ManyRoots() {
        return new Object[][]{
//            {(DoubleUnaryOperator)x ->  (x-2)*x*(x+2),  -3,  3},
//            {(DoubleUnaryOperator)x -> -(x-2)*x*(x+2), -30, 30},
            {(DoubleUnaryOperator)Math::sin, -33, 33},
            {(DoubleUnaryOperator)Math::cos, -12, 42},
        };
    }

    /**
     * Feeds {@link Bisection#solve(double, DoubleBinaryOperator, int, DoubleUnaryOperator, double, double)}
     * invalid input and tests that the proper exception is thrown.
     */
    @Test(expected = IllegalArgumentException.class)
    @Parameters
    @TestCaseName("Test #{index}: Bisection.solve({0},{1},{2})")
    public final void testSolve_Negative(DoubleUnaryOperator function, double a, double b) {
        Bisection.solve(maxError, errorFunction, maxIterations, function, a, b);
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
