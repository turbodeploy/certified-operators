package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.function.DoubleUnaryOperator;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

/**
 * An implementation of the 'bisection' numerical root-finding method.
 */
public class Bisection {
    // Methods

    /**
     * Reinterprets a {@code double} as a {@code long} and converts the latter from signed magnitude
     * to 2's complement representation.
     *
     * <p>
     *  This is an almost invertible function (see {@link #asDouble(long)} for the inverse) that
     *  preserves order. i.e. x <ᵈ y ⇒ asLong(x) <ˡ asLong(y), where <ᵈ and <ˡ represent double and
     *  long comparison operators respectively.
     * </p>
     *
     * <p>
     *  If the argument is a NaN, the inverse can still map it back to itself, but there is no order
     *  to preserve.
     * </p>
     *
     * <p>
     *  Both positive and negative 0.0 are mapped to 0L so negative 0.0 isn't mapped back to itself.
     * </p>
     *
     * @param x The {@code double} value to reinterpret/convert to {@code long}.
     * @return {@link Double#doubleToRawLongBits(double)}(x) converted to 2's complement.
     *
     * @see #asDouble(long)
     */
    @Pure
    static long asLong(double x) {
        long l = Double.doubleToRawLongBits(x);
        return l < 0 ? -(l & 0x7fff_ffff_ffff_ffffL) : l;
    }

    /**
     * Converts a {@code long} from 2's complement to singed magnitude representation and
     * reinterprets it as a {@code double}.
     *
     * <p>
     *  This is an almost invertible function (see {@link #asLong(double)} for the inverse) that
     *  preserves order. i.e. x <ˡ y ⇒ asDouble(x) <ᵈ asDouble(y), where <ˡ and <ᵈ represent long
     *  and double comparison operators respectively.
     * </p>
     *
     * <p>
     *  If the result is a NaN, the inverse can still map it back to itself, but there is no order
     *  to preserve.
     * </p>
     *
     * <p>
     *  The one exception to the invertibility of this function is Long.MIN_VALUE which is mapped to
     *  -0.0 that {@code asLong} maps to 0.
     * </p>
     *
     * @param l The {@code long} value to reinterpret/convert to {@code double}.
     * @return {@code l} converted to signed magnitude and fed to {@link Double#longBitsToDouble(long)}.
     *
     * @see #asLong(double)
     */
    @Pure
    static double asDouble(long l) {
        return Double.longBitsToDouble(l < 0 ? -l | 0x8000_0000_0000_0000L : l);
    }

    /**
     * Finds a root of a continuous function in an interval using the Bisection method.
     *
     * Provided the images of the interval's endpoints through the function have different signs
     * (i.e. f(a)•f(b)<0), it is guaranteed to return a root of the function (i.e. a value x such
     * that f(x)=0). If there is only one root in the interval, it will return this root. If there
     * are more, it may return any of them, but the same each time it's called with the same
     * arguments.
     *
     * The current implementation is loosely based on <a href="https://www.shapeoperator.com/2014/02/22/bisecting-floats/">
     * this Julia post</a> and is guaranteed to terminate after 64 iterations or less, although
     * other implementations are also possible.
     *
     * Since floating-point numbers have finite precision, this function returns the closest
     * representable value to the actual root.
     *
     * @param continuousFunction A continuous real-valued function of a real variable. It must be pure.
     * @param leftEndpoint The left endpoint of the interval. Must be < than {@code rightEndpoint}.
     * @param rightEndpoint The right endpoint of the interval. Must be > than {@code leftEndpoint}.
     * @return An x ∈ ({@code leftEndpoint}, {@code rightEndpoint}) such that
     *         {@code continuousFunction}(x)=0.0}
     */
    @Pure
    public static double solve(@NonNull DoubleUnaryOperator continuousFunction,
                               double leftEndpoint, double rightEndpoint) {
        checkArgument(leftEndpoint < rightEndpoint, "Requires %s < %s",
                        leftEndpoint, rightEndpoint); // also catches NaNs
        checkArgument(continuousFunction.applyAsDouble(leftEndpoint)
                    * continuousFunction.applyAsDouble(rightEndpoint) < 0,
                    "Requires f(%s)*f(%s) < 0", leftEndpoint, rightEndpoint);
        /*
         * The central idea behind this implementation is that instead of reducing the distance
         * between the interval's endpoints to half in each iteration, we halve the number of
         * representable floating-point numbers in the interval. We can have up to 2⁶⁴ of those, so
         * up to 64 iterations.
         *
         * In order to do this, we need to have a function f that maps the interval's endpoints a
         * and b to integers f(a) and f(b) in such a way that ∀x∈(a,b), f(x)∈(f(a),f(b)) and vice
         * versa. The `asLong` above is such a function.
         *
         * Note that the `asLong` function above maps both positive and negative 0.0 to 0L. If the
         * `continuousFunction` mapped positive and negative 0.0 to different values (e.g. 1/x
         * does), that could lead to problems, but this can't happen in the interval of interest
         * because bisection in general requires that the function be continuous in the interval.
         */
        final boolean swapClauses = continuousFunction.applyAsDouble(leftEndpoint) > 0;
        long left = asLong(leftEndpoint);
        long right = asLong(rightEndpoint);
        long mid = left < 0 && right > 0 ? 0 : left + (right - left)/2;
            // avoid overflow in case of huge endpoints with different signs

        /* Loop invariants:
         * - f(left₀) < 0 ⇒ ∀i, f(leftᵢ) < 0
         * - f(left₀) > 0 ⇒ ∀i, f(leftᵢ) > 0
         * - f(right₀) < 0 ⇒ ∀i, f(rightᵢ) < 0
         * - f(right₀) > 0 ⇒ ∀i, f(rightᵢ) > 0
         */
        while (left < mid) { // if the way mid is calculated is changed, the condition should change to match.
            final double midImage = continuousFunction.applyAsDouble(asDouble(mid));

            if (midImage == 0)
                break;
            else if (midImage < 0 ^ swapClauses)
                left = mid;
            else
                right = mid;

            mid = left + (right - left)/2; // this way to calculate mid avoids overflow when right
                                          // and left have huge magnitude and same signs.
        } // end while

        return asDouble(mid);
    } // end method solve

} //end class Bisection
