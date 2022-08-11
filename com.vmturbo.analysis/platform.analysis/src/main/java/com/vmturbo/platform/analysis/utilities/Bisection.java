package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

public class Bisection {

    /**
     * Check if the two arguments have different signs.
     *
     * @param intervalMinValue Value at the beginning of the interval.
     * @param intervalMaxValue Value at the end of the interval.
     * @return True if the two arguments have different signs.
     */
    private static boolean haveDifferentSign(double intervalMinValue, double intervalMaxValue) {
        double minSign = Math.signum(intervalMinValue);
        double maxSign = Math.signum(intervalMaxValue);
        double productOfOnes = minSign * maxSign;
        return productOfOnes < 0;
    }

    /**
     * Bisection method to find the root for the function in the given interval.
     *
     * @param epsilon The desired root accuracy.
     * @param errorFunction The binary error function to be used to calculate error.
     *                      It takes the bisection interval as argument.
     * @param maxIterations The maximum number of iterations of the algorithm.
     * @param function The unary function for which we are finding the root.
     * @param intervalMin The beginning of the interval.
     * @param intervalMax The end of the interval.
     * @return The root.
     */
    public static double solve(double epsilon, DoubleBinaryOperator errorFunction, int maxIterations,
                  DoubleUnaryOperator function, double intervalMin, double intervalMax) {
        checkArgument(intervalMin < intervalMax,
                      "Expected intervalMin %s < intervalMax %s", intervalMin, intervalMax);
        checkArgument(haveDifferentSign(function.applyAsDouble(intervalMin),
                                        function.applyAsDouble(intervalMax)),
                      "Interval (%s, %s)", intervalMin, intervalMax);
        double error;
        double root;
        double begin = intervalMin;
        double end = intervalMax;
        double iterations = 0;
        do {
            double mid = begin + (end - begin) / 2;
            double beginValue = function.applyAsDouble(begin);
            double midValue = function.applyAsDouble(mid);
            if (haveDifferentSign(beginValue, midValue)) {
                end = mid;
            } else {
                begin = mid;
            }
            error = errorFunction.applyAsDouble(begin, end);
            root = mid;
            iterations++;
        } while (iterations < maxIterations && error > epsilon);

        if (iterations == maxIterations) {
            throw new IllegalStateException("Exceeded maximum Iterations " +
                           "Interval (" + intervalMin + ", " + intervalMax + ")");
        }
        return root;
    }
}
