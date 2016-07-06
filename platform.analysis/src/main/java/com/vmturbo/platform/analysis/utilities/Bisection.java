package com.vmturbo.platform.analysis.utilities;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.function.DoubleUnaryOperator;

public class Bisection {

    private static boolean haveDifferentSign(double intervalMinValue, double intervalMaxValue) {
        double minSign = Math.signum(intervalMinValue);
        double maxSign = Math.signum(intervalMaxValue);
        double productOfOnes = minSign * maxSign;
        return productOfOnes < 0;
    }

    public static double solve(double epsilon, int maxIterations,
                  DoubleUnaryOperator function, double intervalMin, double intervalMax) {
        checkArgument(intervalMin < intervalMax, "Expected intervalMin %s < intervalMax %s", intervalMin, intervalMax);
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
            error = end - begin;
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
