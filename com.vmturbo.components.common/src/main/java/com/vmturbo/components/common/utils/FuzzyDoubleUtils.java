package com.vmturbo.components.common.utils;

import com.google.common.math.DoubleMath;

/**
 * Utility methods around fuzzy operations on double values.
 */
public class FuzzyDoubleUtils {

    private FuzzyDoubleUtils() {}

    /**
     * Checks whether {@code value} is a positive value. Uses a default tolerance of
     * {@link FuzzyDouble#DEFAULT_TOLERANCE}.
     * @param value The value to check.
     * @return True, if {@code value} is positive. False otherwise.
     */
    public static boolean isPositive(double value) {
        return !Double.isNaN(value)
                && DoubleMath.fuzzyCompare(value, 0.0, FuzzyDouble.DEFAULT_TOLERANCE) > 0;
    }

    /**
     * Checks whether {@code value} is a negative value. Uses a default tolerance of
     * {@link FuzzyDouble#DEFAULT_TOLERANCE}.
     * @param value The value to check.
     * @return True, if {@code value} is negative. False otherwise.
     */
    public static boolean isNegative(double value) {
        return !Double.isNaN(value)
                && DoubleMath.fuzzyCompare(value, 0.0, FuzzyDouble.DEFAULT_TOLERANCE) < 0;
    }
}
