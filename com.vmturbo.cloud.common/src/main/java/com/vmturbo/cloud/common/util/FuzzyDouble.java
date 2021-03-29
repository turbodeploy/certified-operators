package com.vmturbo.cloud.common.util;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.math.DoubleMath;

/**
 * An immutable double value, wrapping some configurable tolerance for comparison to other values.
 *
 * <p>NOTE: This class does not respect order of operations.
 */
public class FuzzyDouble {

    /**
     * The default tolerance used in creating {@link FuzzyDouble} instances.
     */
    public static final double DEFAULT_TOLERANCE = .001;

    private final double value;

    private final double tolerance;

    private FuzzyDouble(double value, double tolerance) {
        this.value = value;
        this.tolerance = tolerance;
    }

    /**
     * Checks whether the wrapped value is positive, using the configured tolerance.
     * @return Whether the wrapped value is positive.
     */
    public boolean isPositive() {
        return DoubleMath.fuzzyCompare(value, 0.0, tolerance) > 0;
    }

    /**
     * Checks whether the wrapped value is less than {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is less than {@code targetValue}.
     */
    public boolean isLessThan(double targetValue) {
        return DoubleMath.fuzzyCompare(value, targetValue, tolerance) < 0;
    }

    /**
     * Checks whether the wrapped value is less than or equal to {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is less than or equal to {@code targetValue}.
     */
    public boolean isLessThanEq(double targetValue) {
        return DoubleMath.fuzzyCompare(value, targetValue, tolerance) <= 0;
    }

    /**
     * Checks whether the wrapped value is greater than {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is greater than {@code targetValue}.
     */
    public boolean isGreaterThan(double targetValue) {
        return DoubleMath.fuzzyCompare(value, targetValue, tolerance) > 0;
    }

    /**
     * Checks whether the wrapped value is greater than or equal to {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is greater than or equal to {@code targetValue}.
     */
    public boolean isGreaterThanEq(double targetValue) {
        return DoubleMath.fuzzyCompare(value, targetValue, tolerance) >= 0;
    }

    /**
     * Subtracts {@code operand} from the wrapped value, returning a new {@link FuzzyDouble} instance
     * representing the result.
     * @param operand The operand value.
     * @return A new {@link FuzzyDouble}, representing the subtraction result.
     */
    public FuzzyDouble subtract(double operand) {
        return FuzzyDouble.newFuzzy(value - operand, tolerance);
    }

    /**
     * Subtracts the wrapped value from {@code operand}, returning a new {@link FuzzyDouble} instance
     * representing the result.
     * @param operand The operand value.
     * @return A new {@link FuzzyDouble}, representing the subtraction result.
     */
    public FuzzyDouble subtractFrom(double operand) {
        return FuzzyDouble.newFuzzy(operand - value, tolerance);
    }

    /**
     * Adds {@code operand} to the wrapped value, returning a new {@link FuzzyDouble} instance
     * representing the result.
     * @param operand The operand value.
     * @return A new {@link FuzzyDouble}, representing the addition result.
     */
    public FuzzyDouble add(double operand) {
        return FuzzyDouble.newFuzzy(value + operand, tolerance);
    }

    /**
     * Divides the wrapped value by {@code divisor}, returning a new {@link FuzzyDouble} instance
     * representing the result. This method will first check that {@code divisor} is not zero.
     * @param divisor The divisor.
     * @return A new {@link FuzzyDouble}, representing the division result.
     */
    public FuzzyDouble dividedBy(double divisor) {
        Preconditions.checkArgument(divisor != 0.0);

        return FuzzyDouble.newFuzzy(value / divisor, tolerance);
    }

    /**
     * Multiplies the wrapped value by {@code multiplier}, returning a new {@link FuzzyDouble} instance
     * representing the result.
     * @param multiplier The multiplier.
     * @return A new {@link FuzzyDouble}, representing the multiplication result.
     */
    public FuzzyDouble times(double multiplier) {
        return FuzzyDouble.newFuzzy(value * multiplier, tolerance);
    }

    /**
     * Returns the wrapped double value.
     * @return The wrapped double value.
     */
    public double value() {
        return value;
    }

    /**
     * Converts this immutable instance to a {@link MutableFuzzyDouble}.
     * @return A new {@link MutableFuzzyDouble} instance mirroring this instance.
     */
    public MutableFuzzyDouble asMutableFuzzy() {
        return MutableFuzzyDouble.newFuzzy(value, tolerance);
    }

    /**
     * Creates a new {@link FuzzyDouble} instance.
     * @param value The double value to wrap.
     * @param tolerance The tolerance to use in comparing to other values.
     * @return The newly created {@link FuzzyDouble}.
     */
    @Nonnull
    public static FuzzyDouble newFuzzy(double value, double tolerance) {
        return new FuzzyDouble(value, tolerance);
    }

    /**
     * Creates a new {@link FuzzyDouble} instance with {@link #DEFAULT_TOLERANCE}.
     * @param value The double value to wrap.
     * @return The newly created {@link FuzzyDouble}.
     */
    @Nonnull
    public static FuzzyDouble newFuzzy(double value) {
        return new FuzzyDouble(value, DEFAULT_TOLERANCE);
    }
}
