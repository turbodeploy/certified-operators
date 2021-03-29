package com.vmturbo.cloud.common.util;

import javax.annotation.Nonnull;

import com.google.common.math.DoubleMath;
import com.google.common.util.concurrent.AtomicDouble;

/**
 * A mutable version of {@link FuzzyDouble}, in which any operations modify the wrapped value.
 */
public class MutableFuzzyDouble {

    /**
     * The default tolerance used in creating {@link MutableFuzzyDouble} instances.
     */
    public static final double DEFAULT_TOLERANCE = .001;

    private final AtomicDouble value;

    private final double tolerance;

    private MutableFuzzyDouble(double value, double tolerance) {
        this.value = new AtomicDouble(value);
        this.tolerance = tolerance;
    }

    /**
     * Checks whether the wrapped value is positive, using the configured tolerance.
     * @return Whether the wrapped value is positive.
     */
    public boolean isPositive() {
        return DoubleMath.fuzzyCompare(value.get(), 0.0, tolerance) > 0;
    }

    /**
     * Checks whether the wrapped value is less than {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is less than {@code targetValue}.
     */
    public boolean isLessThan(double targetValue) {
        return DoubleMath.fuzzyCompare(value.get(), targetValue, tolerance) < 0;
    }

    /**
     * Checks whether the wrapped value is less than or equal to {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is less than or equal to {@code targetValue}.
     */
    public boolean isLessThanEq(double targetValue) {
        return DoubleMath.fuzzyCompare(value.get(), targetValue, tolerance) <= 0;
    }

    /**
     * Checks whether the wrapped value is greater than {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is greater than {@code targetValue}.
     */
    public boolean isGreaterThan(double targetValue) {
        return DoubleMath.fuzzyCompare(value.get(), targetValue, tolerance) > 0;
    }

    /**
     * Checks whether the wrapped value is greater than or equal to {@code targetValue}, using the configured
     * tolerance.
     * @param targetValue The target double value.
     * @return Whether the wrapped value is greater than or equal to {@code targetValue}.
     */
    public boolean isGreaterThanEq(double targetValue) {
        return DoubleMath.fuzzyCompare(value.get(), targetValue, tolerance) >= 0;
    }

    /**
     * Subtracts {@code operand} from the wrapped value, storing the result as the wrapped value.
     * @param operand The operand.
     */
    public void subtract(double operand) {
        value.addAndGet(operand * -1.0);
    }

    /**
     * Adds {@code operand} to the wrapped value, storing the result as the wrapped value.
     * @param operand The operand.
     */
    public void add(double operand) {
        value.addAndGet(operand);
    }

    /**
     * Returns the wrapped value.
     * @return The wrapped value.
     */
    public double value() {
        return value.get();
    }

    /**
     * Creates a new {@link MutableFuzzyDouble} instance.
     * @param value The double value to wrap.
     * @param tolerance The tolerance to use in comparison operations.
     * @return The newly created {@link MutableFuzzyDouble} instance.
     */
    @Nonnull
    public static MutableFuzzyDouble newFuzzy(double value, double tolerance) {
        return new MutableFuzzyDouble(value, tolerance);
    }

    /**
     * Creates a new {@link MutableFuzzyDouble} instance using {@link #DEFAULT_TOLERANCE}.
     * @param value The double value to wrap.
     * @return The newly created {@link MutableFuzzyDouble} instance.
     */
    @Nonnull
    public static MutableFuzzyDouble newFuzzy(double value) {
        return new MutableFuzzyDouble(value, DEFAULT_TOLERANCE);
    }
}
