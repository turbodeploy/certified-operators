package com.vmturbo.platform.analysis.ede;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

/**
 * Certain numbers (quantities, capacities, thresholds, etc.) in the market must be converted
 * from one unit to another to properly support consistent scaling. An example of this are
 * CPU numbers on containers which are analyzed in market analysis in (normalized) MHz units
 * but must be made consistent across a group of resizes in a different unit (millicores).
 * The {@link ConsistentScalingNumber} ensures that arithmetic and comparisons done on
 * numbers are done on the same unit by forcing conversion from the normalized unit
 * into the consistent unit before any arithmetic or comparisons can be done. Also provides
 * facilities to convert from consistent units back into normalized units.
 * <p/>
 * Note that most comparison operations are done using approximate equality, however the
 * {@link #equals(Object)} method requires strict equality on the contained number. Use
 * the separate {@link #approxEquals(ConsistentScalingNumber)} method for
 * approximate equality. Note that this breaks transitivity over the supported approximate
 * comparison operations. ie if a.approxEquals(b) and b.approxEquals(c), it does not
 * imply that a.approxEquals(c). This is usually ok for the arithmetic and comparisons
 * we have to do in practice with these numbers. We do arithmetic and comparisons this way
 * because tiny arithmetic differences are introduced in the initial conversion
 * from consistentUnits to normaizedUnits and the conversion back result in them being very
 * slightly different even though they were originally the same value. Actual changes
 * in the real environment performed on these numbers tend to be many orders of magnitude
 * greater than the epsilon used for approximation.
 * <p/>
 * Consistent scaling numbers are immutable. Arithmetic on {@link ConsistentScalingNumber}
 * values results in the creation of a new {@link ConsistentScalingNumber} rather
 * than changing the value stored in an existing {@link ConsistentScalingNumber}.
 */
@Immutable
public class ConsistentScalingNumber {
    private final double consistentScalingNumber;

    /**
     * The maximum value for a {@link ConsistentScalingNumber}. Number is equal to
     * {@link Double#MAX_VALUE}.
     */
    public static final ConsistentScalingNumber MAX_VALUE = new ConsistentScalingNumber(Double.MAX_VALUE);

    /**
     * The zero value for a {@link ConsistentScalingNumber}. Number is equal to 0.
     */
    public static final ConsistentScalingNumber ZERO = new ConsistentScalingNumber(0);

    /**
     * When comparing two {@link ConsistentScalingNumber}, they are considered approximately equal
     * if the absolute value of their difference is less than this epsilon.
     */
    public static final double COMPARISON_EPSILON = 1e-4;

    /**
     * Create a new {@link ConsistentScalingNumber}.
     *
     * @param consistentScalingNumber The number in consistent scaling units.
     */
    private ConsistentScalingNumber(final double consistentScalingNumber) {
        this.consistentScalingNumber = consistentScalingNumber;
    }

    /**
     * Get the number in consistent scaling units.
     *
     * @return the number in consistent scaling units.
     */
    public double inConsistentUnits() {
        return consistentScalingNumber;
    }

    /**
     * Get the number in normalized units. Note that:
     * normalizedUnits = consistentUnits / consistentScalingFactor.
     *
     * @param consistentScalingFactor The consistent scaling factor for conversion back from consistent
     *                                units into normalized units.
     * @return The number in normalized units.
     */
    public double inNormalizedUnits(final double consistentScalingFactor) {
        return consistentScalingNumber / consistentScalingFactor;
    }

    @Override
    public String toString() {
        return "CSQ=" + Double.toString(consistentScalingNumber);
    }

    // ------------------------------------------------------------
    // Arithmetic
    // ------------------------------------------------------------

    /**
     * Compute a new number equal to this.number + addend.number.
     *
     * @param addend The number to add to this one.
     * @return A new number equal to this.number + addend.number
     */
    public ConsistentScalingNumber plus(@Nonnull final ConsistentScalingNumber addend) {
        return new ConsistentScalingNumber(consistentScalingNumber + addend.consistentScalingNumber);
    }

    /**
     * Compute a new number equal to this.number - subtrahend.number.
     *
     * @param subtrahend The number to subtract from this one.
     * @return A new number equal to this.number - subtrahend.number.
     */
    public ConsistentScalingNumber minus(@Nonnull final ConsistentScalingNumber subtrahend) {
        return new ConsistentScalingNumber(consistentScalingNumber - subtrahend.consistentScalingNumber);
    }

    /**
     * Compute a new number equal to minuend.number - this.number.
     *
     * @param minuend The number from which to subtract this number.
     * @return A new number equal to minuend.number - this.number.
     */
    public ConsistentScalingNumber subtractFrom(@Nonnull final ConsistentScalingNumber minuend) {
        return new ConsistentScalingNumber(minuend.consistentScalingNumber - consistentScalingNumber);
    }

    /**
     * Compute a new number equal to this.number * multiplier.number.
     *
     * @param multiplier The number by which to multiply this number.
     * @return A new number equal to this.number * multiplier.number.
     */
    public ConsistentScalingNumber times(@Nonnull final ConsistentScalingNumber multiplier) {
        return new ConsistentScalingNumber(consistentScalingNumber * multiplier.consistentScalingNumber);
    }

    /**
     * Compute a new number equal to this.number * factor.
     *
     * @param factor The factor by which to multiply this number.
     * @return A new number equal to this.number * factor.
     */
    public ConsistentScalingNumber timesFactor(final double factor) {
        return new ConsistentScalingNumber(consistentScalingNumber * factor);
    }

    /**
     * Compute a new number equal to this.number * factor.
     *
     * @param factor The factor by which to multiply this number.
     * @return A new number equal to this.number * factor.
     */
    public ConsistentScalingNumber timesFactor(final int factor) {
        return new ConsistentScalingNumber(consistentScalingNumber * factor);
    }

    /**
     * Compute a new number equal to this.number / divisor.number.
     *
     * @param divisor The number by which to divide this number.
     * @return A new number equal to this.number / divisor.number.
     */
    public ConsistentScalingNumber dividedBy(@Nonnull final ConsistentScalingNumber divisor) {
        return new ConsistentScalingNumber(consistentScalingNumber / divisor.consistentScalingNumber);
    }

    /**
     * Compute a new number equal to this.number / factor.
     *
     * @param factor The factor by which to divide this number.
     * @return A new number equal to this.number / factor.
     */
    public ConsistentScalingNumber dividedByFactor(final double factor) {
        return new ConsistentScalingNumber(consistentScalingNumber / factor);
    }

    /**
     * Compute a new number equal to this.number / factor.
     *
     * @param factor The factor by which to divide this number.
     * @return A new number equal to this.number / factor.
     */
    public ConsistentScalingNumber dividedByFactor(final int factor) {
        return new ConsistentScalingNumber(consistentScalingNumber / factor);
    }

    /**
     * Compute a new number equal to Math.ceiling(this.number). If given number is rounded to
     * Math.floor(this.number) due to floating point, return Math.floor(this.number).
     * For example, 1.00001 should be treated as 1.
     *
     * @return A new number equal to Math.ceiling(this.number).
     */
    public ConsistentScalingNumber approxCeiling() {
        double consistentScalingNumberCeiling =
            consistentScalingNumber - Math.floor(consistentScalingNumber) < COMPARISON_EPSILON
                ? Math.floor(consistentScalingNumber)
                : Math.ceil(consistentScalingNumber);
        return new ConsistentScalingNumber(consistentScalingNumberCeiling);
    }

    /**
     * Compute a new number equal to Math.floor(this.number). If given number is rounded to
     * Math.ceiling(this.number) due to floating point, return Math.ceiling(this.number).
     * For example, 1.99999 should be treated as 2.
     *
     * @return A new number equal to Math.floor(this.number).
     */
    public ConsistentScalingNumber approxFloor() {
        double consistentScalingNumberFloor =
            Math.ceil(consistentScalingNumber) - consistentScalingNumber < COMPARISON_EPSILON
                ? Math.ceil(consistentScalingNumber)
                : Math.floor(consistentScalingNumber);
        return new ConsistentScalingNumber(consistentScalingNumberFloor);
    }

    /**
     * Compute a new number equal to Math.abs(this.number).
     *
     * @return A new number equal to Math.abs(this.number).
     */
    public ConsistentScalingNumber abs() {
        return new ConsistentScalingNumber(Math.abs(consistentScalingNumber));
    }

    // ------------------------------------------------------------
    // Factory methods
    // ------------------------------------------------------------
    /**
     * Create a new {@link ConsistentScalingNumber} from a number in normalized units.
     * The conversion from normalized units to consistent units is performed by:
     * consistentNumber = normalizedNumber * consistentScalingFactor
     *
     * @param normalizedNumber The number in normalized units.
     * @param consistentScalingFactor The consistent scaling factor to be used to convert normalized units
     *                                to consistent units.
     * @return a new {@link ConsistentScalingNumber} equivalent to the normalized number after
     *         application of the consistentScalingFactor.
     */
    public static ConsistentScalingNumber fromNormalizedNumber(final double normalizedNumber,
                                                               final double consistentScalingFactor) {
        return new ConsistentScalingNumber(normalizedNumber * consistentScalingFactor);
    }

    /**
     * Create a new {@link ConsistentScalingNumber} from a number already in consistent units.
     *
     * @param consistentScalingNumber The number in consistent units.
     * @return a new {@link ConsistentScalingNumber} from a number already in consistent units.
     */
    public static ConsistentScalingNumber fromConsistentNumber(final double consistentScalingNumber) {
        return new ConsistentScalingNumber(consistentScalingNumber);
    }

    // ------------------------------------------------------------
    // Comparison
    // ------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ConsistentScalingNumber)) {
            return false;
        }

        final ConsistentScalingNumber other = (ConsistentScalingNumber)o;
        // Note that we use strict equality here rather than approximate equality.
        return this.consistentScalingNumber == other.consistentScalingNumber;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(consistentScalingNumber);
    }

    /**
     * Return the larger {@link ConsistentScalingNumber} between a and b. Note that
     * {@link #max(ConsistentScalingNumber, ConsistentScalingNumber)} does NOT use
     * an approximation epsilon for comparison. If the numbers are exactly equal, returns a.
     *
     * @param a One number.
     * @param b Another number.
     * @return The larger number between a and b. If the numbers are exactly equal, returns a.
     */
    public static ConsistentScalingNumber max(@Nonnull final ConsistentScalingNumber a,
                                                @Nonnull final ConsistentScalingNumber b) {
        return a.consistentScalingNumber > b.consistentScalingNumber ? a : b;
    }

    /**
     * Return the smaller {@link ConsistentScalingNumber} between a and b. Note that
     * {@link #min(ConsistentScalingNumber, ConsistentScalingNumber)} does NOT use
     * an approximation epsilon for comparison. If the numbers are exactly equal, returns a.
     *
     * @param a One number.
     * @param b Another number.
     * @return The smaller number between a and b. If the numbers are exactly equal, returns a.
     */
    public static ConsistentScalingNumber min(@Nonnull final ConsistentScalingNumber a,
                                                @Nonnull final ConsistentScalingNumber b) {
        return a.consistentScalingNumber <= b.consistentScalingNumber ? a : b;
    }

    /**
     * Check if this number is approximately equal to another. Two numbers are considered
     * approximately equal if the absolute value of their difference is less than
     * {@link #COMPARISON_EPSILON}.
     *
     * @param other The other number to compare for approximate equality.
     * @return Whether this number is approximately equal to the other number.
     */
    public boolean approxEquals(@Nonnull final ConsistentScalingNumber other) {
        return Math.abs(this.consistentScalingNumber - other.consistentScalingNumber) < COMPARISON_EPSILON;
    }

    /**
     * Check if this number is greater than AND NOT approximately equal to the other.
     * Returns false if this.approxEquals(other) even if this.number > other.number.
     * This may happen if this number is a tiny bit greater than the other.
     *
     * @param other The other number to compare against.
     * @return whether this number is greater than another AND NOT approximately equal to the other.
     */
    public boolean isGreaterThan(@Nonnull final ConsistentScalingNumber other) {
        return this.consistentScalingNumber > other.consistentScalingNumber
            && !approxEquals(other);
    }

    /**
     * Check if this number is less than AND NOT approximately equal to the other.
     * Returns false if this.approxEquals(other) even if this.number < other.number.
     * This may happen if this number is a tiny bit less than the other.
     *
     * @param other The other number to compare against.
     * @return whether this number is less than another AND NOT approximately equal to the other.
     */
    public boolean isLessThan(@Nonnull final ConsistentScalingNumber other) {
        return this.consistentScalingNumber < other.consistentScalingNumber
            && !approxEquals(other);
    }

    /**
     * Check if this number is greater than OR approximately equal to the other.
     *
     * @param other The other number to compare against.
     * @return whether this number is greater than OR approximately equal to the other.
     */
    public boolean isGreaterThanOrApproxEqual(@Nonnull final ConsistentScalingNumber other) {
        return this.consistentScalingNumber > other.consistentScalingNumber
            || approxEquals(other);
    }

    /**
     * Check if this number is less than OR approximately equal to the other.
     *
     * @param other The other number to compare against.
     * @return whether this number is less than OR approximately equal to the other.
     */
    public boolean isLessThanOrApproxEqual(@Nonnull final ConsistentScalingNumber other) {
        return this.consistentScalingNumber < other.consistentScalingNumber
            || approxEquals(other);
    }
}
