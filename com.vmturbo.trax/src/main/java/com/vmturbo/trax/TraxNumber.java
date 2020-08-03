package com.vmturbo.trax;

import static com.vmturbo.trax.Trax.trax;

import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import com.google.common.base.MoreObjects;

import com.vmturbo.trax.Trax.TraxCalculation;
import com.vmturbo.trax.Trax.TraxInfixOperation;
import com.vmturbo.trax.Trax.TraxIntermediateCalculation;
import com.vmturbo.trax.Trax.TraxIntermediateTrackingCalculation;
import com.vmturbo.trax.Trax.TraxTrackable;
import com.vmturbo.trax.TraxConfiguration.TraxContext;

/**
 * TRAX: Calculation Tracking library.
 * Use a TraxNumber to trace the elements of a calculation as the calculation is composed.
 * <p/>
 * Consider the following calculation of a {@link TraxNumber}:
 * {@code TraxNumber addition = trax(5.0, "foo").plus(10.0, "bar").compute("addition")}
 * <p/>
 * then (when configured at DEBUG level verbosity) the result of {@code addition#calculationStack()}
 * will be:
 * <p/>
 * assertEquals(
 *     "15[addition] = 5[foo] + 10[bar]\n" +
 *     "├── 5[foo]\n" +
 *     "└── 10[bar]\n",
 *         addition.calculationStack());
 * <p/>
 * Usually, it's not necessary to do this sort of deep inspection of a particular calculation
 * result (although when it is necessary, it is usually incredibly valuable to be able to do so).
 * In cases where a calculation is not configured for debugging or tracing, we want
 * to introduce the minimum possible performance overhead in performing the actual calculation.
 * As such, a bare {@link TraxNumber} is a thin wrapper around a primitive double value. We only
 * retain additional information when {@link Trax} tracking has been configured to debug
 * a given calculation.
 * <p/>
 * Perform arithmetic on {@link TraxNumber}s with the arithmetic operators such as
 * {@link #plus(double)}, {@link #minus(double)}, {@link #subtractFrom(double)}, etc.
 * <p/>
 * Do not directly instantiate {@link TraxNumber}s through their constructor (which
 * is package-private). Instead, call {@link Trax#trax(double)} and its variants.
 * These {@link Trax} factory methods will look up the appropriate {@link TraxContext}
 * for the current thread and decide whether to instantiate a plain {@link TraxNumber}
 * (minimal performance impact, but does not trace the calculation hierarchy that
 * goes into composing itself), or whether to allocate a {@link TraxTrackingNumber}
 * which requires more memory than a simple wrapper around a double, but permits
 * deep introspection into every sub-calculation that goes into calculating that
 * number.
 * <p/>
 * Note that {@link TraxNumber} and {@link TraxTrackingNumber} are {@link Immutable}.
 * Calling an arithmetic operation on a {@link TraxNumber} creates a new
 * {@link TraxNumber} rather than modifying the existing one. Since they are
 * {@link Immutable}, {@link TraxNumber}s are also
 * {@link javax.annotation.concurrent.ThreadSafe}.
 */
@Immutable
public class TraxNumber implements TraxTrackable, Comparable<TraxNumber> {
    /**
     * The value of the number.
     * <p/>
     * Package-private so that it can be accessed by the {@link TraxTrackingNumber} subclass.
     */
    final double value;

    /**
     * Create a new {@link TraxNumber}. A {@link TraxNumber} is {@link Immutable}.
     *
     * @param value The value of the number.
     */
    TraxNumber(final double value) {
        this.value = value;
    }

    /**
     * Compare another {@link TraxNumber}'s value with {@link this} one's for equivalence.
     *
     * @param other Another {@link TraxNumber} to compare for value equality.
     * @return True if the other {@link TraxNumber}'s interior value is equal to this one's.
     * False otherwise, including if the other is null.
     */
    public boolean valueEquals(@Nullable final TraxNumber other) {
        if (other == null) {
            return false;
        }

        if (other == this) {
            return true;
        }

        return other.getValue() == value;
    }

    /**
     * Compare another double value with {@link this} object's value for equivalence.
     *
     * @param other Another double number to compare for value equality.
     * @return True if the other number is equal to this one's interior double value.
     */
    public boolean valueEquals(double other) {
        return other == value;
    }

    /**
     * Compare {@link this} number's interior value with that of another.
     *
     * @param o The other object to compare.
     * @return Returns a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(@Nonnull final TraxNumber o) {
        return Double.compare(value, o.getValue());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("value", value)
            .toString();
    }

    /**
     * Get the {@link TraxNumber}'s value.
     *
     * @return The value of the {@link TraxNumber}
     */
    public double getValue() {
        return value;
    }

    /**
     * Get the name assigned to this {@link TraxNumber}. The name has no impact on the behavior
     * of the number, it is used during display to produce a human-readable explanation
     * of what this number represents.
     *
     * @return The name of this {@link TraxNumber}.
     */
    @Nullable
    public String getName() {
        return null;
    }

    /**
     * Check whether this {@link TraxNumber} was given a name. If true, calls to
     * {@link #getName()} will return {@link Nonnull}. If false, calls to
     * {@link #getName()} will return null.
     *
     * @return whether this {@link TraxNumber} was given a name.
     */
    public boolean hasName() {
        return false;
    }

    /**
     * Get the details string assigned to this {@link TraxNumber}. This string has no impact
     * on the behavior of the number, it is used during display to produce a human-readable
     * description providing additional details about the number. By default, this description
     * will contain the file name and line number of the line of code that was responsible
     * for constructing the number.
     * <p/>
     * Details are only displayed in the calculation stack when {@link Trax} is configured
     * to {@code com.vmturbo.components.common.calculation.tracing.Trax.Verbosity.TRACE}.
     *
     * @return The details of this {@link TraxNumber}.
     */
    @Nullable
    public String getDetails() {
        return null;
    }

    /**
     * Check whether this {@link TraxNumber} was given any details. If true, calls to
     * {@link #getDetails()} will return {@link Nonnull}. If false, calls to
     * {@link #getDetails()} will return null.
     *
     * @return whether this {@link TraxNumber} was given a name.
     */
    public boolean hasDetails() {
        return false;
    }

    /**
     * An equivalent is another {@link Trax} element that is equal to this number.
     * Typically, the equivalent will be the {@link TraxCalculation} used to compose
     * the current {@link TraxNumber}.
     * <p/>
     * Note that the equivalent for a {@link TraxNumber} when tracking is not enabled
     * will always be {@link null}.
     * <p/>
     * In the following example:
     * {@code TraxNumber addition = trax(5.0, "foo").plus(10.0, "bar").compute("addition")}
     * <p/>
     * The "addition" {@link TraxNumber} will have an equivalent set to be the calculation
     * data for "foo" + "bar".
     *
     * @return The equivalent calculation used to compute this {@link TraxNumber}. Returns
     * null if the number is not performing tracking or was not generated as
     * the result of a calculation.
     */
    @Nullable
    public TraxTrackable getEquivalent() {
        return null;
    }

    /**
     * Whether this {@link TraxNumber} is retaining the details for its equivalent.
     * Usually the equivalent is a {@link TraxCalculation}.
     * Will always be false if tracking is not enabled.
     *
     * @return whether this {@link TraxNumber} is retaining the details for its equivalent.
     */
    public boolean hasEquivalent() {
        return false;
    }

    /**
     * Render a String containing the details of the {@link TraxNumber} as well as all the details
     * of the calculations performed to compute the {@link TraxNumber}.
     * <p/>
     * Consider the following calculation of a {@link TraxNumber}:
     * {@code TraxNumber addition = trax(5.0, "foo").plus(10.0, "bar").compute("addition")}
     * <p/>
     * then (when configured at DEBUG level verbosity) the result of {@code addition#calculationStack()}
     * will be:
     * <p/>
     * assertEquals(
     * "15[addition] = 5[foo] + 10[bar]\n" +
     * "├── 5[foo]\n" +
     * "└── 10[bar]\n",
     * addition.calculationStack());
     * <p/>
     * If the applicable {@link TraxContext} does not have calculation tracking enabled,
     * the rendered string contains only the base value.
     *
     * @return The calculation stack for the calculations used to compute this {@link TraxNumber}.
     */
    @Nonnull
    public String calculationStack() {
        final TraxContext context = TraxConfiguration.mostVerboseContextFor(Thread.currentThread());

        if (context.on()) {
            final StringBuilder builder = new StringBuilder(256);
            appendChildrenStrings(builder, "", "", context);
            return builder.toString();
        } else {
            return context.formatDecimal(value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendSelfString(@Nonnull final StringBuilder builder,
                                 @Nonnull final TraxContext context) {
        builder.append(context.formatDecimal(value));
        if (context.on() && hasName()) {
            builder.append("[").append((hasName() ? getName() : ""));
            builder.append("]");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void appendChildrenStrings(@Nonnull final StringBuilder builder,
                                      @Nonnull final String prefix,
                                      @Nonnull final String childrenPrefix,
                                      @Nonnull final TraxContext context) {
        // https://stackoverflow.com/a/8948691
        builder.append(prefix);
        appendSelfString(builder, context);

        if (hasEquivalent()) {
            // First append details of the current level, then append details of children
            builder.append(" = ");
            getEquivalent().appendSelfString(builder, context);
        }

        if (context.trace() && hasDetails()) {
            builder.append(" (").append(getDetails()).append(")");
        }

        builder.append("\n");
        if (hasEquivalent()) {
            getEquivalent().appendChildrenStrings(builder, childrenPrefix, childrenPrefix, context);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Create a new {@link TraxNumber} with the given name whose value will be
     * equivalent to {@link this} number's value.
     *
     * @param name The name of the new number.
     * @return a new {@link TraxNumber} with the given name whose value will be
     *         equivalent to {@link this} number's value.
     */
    public TraxNumber named(@Nonnull final String name) {
        return new TraxNumber(getValue());
    }

    // ------------------------------------------------------------------------

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value + addend.
     *
     * @param addend The value to add.
     * @return a new {@link TraxIntermediateCalculation} = this.value + addend.
     */
    public TraxIntermediateCalculation plus(final double addend) {
        return new TraxIntermediateCalculation(addend + value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value + addend.
     *
     * @param addend     The value to add.
     * @param addendName The name of the addend parameter.
     * @return a new {@link TraxIntermediateCalculation} = this.value + addend.
     */
    public TraxIntermediateCalculation plus(final double addend,
                                            @Nonnull final String addendName) {
        return new TraxIntermediateCalculation(addend + value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value + addend.
     *
     * @param addend        The value to add.
     * @param addendName    The name of the addend parameter.
     * @param addendDetails Detail descriptions of the addend.
     * @return a new {@link TraxIntermediateCalculation} = this.value + addend.
     */
    public TraxIntermediateCalculation plus(final double addend,
                                            @Nonnull final String addendName,
                                            @Nonnull final String addendDetails) {
        return new TraxIntermediateCalculation(addend + value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value + addend.value.
     *
     * @param addend The {@link TraxNumber} to add.
     * @return a new {@link TraxIntermediateCalculation} = this.value + addend.value.
     */
    public TraxIntermediateCalculation plus(@Nonnull final TraxNumber addend) {
        return new TraxIntermediateCalculation(addend.value + value);
    }

    // ------------------------------------------------------------------------

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.
     *
     * @param subtrahend The value to subtract off.
     * @return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.
     */
    public TraxIntermediateCalculation minus(final double subtrahend) {
        return new TraxIntermediateCalculation(value - subtrahend);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.
     *
     * @param subtrahend     The value to subtract off.
     * @param subtrahendName The name of the subtrahend parameter.
     * @return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.
     */
    public TraxIntermediateCalculation minus(final double subtrahend,
                                             @Nonnull final String subtrahendName) {
        return new TraxIntermediateCalculation(value - subtrahend);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.
     *
     * @param subtrahend        The value to subtract off.
     * @param subtrahendName    The name of the {@code subtrahend} argument.
     * @param subtrahendDetails The details of the {@code subtrahend} argument.
     * @return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.
     */
    public TraxIntermediateCalculation minus(final double subtrahend,
                                             @Nonnull final String subtrahendName,
                                             @Nonnull final String subtrahendDetails) {
        return new TraxIntermediateCalculation(value - subtrahend);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.value.
     *
     * @param subtrahend The value to subtract off.
     * @return a new {@link TraxIntermediateCalculation} = this.value - subtrahend.value.
     */
    public TraxIntermediateCalculation minus(@Nonnull final TraxNumber subtrahend) {
        return new TraxIntermediateCalculation(value - subtrahend.value);
    }

    // ------------------------------------------------------------------------

    /**
     * Return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     *
     * @param minuend The value to subtract this number from.
     * @return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     */
    public TraxIntermediateCalculation subtractFrom(final double minuend) {
        return new TraxIntermediateCalculation(minuend - value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     *
     * @param minuend     The value to subtract this number from.
     * @param minuendName The name of the {@code minuend} argument.
     * @return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     */
    public TraxIntermediateCalculation subtractFrom(final double minuend,
                                                    @Nonnull final String minuendName) {
        return new TraxIntermediateCalculation(minuend - value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     *
     * @param minuend        The value to subtract this number from.
     * @param minuendName    The name of the {@code minuend} argument.
     * @param minuendDetails The details of the {@code minuend} argument.
     * @return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     */
    public TraxIntermediateCalculation subtractFrom(final double minuend,
                                                    @Nonnull final String minuendName,
                                                    @Nonnull final String minuendDetails) {
        return new TraxIntermediateCalculation(minuend - value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = minuend.value - this.value.
     *
     * @param minuend The value to subtract this number from.
     * @return a new {@link TraxIntermediateCalculation} = minuend - this.value.
     */
    public TraxIntermediateCalculation subtractFrom(@Nonnull final TraxNumber minuend) {
        return new TraxIntermediateCalculation(minuend.value - value);
    }

    // ------------------------------------------------------------------------

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value * multiplier.
     *
     * @param multiplier The value to multiply.
     * @return a new {@link TraxIntermediateCalculation} = this.value * multiplier
     */
    public TraxIntermediateCalculation times(final double multiplier) {
        return new TraxIntermediateCalculation(multiplier * value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value * multiplier.
     *
     * @param multiplier     The value to multiply.
     * @param multiplierName The name of the multiplier parameter.
     * @return a new {@link TraxIntermediateCalculation} = this.value * multiplier.
     */
    public TraxIntermediateCalculation times(final double multiplier,
                                             @Nonnull final String multiplierName) {
        return new TraxIntermediateCalculation(multiplier * value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value * multiplier.
     *
     * @param multiplier        The value to multiply.
     * @param multiplierName    The name of the multiplier parameter.
     * @param multiplierDetails The details of the multiplier parameter.
     * @return a new {@link TraxIntermediateCalculation} = this.value * multiplier.
     */
    public TraxIntermediateCalculation times(final double multiplier,
                                             @Nonnull final String multiplierName,
                                             @Nonnull final String multiplierDetails) {
        return new TraxIntermediateCalculation(multiplier * value);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value * multiplier.value.
     *
     * @param multiplier The value to multiply.
     * @return a new {@link TraxIntermediateCalculation} = this.value * multiplier.value.
     */
    public TraxIntermediateCalculation times(@Nonnull final TraxNumber multiplier) {
        return new TraxIntermediateCalculation(multiplier.value * value);
    }

    // ------------------------------------------------------------------------

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value / divisor.
     *
     * @param divisor The value to divide.
     * @return a new {@link TraxIntermediateCalculation} = this.value / divisor.
     */
    public TraxIntermediateCalculation dividedBy(final double divisor) {
        return new TraxIntermediateCalculation(value / divisor);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value / divisor.
     *
     * @param divisor     The value to divide.
     * @param divisorName The name of the divisor parameter.
     * @return a new {@link TraxIntermediateCalculation} = this.value / divisor.
     */
    public TraxIntermediateCalculation dividedBy(final double divisor,
                                                 @Nonnull final String divisorName) {
        return new TraxIntermediateCalculation(value / divisor);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value / divisor.
     *
     * @param divisor        The value to divide.
     * @param divisorName    The name of the divisor parameter.
     * @param divisorDetails The details of the divisor parameter.
     * @return a new {@link TraxIntermediateCalculation} = this.value / divisor.
     */
    public TraxIntermediateCalculation dividedBy(final double divisor,
                                                 @Nonnull final String divisorName,
                                                 @Nonnull final String divisorDetails) {
        return new TraxIntermediateCalculation(value / divisor);
    }

    /**
     * Return a new {@link TraxIntermediateCalculation} = this.value / divisor.value.
     *
     * @param divisor The value to divide.
     * @return a new {@link TraxIntermediateCalculation} = this.value / divisor.value.
     */
    public TraxIntermediateCalculation dividedBy(@Nonnull final TraxNumber divisor) {
        return new TraxIntermediateCalculation(value / divisor.value);
    }

    /**
     * A subclass of {@link TraxNumber} that tracks the details of its calculation equivalent.
     */
    @Immutable
    public static class TraxTrackingNumber extends TraxNumber {
        private final String name;
        private final String details;
        private final TraxTrackable equivalent;

        /**
         * Create a new {@link TraxTrackingNumber} that tracks the details of its calculation equivalent.
         * If the {@link TraxContext} has trace level verbosity enabled, the created number's details
         * will be set to the caller's stack trace.
         *
         * @param value      The value of the number.
         * @param equivalent The calculation used to compute the number. Pass null if the number was directly
         *                   arrived at without a calculation.
         * @param context    The {@link TraxContext} for the relevant calculation topics.
         */
        TraxTrackingNumber(final double value,
                           @Nullable final TraxTrackable equivalent,
                           @Nonnull final TraxContext context) {
            super(value);
            this.name = null;

            this.details = context.trace() ? Trax.callerFromStackTrace() : null;
            this.equivalent = equivalent;
        }

        /**
         * Create a new {@link TraxTrackingNumber} that tracks the details of its calculation equivalent.
         * If the {@link TraxContext} has trace level verbosity enabled, the created number's details
         * will be set to the caller's stack trace.
         *
         * @param value      The value of the number.
         * @param name       The name of the number.
         * @param equivalent The calculation used to compute the number. Pass null if the number was directly
         *                   arrived at without a calculation.
         * @param context    The {@link TraxContext} for the relevant calculation topics.
         */
        TraxTrackingNumber(final double value,
                           @Nonnull final String name,
                           @Nullable final TraxTrackable equivalent,
                           @Nonnull final TraxContext context) {
            super(value);
            this.name = Objects.requireNonNull(name);
            this.details = context.trace() ? Trax.callerFromStackTrace() : null;
            this.equivalent = equivalent;
        }

        /**
         * Create a new {@link TraxTrackingNumber} that tracks the details of its calculation equivalent.
         *
         * @param value      The value of the number.
         * @param name       The name of the number.
         * @param details    The details description of the number.
         * @param equivalent The calculation used to compute the number. Pass null if the number was directly
         *                   arrived at without a calculation.
         * @param context    The {@link TraxContext} for the relevant calculation topics.
         */
        TraxTrackingNumber(final double value,
                           @Nonnull final String name,
                           @Nonnull final String details,
                           @Nullable final TraxTrackable equivalent,
                           @Nonnull final TraxContext context) {
            super(value);
            this.name = Objects.requireNonNull(name);
            this.details = context.trace() ? details : null;
            this.equivalent = equivalent;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public double getValue() {
            return value;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nullable
        public String getName() {
            return name;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasName() {
            return name != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nullable
        public String getDetails() {
            return details;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasDetails() {
            return details != null;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        @Nullable
        public TraxTrackable getEquivalent() {
            return equivalent;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean hasEquivalent() {
            return equivalent != null;
        }

        // ------------------------------------------------------------------------

        /**
         * Create a new {@link TraxNumber} with the given name whose value will be
         * equivalent to {@link this} number's value.
         *
         * @param name The name of the new number.
         * @return a new {@link TraxNumber} with the given name whose value will be
         *         equivalent to {@link this} number's value.
         */
        public TraxNumber named(@Nonnull final String name) {
            return new TraxTrackingNumber(getValue(), name, new TraxCalculation(this),
                TraxConfiguration.mostVerboseContextFor(Thread.currentThread()));
        }

        // ------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation plus(final double addend) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.PLUS, trax(addend));
            return new TraxIntermediateTrackingCalculation(addend + value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation plus(final double addend,
                                                @Nonnull final String addendName) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.PLUS, trax(addend, addendName));
            return new TraxIntermediateTrackingCalculation(addend + value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation plus(final double addend,
                                                @Nonnull final String addendName,
                                                @Nonnull final String addendDetails) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.PLUS,
                trax(addend, addendName, addendDetails));
            return new TraxIntermediateTrackingCalculation(addend + value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation plus(@Nonnull final TraxNumber addend) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.PLUS, addend);
            return new TraxIntermediateTrackingCalculation(addend.value + value, calc);
        }

        // ------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation minus(final double subtrahend) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.MINUS, trax(subtrahend));
            return new TraxIntermediateTrackingCalculation(value - subtrahend, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation minus(final double subtrahend,
                                                 @Nonnull final String subtrahendName) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.MINUS, trax(subtrahend, subtrahendName));
            return new TraxIntermediateTrackingCalculation(value - subtrahend, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation minus(final double subtrahend,
                                                 @Nonnull final String subtrahendName,
                                                 @Nonnull final String subtrahendDetails) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.MINUS,
                trax(subtrahend, subtrahendName, subtrahendDetails));
            return new TraxIntermediateTrackingCalculation(value - subtrahend, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation minus(@Nonnull final TraxNumber subtrahend) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.MINUS, subtrahend);
            return new TraxIntermediateTrackingCalculation(value - subtrahend.value, calc);
        }

        // ------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation subtractFrom(final double minuend) {
            final TraxCalculation calc = calc(trax(minuend), TraxInfixOperation.MINUS, this);
            return new TraxIntermediateTrackingCalculation(minuend - value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation subtractFrom(final double minuend,
                                                        @Nonnull final String minuendName) {
            final TraxCalculation calc = calc(trax(minuend, minuendName), TraxInfixOperation.MINUS, this);
            return new TraxIntermediateTrackingCalculation(minuend - value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation subtractFrom(final double minuend,
                                                        @Nonnull final String minuendName,
                                                        @Nonnull final String minuendDetails) {
            final TraxCalculation calc = calc(trax(minuend, minuendName, minuendDetails), TraxInfixOperation.MINUS, this);
            return new TraxIntermediateTrackingCalculation(minuend - value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation subtractFrom(@Nonnull final TraxNumber minuend) {
            final TraxCalculation calc = calc(minuend, TraxInfixOperation.MINUS, this);
            return new TraxIntermediateTrackingCalculation(minuend.value - value, calc);
        }

        // ------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation times(final double multiplier) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.TIMES, trax(multiplier));
            return new TraxIntermediateTrackingCalculation(multiplier * value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation times(final double multiplier,
                                                 @Nonnull final String multiplierName) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.TIMES, trax(multiplier, multiplierName));
            return new TraxIntermediateTrackingCalculation(multiplier * value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation times(final double multiplier,
                                                 @Nonnull final String multiplierName,
                                                 @Nonnull final String multiplierDetails) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.TIMES, trax(multiplier, multiplierName, multiplierDetails));
            return new TraxIntermediateTrackingCalculation(multiplier * value, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation times(@Nonnull final TraxNumber multiplier) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.TIMES, multiplier);
            return new TraxIntermediateTrackingCalculation(multiplier.value * value, calc);
        }

        // ------------------------------------------------------------------------

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation dividedBy(final double divisor) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.DIVIDED_BY, trax(divisor));
            return new TraxIntermediateTrackingCalculation(value / divisor, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation dividedBy(final double divisor,
                                                     @Nonnull final String divisorName) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.DIVIDED_BY,
                trax(divisor, divisorName));
            return new TraxIntermediateTrackingCalculation(value / divisor, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation dividedBy(final double divisor,
                                                     @Nonnull final String divisorName,
                                                     @Nonnull final String divisorDetails) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.DIVIDED_BY,
                trax(divisor, divisorName, divisorDetails));
            return new TraxIntermediateTrackingCalculation(value / divisor, calc);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public TraxIntermediateCalculation dividedBy(@Nonnull final TraxNumber divisor) {
            final TraxCalculation calc = calc(this, TraxInfixOperation.DIVIDED_BY, divisor);
            return new TraxIntermediateTrackingCalculation(value / divisor.value, calc);
        }

        /**
         * Create a {@link TraxCalculation} wrapping the elements.
         *
         * @param elements The elements to wrap with a {@link TraxCalculation}.
         * @return a {@link TraxCalculation} wrapping the elements.
         */
        @Nullable
        private TraxCalculation calc(@Nonnull final TraxTrackable... elements) {
            return new TraxCalculation(elements);
        }
    }

    /**
     * A subclass of {@link TraxNumber} that tracks the details of its calculation equivalent.
     * <p/>
     * For use when creating TraxConstants.
     * Constants are not permitted to have an equivalent.
     */
    @Immutable
    static class TraxConstantNumber extends TraxTrackingNumber {

        /**
         * Create a new {@link com.vmturbo.trax.TraxNumber.TraxConstantNumber}.
         *
         * @param value The value of the number.
         * @param name A name describing the number.
         * @param context The {@link TraxContext} for the number. Should be the CONTEXT_FOR_CONSTANTS.
         */
        TraxConstantNumber(double value, @Nonnull String name, @Nonnull TraxContext context) {
            super(value, name, null, context);
        }

        /**
         * {@inheritDoc}
         */
        @Nonnull
        @Override
        public String calculationStack() {
            final StringBuilder builder = new StringBuilder(256);
            appendChildrenStrings(builder, "", "", TraxConfiguration.CONTEXT_FOR_CONSTANTS);
            return builder.toString();
        }
    }
}