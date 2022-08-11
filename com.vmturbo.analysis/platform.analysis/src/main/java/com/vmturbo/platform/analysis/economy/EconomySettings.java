package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.actions.Move;

/**
 * The settings associated with and parameterizing the behavior of a single {@link Economy}.
 */
public final class EconomySettings {
    // Fields

    /**
     * The value returned by {@link #getMinSellersForParallelism()} when called on a newly
     * constructed instance.
     */
    // '256' was selected as the default value after finding the time-as-a-function-of-size curves
    // for parallel and sequential execution and finding their intersection.
    public static final int DEFAULT_MIN_SELLERS_FOR_PARALLELISM = 256;

    /**
     * The value returned by {@link #getQuoteFactor()} when called on a newly constructed instance.
     */
    // 0.999 corresponding to a 0.1% improvement seemed reasonable but there is no particular reason
    // not to use another value.
    public static final double DEFAULT_QUOTE_FACTOR = 0.999;

    private int minSellersForParallelism_ = DEFAULT_MIN_SELLERS_FOR_PARALLELISM;
    private double quoteFactor_ = DEFAULT_QUOTE_FACTOR;

    private double rightSizeLower_;
    private double rightSizeUpper_;

    // Constructors

    /**
     * Constructs a new default-initialized EconomySettings instance.
     */
    public EconomySettings() {/* empty body */}

    // Getters

    /**
     * Returns minimum number of sellers in a market required for parallel computation of minimum
     * quote to kick-in.
     *
     * <p>
     *  This is used because the overhead of running the computation in multiple threads can exceed
     *  the gains when the number of sellers is low.
     * </p>
     */
    @Pure
    public int getMinSellersForParallelism(@ReadOnly EconomySettings this) {
        return minSellersForParallelism_;
    }

    /**
     * Returns the <b>quote factor</b>.
     *
     * <p>
     *  The quote factor is a number (normally between 0 and 1), so that {@link Move} actions are
     *  only generated if best-quote < quote-factor * current-quote. That means that if we only want
     *  {@link Move}s that result in at least 1% improvement we should use a quote-factor of 0.99.
     * </p>
     */
    @Pure
    public double getQuoteFactor(@ReadOnly EconomySettings this) {
        return quoteFactor_;
    }

    // Setters

    /**
     * Sets the value of the <b>min sellers for parallelism</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param minSellersForParallelism the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getMinSellersForParallelism()
     */
    @Deterministic
    public EconomySettings setMinSellersForParallelism(int minSellersForParallelism) {
        checkArgument(minSellersForParallelism >= 0, "minSellersForParallelism = " + minSellersForParallelism);
        minSellersForParallelism_ = minSellersForParallelism;
        return this;
    }

    /**
     * Sets the value of the <b>quote factor</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param quoteFactor the new value for the field. Must be non-negative.
     * @return {@code this}
     *
     * @see #getQuoteFactor()
     */
    @Deterministic
    public EconomySettings setQuoteFactor(double quoteFactor) {
        checkArgument(quoteFactor >= 0, "quoteFactor = " + quoteFactor);
        quoteFactor_ = quoteFactor;
        return this;
    }

    @Pure
    public double getRightSizeLower(@ReadOnly EconomySettings this) {
        return rightSizeLower_;
    }

    @Pure
    public double getRightSizeUpper(@ReadOnly EconomySettings this) {
        return rightSizeUpper_;
    }

    @Deterministic
    public EconomySettings setRightSizeLower(double rightSizeLower) {
        checkArgument(rightSizeLower >= 0, "rightSizeLower = " + rightSizeLower);
        rightSizeLower_ = rightSizeLower;
        return this;
    }

    @Deterministic
    public EconomySettings setRightSizeUpper(double rightSizeUpper) {
        checkArgument(rightSizeUpper >= 0, "rightSizeUpper = " + rightSizeUpper);
        rightSizeUpper_ = rightSizeUpper;
        return this;
    }

    // Methods

    /**
     * Resets {@code this} {@link EconomySettings} instance to the state it was in just after
     * construction.
     *
     * <p>
     *  It has no other observable side-effects.
     * </p>
     */
    public void clear() {
        minSellersForParallelism_ = DEFAULT_MIN_SELLERS_FOR_PARALLELISM;
        quoteFactor_ = DEFAULT_QUOTE_FACTOR;
    }

} // end EconomySettings class
