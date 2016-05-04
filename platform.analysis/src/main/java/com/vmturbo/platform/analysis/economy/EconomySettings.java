package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * The settings associated with and parameterizing the behavior of a single {@link Economy}.
 */
public final class EconomySettings {
    // Fields

    // '256' was selected as the default value after finding the time-as-a-function-of-size curves
    // for parallel and sequential execution and finding their intersection.
    public static final int DEFAULT_MIN_SELLERS_FOR_PARALLELISM = 256;

    private int minSellersForParallelism_ = DEFAULT_MIN_SELLERS_FOR_PARALLELISM;

    // Constructors

    /**
     * Constructs a new default-initialized EconomySettings instance.
     */
    public EconomySettings() {/* empty body */}

    // Methods

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
     * Resets {@code this} {@link EconomySettings} instance to the state it was in just after
     * construction.
     *
     * <p>
     *  It has no other observable side-effects.
     * </p>
     */
    public void clear() {
        minSellersForParallelism_ = DEFAULT_MIN_SELLERS_FOR_PARALLELISM;
    }

} // end EconomySettings class
